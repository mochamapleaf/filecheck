use anyhow::{bail, Context, Result};
use log::{debug, error, info};

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::thread;

// dump file layout (bincode): (DUMP_MAGIC, DUMP_VERSION, hash type name, checksum -> relative paths)
// relative paths always use '/' so dumps from Windows and Linux can be compared
const DUMP_MAGIC: &str = "filecheck-dump";
const DUMP_VERSION: u32 = 2;

#[derive(clap::ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
pub enum HashType{
    MD5,
    SHA256,
}

impl HashType{
    fn name(self) -> &'static str{
        match self{
            HashType::MD5 => "md5",
            HashType::SHA256 => "sha256",
        }
    }

    fn from_name(name: &str) -> Result<Self>{
        match name{
            "md5" => Ok(HashType::MD5),
            "sha256" => Ok(HashType::SHA256),
            _ => bail!("unknown hash type in dump: {}", name),
        }
    }
}

pub type ChecksumMap = HashMap<Vec<u8>, Vec<String>>;

#[derive(Debug)]
pub struct Dump{
    pub hashtype: HashType,
    pub files: ChecksumMap,
}

#[derive(Debug, Clone)]
pub struct GenerateOptions{
    pub jobs: usize,
    pub hashtype: HashType,
    /// also checksum macOS metadata files (._* and .DS_Store), skipped by default
    pub include_macos_metadata: bool,
    /// bytes read per call; large reads matter on network shares and WSL's /mnt drives
    pub buffer_size: usize,
}

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

impl Default for GenerateOptions{
    fn default() -> Self{
        GenerateOptions{
            jobs: thread::available_parallelism().map(|x| x.get()).unwrap_or(1),
            hashtype: HashType::SHA256,
            include_macos_metadata: false,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

/// Parse a size like "8192", "8K", "1M" (binary units, case-insensitive).
pub fn parse_size(s: &str) -> Result<usize>{
    let s = s.trim();
    let (digits, multiplier) = match s.chars().last().map(|c| c.to_ascii_uppercase()){
        Some('K') => (&s[..s.len() - 1], 1024),
        Some('M') => (&s[..s.len() - 1], 1024 * 1024),
        Some('G') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    let value: usize = digits.trim().parse()
        .with_context(|| format!("invalid size {:?}, expected e.g. 8K or 1M", s))?;
    let size = value.checked_mul(multiplier)
        .with_context(|| format!("size {:?} is too large", s))?;
    if size == 0{
        bail!("size must be greater than 0");
    }
    Ok(size)
}

#[derive(Debug)]
pub struct Generated{
    pub dump: Dump,
    /// number of regular files found
    pub file_count: usize,
    /// symlinks and other non-regular entries, not followed and not checksummed
    pub skipped_count: usize,
    /// (path, reason) for every entry that could not be read or checksummed
    pub failures: Vec<(String, String)>,
    /// non-empty files whose content is all zero bytes (often a broken or never-downloaded copy), sorted
    pub zero_files: Vec<String>,
}

#[derive(Debug, Default)]
pub struct SubsetReport{
    /// checksum not present in destination at all
    pub missing_checksum: Vec<String>,
    /// checksum present in destination, but not at this relative path
    pub missing_duplicate: Vec<String>,
}

impl SubsetReport{
    pub fn is_subset(&self) -> bool{
        self.missing_checksum.is_empty() && self.missing_duplicate.is_empty()
    }
}

/// Generate checksums for every regular file under `base_dir`, recursively.
/// Hidden files and files listed in .gitignore are included: this is meant for verifying copies.
pub fn generate(base_dir: &Path, opts: &GenerateOptions) -> Result<Generated>{
    let base_dir = base_dir.canonicalize()
        .with_context(|| format!("cannot open directory {:?}", base_dir))?;
    info!("Generating {} checksums for files in {:?}", opts.hashtype.name(), base_dir);

    // plain walkdir: no ignore files are wanted here, and ignore 0.4.23's sequential walker
    // panics (walk.rs:1004) after a directory it cannot read on Windows
    let walker = walkdir::WalkDir::new(&base_dir)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| opts.include_macos_metadata || e.depth() == 0 || !is_macos_metadata(e.file_name()));

    // walk first, then hash: the queue is complete before any worker starts,
    // so workers simply stop when it is empty
    let mut queue = VecDeque::new();
    let mut failures = Vec::new();
    let mut skipped_count = 0_usize;
    for entry in walker{
        match entry{
            Ok(entry) => {
                let file_type = entry.file_type();
                if file_type.is_file(){
                    queue.push_back(entry.into_path());
                } else if !file_type.is_dir(){
                    debug!("Skipping non-regular file {:?}", entry.path());
                    skipped_count += 1;
                }
            },
            // e.g. a directory without read permission: record it and keep walking
            Err(e) => {
                error!("Failed to read directory entry: {}", e);
                let path = e.path().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
                failures.push((path, e.to_string()));
            },
        }
    }
    let file_count = queue.len();

    let queue = Mutex::new(queue);
    let files = Mutex::new(ChecksumMap::new());
    let zero_files = Mutex::new(Vec::new());
    let failures = Mutex::new(failures);
    let jobs = opts.jobs.max(1);
    let buffer_size = opts.buffer_size.max(1);
    thread::scope(|scope|{
        for _ in 0..jobs{
            scope.spawn(||{
                // one read buffer per worker, reused for every file
                let mut buffer = vec![0_u8; buffer_size];
                loop{
                    let Some(path) = queue.lock().unwrap().pop_front() else { break };
                    let result = relative_path(&base_dir, &path)
                        .and_then(|rel| checksum_with_buffer(&path, opts.hashtype, &mut buffer).map(|sum| (rel, sum)));
                    match result{
                        Ok((rel, (sum, zero))) => {
                            if zero{
                                zero_files.lock().unwrap().push(rel.clone());
                            }
                            files.lock().unwrap().entry(sum).or_default().push(rel);
                        },
                        Err(e) => {
                            error!("Failed to checksum {:?}: {:#}", path, e);
                            failures.lock().unwrap().push((path.to_string_lossy().into_owned(), format!("{:#}", e)));
                        },
                    }
                }
            });
        }
    });

    let files = files.into_inner().unwrap();
    let failures = failures.into_inner().unwrap();
    let mut zero_files = zero_files.into_inner().unwrap();
    zero_files.sort();
    let hashed: usize = files.values().map(Vec::len).sum();
    info!("Found {} files, checksummed {}, failed {}, skipped {} non-regular entries",
        file_count, hashed, failures.len(), skipped_count);
    Ok(Generated{
        dump: Dump{ hashtype: opts.hashtype, files },
        file_count,
        skipped_count,
        failures,
        zero_files,
    })
}

// macOS metadata skipped by default: AppleDouble "._*" files and .DS_Store
fn is_macos_metadata(name: &std::ffi::OsStr) -> bool{
    name == ".DS_Store" || name.as_encoded_bytes().starts_with(b"._")
}

// relative path with '/' separators, every name encoded losslessly by encode_name
fn relative_path(base_dir: &Path, path: &Path) -> Result<String>{
    let rel = path.strip_prefix(base_dir)
        .with_context(|| format!("{:?} is not under {:?}", path, base_dir))?;
    let mut parts = Vec::new();
    for component in rel.components(){
        match component{
            Component::Normal(part) => parts.push(encode_name(part)),
            other => bail!("unexpected path component {:?} in {:?}", other, path),
        }
    }
    Ok(parts.join("/"))
}

/// Encode a file name as text without losing information, so names that are not valid
/// UTF-8 (e.g. GBK names unpacked on Linux) can still be checksummed and compared.
/// Valid text is kept as is, except `\` -> `\\` and newline -> `\n` (the same escapes sha256sum uses);
/// on Unix a name that is not UTF-8 has all its non-ASCII bytes written as `\xHH`,
/// on Windows unpaired UTF-16 surrogates become `\uDXXX`.
pub fn encode_name(name: &std::ffi::OsStr) -> String{
    let mut out = String::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let bytes = name.as_bytes();
        match std::str::from_utf8(bytes){
            Ok(text) => push_escaped(&mut out, text),
            // not UTF-8 (e.g. GBK): escape every non-ASCII byte, so the same name always
            // encodes the same way instead of mixing stray valid sequences into the text
            Err(_) => for &byte in bytes{
                if byte.is_ascii(){
                    push_escaped(&mut out, (byte as char).encode_utf8(&mut [0; 4]));
                } else {
                    out.push_str(&format!("\\x{:02X}", byte));
                }
            },
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        for unit in char::decode_utf16(name.encode_wide()){
            match unit{
                Ok(c) => push_escaped(&mut out, c.encode_utf8(&mut [0; 4])),
                Err(e) => out.push_str(&format!("\\u{:04X}", e.unpaired_surrogate())),
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        push_escaped(&mut out, &name.to_string_lossy());
    }
    out
}

fn push_escaped(out: &mut String, text: &str){
    for c in text.chars(){
        match c{
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

pub fn compute_checksum(path: &Path, hashtype: HashType) -> Result<Vec<u8>>{
    checksum_with_buffer(path, hashtype, &mut vec![0_u8; DEFAULT_BUFFER_SIZE]).map(|(sum, _)| sum)
}

// reads the file in buffer-sized chunks straight into the hasher;
// also reports whether a non-empty file holds only zero bytes
fn checksum_with_buffer(path: &Path, hashtype: HashType, buffer: &mut [u8]) -> Result<(Vec<u8>, bool)>{
    let file = std::fs::File::open(path)?;
    match hashtype{
        HashType::MD5 => hash_reader(file, md5::Md5::default(), buffer),
        HashType::SHA256 => hash_reader(file, sha2::Sha256::default(), buffer),
    }
}

fn hash_reader<R: io::Read, D: sha2::Digest>(mut reader: R, mut hasher: D, buffer: &mut [u8]) -> Result<(Vec<u8>, bool)>{
    let mut total = 0_u64;
    let mut all_zero = true;
    loop{
        let n = match reader.read(buffer){
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        hasher.update(&buffer[..n]);
        total += n as u64;
        all_zero = all_zero && buffer[..n].iter().all(|&b| b == 0);
    }
    Ok((hasher.finalize().to_vec(), total > 0 && all_zero))
}

/// Write a dump; refuses to overwrite an existing file unless `force` is set.
pub fn write_dump(dump: &Dump, output: &Path, force: bool) -> Result<()>{
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .create_new(!force)
        .truncate(true)
        .open(output)
        .with_context(|| format!("cannot create {:?} (use --force to overwrite)", output))?;
    let data = (DUMP_MAGIC, DUMP_VERSION, dump.hashtype.name(), &dump.files);
    let mut writer = io::BufWriter::new(file);
    bincode::serialize_into(&mut writer, &data)?;
    // flush explicitly, a failed write on drop would be silently ignored
    io::Write::flush(&mut writer)?;
    Ok(())
}

pub fn read_dump(path: &Path) -> Result<Dump>{
    let file = io::BufReader::new(std::fs::File::open(path)
        .with_context(|| format!("cannot open {:?}", path))?);
    let (magic, version, hashtype, files): (String, u32, String, ChecksumMap) = bincode::deserialize_from(file)
        .with_context(|| format!("{:?} is not a filecheck dump (dumps from 0.1.x must be regenerated)", path))?;
    if magic != DUMP_MAGIC || version != DUMP_VERSION{
        bail!("{:?} is not a filecheck v{} dump", path, DUMP_VERSION);
    }
    Ok(Dump{ hashtype: HashType::from_name(&hashtype)?, files })
}

/// Check that every file in `src` exists in `dest` with the same content at the same relative path.
pub fn subset(src: &Dump, dest: &Dump) -> Result<SubsetReport>{
    if src.hashtype != dest.hashtype{
        bail!("cannot compare a {} dump with a {} dump", src.hashtype.name(), dest.hashtype.name());
    }
    let mut report = SubsetReport::default();
    for (hash, paths) in src.files.iter(){
        match dest.files.get(hash){
            Some(dest_paths) => {
                let dest_set: HashSet<&String> = dest_paths.iter().collect();
                for path in paths{
                    if !dest_set.contains(path){
                        report.missing_duplicate.push(path.clone());
                    }
                }
            },
            None => report.missing_checksum.extend(paths.iter().cloned()),
        }
    }
    report.missing_checksum.sort();
    report.missing_duplicate.sort();
    Ok(report)
}

/// Lines in `sha256sum` / `md5sum` format ("<hex>  ./<path>"), sorted by path in byte order,
/// so a dump can be compared with `find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum`.
pub fn export_lines(dump: &Dump) -> Vec<String>{
    let mut entries: Vec<(&String, &Vec<u8>)> = dump.files.iter()
        .flat_map(|(hash, paths)| paths.iter().map(move |path| (path, hash)))
        .collect();
    entries.sort();
    // like sha256sum, a line whose name contains escapes starts with '\'
    entries.into_iter()
        .map(|(path, hash)|{
            let prefix = if path.contains('\\') { "\\" } else { "" };
            format!("{}{}  ./{}", prefix, to_hex(hash), path)
        })
        .collect()
}

pub fn to_hex(bytes: &[u8]) -> String{
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn default_output() -> PathBuf{
    PathBuf::from("filecheck.dump")
}
