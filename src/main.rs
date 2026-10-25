use clap::{Parser, Subcommand};
use anyhow::Result;
use filecheck::{GenerateOptions, HashType};

use std::ffi::OsString;
use std::path::Path;
use std::process::ExitCode;

// filecheck generate src -j $(nproc) -o output
//   generate checksums for every file under src (hidden and gitignored files included)
// filecheck subset src.dump dest.dump
//   check every file of src exists in dest with the same content and relative path
// filecheck export dump
//   print a dump in sha256sum/md5sum format, sorted by path
// filecheck locate src.dump dest1.dump dest2.dump --exclude _trash [--expect staging.dump --expect-prefix sub]
//   for every file of src, print "<status>\t<path>\t<where>": is its content anywhere in the destinations?
// exit code is 1 when a file could not be read, or when subset/locate finds missing files

#[derive(Parser, Debug)]
#[command(name="filecheck")]
#[command(version, about, long_about = None)]
struct Cli{
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands{
    /// Generate checksum for directory recursively
    Generate (GenerateArgs),

    /// Check if destination contains the source (source /subseteq destination)
    Subset (SubsetArgs),

    /// Print a dump as "<checksum>  ./<path>" lines, sorted by path
    Export (ExportArgs),

    /// For every file in SRC, find whether its content (any path) exists in the DEST dumps
    Locate (LocateArgs),
}

#[derive(Debug, Parser, Clone)]
struct GenerateArgs {
    /// path to target directory
    #[arg(required=true)]
    src: OsString,
    /// number of jobs to run concurrently
    #[arg(short, long, default_value_t=GenerateOptions::default().jobs)]
    jobs: usize,
    /// path to output file
    #[arg(short, long, default_value_os="filecheck.dump")]
    output: OsString,
    /// checksum type used
    #[arg(short='t', long="hashtype", value_enum, default_value_t=HashType::SHA256)]
    hashtype: HashType,
    /// also checksum macOS metadata files (._* and .DS_Store)
    #[arg(long)]
    include_macos_metadata: bool,
    /// bytes read per call, e.g. 8K or 1M; larger is faster on network shares and WSL /mnt drives
    #[arg(short='b', long, default_value="1M", value_parser=filecheck::parse_size)]
    buffer_size: usize,
    /// overwrite the output file if it exists
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Parser)]
struct SubsetArgs{
    /// source file
    #[arg(required=true)]
    src: OsString,
    /// destination file
    #[arg(required=true)]
    dest: OsString,
}

#[derive(Debug, Parser)]
struct ExportArgs{
    /// dump file
    #[arg(required=true)]
    dump: OsString,
}

#[derive(Debug, Parser)]
struct LocateArgs{
    /// dump of the files to look for
    #[arg(required=true)]
    src: OsString,
    /// dumps to search, read one at a time
    #[arg(required=true, num_args=1..)]
    dest: Vec<OsString>,
    /// destination paths containing this text do not count as a copy (repeatable)
    #[arg(long)]
    exclude: Vec<String>,
    /// also require every SRC file to have the same checksum at the same path in this dump
    #[arg(long)]
    expect: Option<OsString>,
    /// directory inside the expect dump that SRC paths are relative to
    #[arg(long, default_value="", requires="expect")]
    expect_prefix: String,
}

fn main() -> Result<ExitCode>{
    let args = Cli::parse();
    //setup log level, info by default
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command{
        Commands::Generate(args) => generate_command(&args),
        Commands::Subset(args) => subset_command(&args),
        Commands::Export(args) => export_command(&args),
        Commands::Locate(args) => locate_command(&args),
    }
}

fn locate_command(args: &LocateArgs) -> Result<ExitCode>{
    let src = filecheck::read_dump(Path::new(&args.src))?;
    let mut locator = filecheck::Locator::new(&src);
    for dest in args.dest.iter(){
        // read and drop each destination in turn to bound memory
        let dump = filecheck::read_dump(Path::new(dest))?;
        let label = Path::new(dest).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        locator.add_dest(&label, &dump, &args.exclude)?;
    }
    let expect = match &args.expect{
        Some(path) => Some(filecheck::read_dump(Path::new(path))?),
        None => None,
    };
    let entries = locator.finish(expect.as_ref().map(|d| (d, args.expect_prefix.as_str())))?;

    let mut counts = std::collections::BTreeMap::new();
    for entry in entries.iter(){
        *counts.entry(entry.status.name()).or_insert(0_usize) += 1;
        println!("{}\t{}\t{}", entry.status.name(), entry.path, entry.location.as_deref().unwrap_or(""));
    }
    let summary: Vec<String> = counts.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
    eprintln!("Total {}: {}", entries.len(), summary.join(" "));
    let all_found = entries.iter().all(|e| e.status == filecheck::LocateStatus::Found);
    Ok(if all_found { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}

fn generate_command(args: &GenerateArgs) -> Result<ExitCode>{
    let opts = GenerateOptions{
        jobs: args.jobs,
        hashtype: args.hashtype,
        include_macos_metadata: args.include_macos_metadata,
        buffer_size: args.buffer_size,
    };
    let generated = filecheck::generate(Path::new(&args.src), &opts)?;
    filecheck::write_dump(&generated.dump, Path::new(&args.output), args.force)?;
    println!("Output written to {:?}", args.output);
    println!("Files: {}, failed: {}, skipped non-regular: {}, all-zero content: {}",
        generated.file_count, generated.failures.len(), generated.skipped_count, generated.zero_files.len());

    // all-zero files are only a warning: the checksums are still valid, but such copies are usually broken
    if !generated.zero_files.is_empty(){
        let mut report = std::path::PathBuf::from(&args.output).into_os_string();
        report.push(".zero.txt");
        std::fs::write(&report, generated.zero_files.join("\n") + "\n")?;
        eprintln!("Warning: {} non-empty files contain only zero bytes, list written to {:?}",
            generated.zero_files.len(), report);
    }

    // the dump is still written, but an incomplete dump must not look like success
    if !generated.failures.is_empty(){
        eprintln!("Failed to checksum {} entries:", generated.failures.len());
        for (path, reason) in generated.failures.iter(){
            eprintln!("{}: {}", path, reason);
        }
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

fn subset_command(args: &SubsetArgs) -> Result<ExitCode>{
    let src = filecheck::read_dump(Path::new(&args.src))?;
    let dest = filecheck::read_dump(Path::new(&args.dest))?;
    let report = filecheck::subset(&src, &dest)?;

    println!("Missing checksum: ");
    for path in report.missing_checksum.iter(){
        println!("{}", path);
    }
    println!("Missing duplicate: ");
    for path in report.missing_duplicate.iter(){
        println!("{}", path);
    }
    println!("Total missing checksum: {}, Total missing duplicate: {}", report.missing_checksum.len(), report.missing_duplicate.len());
    Ok(if report.is_subset() { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}

fn export_command(args: &ExportArgs) -> Result<ExitCode>{
    let dump = filecheck::read_dump(Path::new(&args.dump))?;
    for line in filecheck::export_lines(&dump){
        println!("{}", line);
    }
    Ok(ExitCode::SUCCESS)
}
