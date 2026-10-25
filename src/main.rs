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
// exit code is 1 when a file could not be read, or when subset finds missing files

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

fn main() -> Result<ExitCode>{
    let args = Cli::parse();
    //setup log level, info by default
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command{
        Commands::Generate(args) => generate_command(&args),
        Commands::Subset(args) => subset_command(&args),
        Commands::Export(args) => export_command(&args),
    }
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
    println!("Files: {}, failed: {}, skipped non-regular: {}",
        generated.file_count, generated.failures.len(), generated.skipped_count);

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
