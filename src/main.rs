use clap::{Args, Parser, Subcommand, ValueEnum};
use anyhow::Result;
use walkdir::{WalkDir, DirEntry};

use std::collections::{HashMap, VecDeque};
use std::ffi::{OsString, OsStr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::io::{self,BufReader};

use generic_array::GenericArray;

//takes argument
// file_checker -g src -j $(nproc) -o output
// -g, generate checksum for directory, output a custom serialized file
// -o, specify output file
// file_checker -c src_file dest_file
// -c, contains, check if destination contains the src, must pass in files, not directory
// remember to use -g to generate before using -c
// -j, job count, specify how many threads used for generation
// -t, checksum type, [md5] or [sha256], md5 is default
// -h, output would be in human readble list

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
    Generate (GenerateArgs)
}

#[derive(Debug, Parser, Clone)]
struct GenerateArgs {
    /// path to target directory
    #[arg(required=true)]
    src: OsString,
    /// number of jobs to run concurrently
    #[arg(short, long, default_value_t=get_nproc())]
    jobs: usize,
    /// path to output file
    #[arg(short, long, default_value_os="filecheck.dump")]
    output: OsString,
    /// checksum type used
    #[arg(short='t', long="hashtype", default_value_os="md5")]
    hashtype: HashType,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum HashType{
    MD5,
    SHA256,
}

// get number of threads available
fn get_nproc() -> usize{ thread::available_parallelism().map(|x| x.get()).unwrap_or(1) }
fn main() {
    let args = Cli::parse();
    match args.command{
        Commands::Generate(args) => generate_command(&args),
        _ => todo!(),
    }
}

fn generate_command(args: &GenerateArgs){
    // uses a job queue, used to generate checksum for each file
    // not using mpsc for simplicity

    let job_queue = Arc::new(Mutex::new(VecDeque::new()));
    let condvar = Arc::new(Condvar::new());
    let hash_map = Arc::new(Mutex::new(HashMap::new()));
    let args = Arc::new(args.clone());

    // create a thread pool with the number of jobs
    let mut threads = Vec::new();
    for _ in 0..args.jobs{
        let job_queue = job_queue.clone();
        let condvar = condvar.clone();
        let hash_map = hash_map.clone();
        let args: Arc<GenerateArgs> = args.to_owned();
        threads.push(thread::spawn(move || worker(job_queue, condvar, hash_map, args)));
    }

    // push all files to job queue
    let walker = WalkDir::new(&args.src).into_iter();
    for entry in walker{
        let entry = entry.unwrap();
        if entry.file_type().is_file(){
            job_queue.lock().unwrap().push_front(entry.into_path());
            condvar.notify_all();
        }
    }

    while !job_queue.lock().unwrap().is_empty(){ 
        // wait for all jobs to finish
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    
}

fn worker(job_queue: Arc<Mutex<VecDeque<PathBuf>>>, condvar: Arc<Condvar>, hash_map: Arc<Mutex<HashMap<Vec<u8>, Vec<PathBuf>>>>, args: Arc<GenerateArgs>){
    loop{
        let path = {
            let mut job_queue = job_queue.lock().unwrap();
            while job_queue.is_empty(){
                job_queue = condvar.wait(job_queue).unwrap();
                // No explict exit check, process will terminate after all jobs done
            }
            job_queue.pop_back().unwrap()
        };
        match compute_checksum(path.as_path(), args.hashtype){
            Ok(checksum) => {
                let mut hash_map = hash_map.lock().unwrap();
                hash_map.entry(checksum).or_insert(Vec::new()).push(path);
            },
            Err(e) => {
                eprintln!("Error: Failed to compute checksum for file: {:?}", path);
            }
        }
    }
}


fn compute_checksum(path: &Path, hashtype: HashType) -> Result<Vec<u8>>{
    
    let mut file = std::io::BufReader::new(std::fs::File::open(path)?);
    
    match hashtype{
        HashType::MD5 => {
            use md5::{Digest, Md5};
            let mut hasher = Md5::new();
            io::copy(&mut file, &mut hasher)?;
            Ok(hasher.finalize().to_vec())
        },
        HashType::SHA256 => {
            todo!();
        }
    }
}

// all generated checksum is stored in a std::mutex< HashMap<checksum, Vec::<RelPath> > >
// the relpath is the relative path of the file with respect to src





// when writing back to file, simply dump the hash file, if -h flag is on, then print out the HashMap one by one, with the format <RelPath> <checksum>
