use clap::{Args, Parser, Subcommand, ValueEnum};
use anyhow::Result;
use walkdir::{WalkDir, DirEntry};

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsString, OsStr};
use std::path::{Path, PathBuf};
use std::sync::{atomic, Arc, Condvar, Mutex};
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
    Generate (GenerateArgs),

    /// Check if destination contains the source (source /subseteq destination)
    Subset (SubsetArgs),
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

#[derive(Debug, Parser)]
struct SubsetArgs{
    /// source file
    #[arg(required=true)]
    src: OsString,
    /// destination file
    #[arg(required=true)]
    dest: OsString,
}

// get number of threads available
fn get_nproc() -> usize{ thread::available_parallelism().map(|x| x.get()).unwrap_or(1) }
fn main() -> Result<()>{
    let args = Cli::parse();
    match args.command{
        Commands::Generate(args) => generate_command(&args),
        Commands::Subset(args) => subset_command(&args),
        _ => todo!(),
    }
}

fn generate_command(args: &GenerateArgs) -> Result<()>{
    // uses a job queue, used to generate checksum for each file
    // not using mpsc for simplicity

    let generate_context = Arc::new(GenerateContext{
        job_queue: Mutex::new(VecDeque::new()),
        condvar: Condvar::new(),
        hash_map: Mutex::new(HashMap::new()),
        args: args.clone(),
    });
    // let job_queue = Arc::new(Mutex::new(VecDeque::new()));
    // let condvar = Arc::new(Condvar::new());
    // let hash_map = Arc::new(Mutex::new(HashMap::new()));
    // let args = Arc::new(args.clone());

    // create a thread pool with the number of jobs
    let mut threads = Vec::new();
    for _ in 0..args.jobs{
        // let job_queue = job_queue.clone();
        // let condvar = condvar.clone();
        // let hash_map = hash_map.clone();
        // let args: Arc<GenerateArgs> = args.to_owned();
        let context = generate_context.clone();
        threads.push(thread::spawn(move || worker(context)));
    }


    // push all files to job queue
    let walker = WalkDir::new(&args.src).into_iter();
    for entry in walker{
        let entry = entry.unwrap();
        if entry.file_type().is_file(){
            generate_context.job_queue.lock().unwrap().push_front(entry.into_path());
            generate_context.condvar.notify_all();
        }
    }

    while !generate_context.job_queue.lock().unwrap().is_empty(){ 
        // wait for all jobs to finish
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    //dump into output file
    bincode::serialize_into(std::fs::File::create(&args.output)?, &*generate_context.hash_map.lock().unwrap())?;
    return Ok(());
    
}

struct GenerateContext{
    job_queue: Mutex<VecDeque<PathBuf>>,
    condvar: Condvar,
    hash_map: Mutex<HashMap<Vec<u8>, Vec<PathBuf>>>,
    args: GenerateArgs,
}

fn worker(context: Arc<GenerateContext>){
    let GenerateContext{job_queue, condvar, hash_map, args} = context.as_ref();
    loop{
        let path = {
            let mut job_queue = job_queue.lock().unwrap();
            while job_queue.is_empty(){
                job_queue = condvar.wait(job_queue).unwrap();
                // if finished.load(atomic::Ordering::Relaxed){ return; }
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

fn subset_command(args: &SubsetArgs) -> Result<()>{
    // load the hash map from file
    let src_hashmap: HashMap<Vec<u8>, Vec<PathBuf>> = bincode::deserialize_from(std::fs::File::open(&args.src)?)?;
    let mut dest_hashmap: HashMap<Vec<u8>, Vec<PathBuf>> = bincode::deserialize_from(std::fs::File::open(&args.dest)?)?;

    let mut diff_result = Vec::<PathBuf>::new();

    for (hash, mut paths) in src_hashmap.into_iter(){
        if let Some(dest_paths) = dest_hashmap.remove(&hash){
            // check if dest_paths contains paths
            if paths.len() == 1 {
                if paths[0] != dest_paths[0]{
                    diff_result.push(paths.pop().unwrap());
                }
            }else{
                // use hashset to check
                let src_set: HashSet<_> = paths.into_iter().collect();
                let dest_set: HashSet<_> = dest_paths.into_iter().collect();
                for path in dest_set.difference(&src_set){
                    diff_result.push(path.clone());
                }
            }
        } else { 
            // dest does not contain src
            diff_result.append(&mut paths);
        }
    }
    for path in diff_result{
        println!("{:?}", path);
    }
    return Ok(());
}