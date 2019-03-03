// use in map reduce core
use std::vec::Vec;
use std::collections::HashMap;
use std::fs::{self, DirEntry};
use std::path::Path;
use std::ffi::OsStr;
use std::io;
use std::env;
use std::path::PathBuf;
use std::time::Instant;


pub fn main() {
    let now = Instant::now();
    let args: Vec<String> = env::args().collect();
    let path_as_string = &args[1];
    let files = list_file_entries(path_as_string);
    let _result = map_reduce::run_map_reduce_framework::<PathBuf, i32, String, i32, String, i32>(map_count_words, reduce_count_words, files, 4, 20);
//    info!("{:?}", result);
    println!("elapsed {}", now.elapsed().as_secs());
}

// ----------------- count words --------------------
// --------- prepare input -------
fn list_file_entries(path: &str) -> Vec<(PathBuf, i32)> {
    let mut input_vector: Vec<(PathBuf, i32)> = Vec::new();
    let mut vector_updater = |entry: DirEntry| {
        if entry.path().extension().and_then(OsStr::to_str).eq(&Some("txt")) {
            input_vector.push((entry.path(), 0));
        }
    };
    visit_dirs(Path::new(path), &mut vector_updater).unwrap();
    return input_vector;
}
// implementation of walking a directory only visiting files
fn visit_dirs(dir: &Path, cb: &mut FnMut(DirEntry)) -> io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(entry);
            }
        }
    }
    Ok(())
}

// --------- MAP -------
fn map_count_words(entry: PathBuf, _unused: i32, emit: &mut FnMut(String, i32)) {
    // read all file
    let tmp = fs::read_to_string(entry).unwrap();

    // tokenize
    let tokens: Vec<&str> = tmp.split(char::is_whitespace).filter(|k| !k.is_empty()).collect();
    let mut counters: HashMap<String, i32> = HashMap::new();
    for token in tokens {
        let counter = counters.entry(token.to_string()).or_insert(0);
        *counter += 1;
    }

    let vec_results =  hash_map2vector_of_pairs(counters);
    for item in vec_results{
        emit(item.0, item.1);
    }
}


fn hash_map2vector_of_pairs<K, V>(mut map: HashMap<K, V>) -> Vec<(K, V)>
    where K : std::cmp::Eq + std::hash::Hash + std::clone::Clone,
{
    // extract an owned list (cloned) of all the keys.
    let mut all_keys: Vec<K> = Vec::new();
    for k in map.keys() {
        all_keys.push(k.clone())
    }
    // move values from the map.
    let mut result: Vec<(K, V)> = Vec::new();
    for k2 in all_keys {
        let v = map.remove_entry(&k2).unwrap();
        result.push(v);
    }
    return result;
}

// -------- REDUCE ------------------
fn reduce_count_words(k: String, v: Vec<i32>, emit: &mut FnMut(String, i32)){
    let mut sum = 0;
    for item in v {
        sum = sum + item;
    }
    emit(k, sum);
}