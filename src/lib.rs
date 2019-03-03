//// logging
#[macro_use]
extern crate log;
//extern crate simplelog;
use simplelog::*;
use std::fs::File;
// use in map reduce core
use std::vec::Vec;
use std::collections::HashMap;
// sync and communication
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
// threading
use std::thread;
use std::clone::Clone;
use std::thread::JoinHandle;
use std::cmp;

/// this is a multi-threaded map-reduce implementation,
///     as described in wikipedia: https://en.wikipedia.org/wiki/MapReduce
///
/// to use this, you need to implement map and reduce functions as in the signature,
///     and pass them as parameters, together with the input items vector.
///
/// the map and reduce functions does not return anything, but emit there output using the "emit" function.
///
pub fn run_map_reduce_framework<K1, V1, K2, V2, K3, V3>(map: fn(K1, V1, emit: &mut FnMut(K2, V2)),
                                                    reduce: fn(K2, Vec<V2>, emit: &mut FnMut(K3, V3)),
                                                    mut items_vec: Vec<(K1, V1)>, number_of_threads:i32, chunk_size:usize) -> Vec<(K3, V3)>
    where K1: std::marker::Send + PartialOrd + 'static,
        V1: std::marker::Send + 'static,
        K2: std::marker::Send + PartialOrd + 'static + std::cmp::Eq + std::hash::Hash + std::clone::Clone + std::fmt::Debug,
        V2: std::marker::Send + 'static + std::fmt::Display + std::fmt::Debug,
        K3: std::marker::Send + PartialOrd + 'static,
        V3: std::marker::Send + 'static
{

    init_log_system();
    info!("MapReduce called, logging system initialized.");

    let mut map_result:Vec<(K2, V2)> = Vec::new();
    let (tx_worker_to_manager, rx_worker_to_manager) = mpsc::channel();
    // map the thread id to the join-handle and the Sender.
    let mut threads: HashMap<i32, (JoinHandle<()>, Sender<MapJob<K1, V1>>)> = HashMap::new();

    // spawn the map workers
    for i in 0..number_of_threads {
        let (tx_manager_to_worker, rx_manager_to_worker) = mpsc::channel();
        let cloned_tx_worker_to_manager = tx_worker_to_manager.clone();
        let handle = thread::spawn(move || { map_worker_function(map, rx_manager_to_worker, cloned_tx_worker_to_manager, i) });
        threads.insert(i, (handle, tx_manager_to_worker));
    }

    // send first job for each worker
    for i in 0..number_of_threads {
        // send first job
        let moved_slice:Vec<(K1, V1)> = move_slicing(&mut items_vec, chunk_size);
        let job = MapJob::Work(moved_slice);
        threads.get(&i).unwrap().1.send(job).unwrap();
    }

    // wait for responses and send more jobs, until finishing mapping.
    while !threads.is_empty() {
        let (mut result, id) = rx_worker_to_manager.recv().unwrap();
        map_result.append(&mut result); // save result in our vector

        if !items_vec.is_empty() {
            // send new job!!
            let moved_slice:Vec<(K1, V1)> = move_slicing(&mut items_vec, chunk_size);
            let job = MapJob::Work(moved_slice);
            threads.get(&id).unwrap().1.send(job).unwrap();
        }
        else {
            // finished mapping, kill worker
            let (id, (join_handel, sender)) = threads.remove_entry(&id).unwrap();
            sender.send(MapJob::Stop).unwrap();
            join_handel.join().expect("join failed");
            info!("map_worker_{} joined seccesfully", id);
        }
    }
    info!("finished mapping");

    let mut shuffled_result = shuffle_(map_result);

    // --------- finished shuffling!!!----------

    let mut reduce_result:Vec<(K3, V3)> = Vec::new();
    let (tx_worker_to_manager, rx_worker_to_manager) = mpsc::channel();
    // map the thread id to the join-handle and the Sender.
    let mut threads: HashMap<i32, (JoinHandle<()>, Sender<ReduceJob<K2, V2>>)> = HashMap::new();
    for i in 0..number_of_threads {
        let (tx_manager_to_worker, rx_manager_to_worker) = mpsc::channel();
        let cloned_tx_worker_to_manager = tx_worker_to_manager.clone();
        let handle = thread::spawn(move || { reduce_worker_function(reduce, rx_manager_to_worker, cloned_tx_worker_to_manager, i) });
        threads.insert(i, (handle, tx_manager_to_worker));
    }
    for i in 0..number_of_threads {
        // send first job
        let moved_slice = move_slicing(&mut shuffled_result, chunk_size);
        let job = ReduceJob::Work(moved_slice);
        threads.get(&i).unwrap().1.send(job).unwrap();
    }

    while !threads.is_empty() {
        let (mut result, id) = rx_worker_to_manager.recv().unwrap();
        reduce_result.append(&mut result); // save result in our vector

        if !shuffled_result.is_empty() {
            // send new job!!
            let moved_slice = move_slicing(&mut shuffled_result, chunk_size);
            let job = ReduceJob::Work(moved_slice);
            threads.get(&id).unwrap().1.send(job).unwrap();
        }
        else {
            // kill worker
            let (id, (join_handel, sender)) = threads.remove_entry(&id).unwrap();
            sender.send(ReduceJob::Stop).unwrap();
            join_handel.join().expect("join failed");
            info!("reduce_worker_{} joined seccesfully", id);
        }
    }
    info!("finished reduce");

    return reduce_result;

}

fn shuffle_<K2, V2>(map_result: Vec<(K2, V2)>) -> Vec<(K2, Vec<V2>)>
    where K2 : std::cmp::Eq + std::hash::Hash + std::clone::Clone,
{
    // shuffle into map; move to vector after.
    let  mut shuffle_map: HashMap<K2, Vec<V2>> =  HashMap::new();
    for tup in map_result {
        let k = tup.0;
        let v = tup.1;
        let vec = shuffle_map.entry(k).or_insert(Vec::new());
        vec.push(v);
    }
    // this will contain the result.
    let mut shuffled_result: Vec<(K2, Vec<V2>)> = Vec::new();
    // clone all keys. reason: cant remove map entry in regular iteration.
    let mut all_keys: Vec<K2> = Vec::new();
    for k2 in shuffle_map.keys() {
        all_keys.push(k2.clone())
    }
    // remove map entries and push into the vector.
    for k2 in all_keys {
        let v = shuffle_map.remove_entry(&k2).unwrap();
        shuffled_result.push(v);
    }
    return shuffled_result;
}

fn move_slicing<T>(src: &mut Vec<T>, slice_size: usize) -> Vec<T> {
    let mut moved_slice:Vec<T> = Vec::new();
    for _j in 0..cmp::min(slice_size, src.len()) {
        moved_slice.push(src.pop().unwrap());
    }
    return moved_slice;
}

enum MapJob<K1, V1> {
    Stop,
    Work(Vec<(K1, V1)>),
}

fn map_worker_function<K1, V1, K2, V2>(map: fn(K1, V1, emit: &mut FnMut(K2, V2)),
                                       rx:Receiver<MapJob<K1, V1>>, tx:Sender<(Vec<(K2, V2)>, i32)>, id: i32)
    where K1: PartialOrd + std::marker::Send,
        V1: std::marker::Send,
        K2: PartialOrd + std::marker::Send,
        V2: std::marker::Send,
{
    info!("mapWorker_{} map worker spawned", id);
    let mut stop = false;
    while !stop {
        let rcv_msg = rx.recv().unwrap();
        match rcv_msg {
            MapJob::Stop => {
                stop = true;
            }
            MapJob::Work(data) => {
                info!("mapWorker_{} working", id);
                let mut result: Vec<(K2, V2)> = Vec::new();
                for item in data {
                    let mut emit = |k2, v2| {result.push((k2, v2));};
                    map(item.0, item.1, &mut emit)
                }
                tx.send((result, id)).unwrap();
            }
        }
    }
    info!("mapWorker_{} map worker finished", id);
}

enum ReduceJob<K2, V2> {
    Stop,
    Work(Vec<(K2, Vec<V2>)>),
}

fn reduce_worker_function<K2, V2, K3, V3>(reduce: fn(K2, Vec<V2>, emit: &mut FnMut(K3, V3)),
                                          rx:Receiver<ReduceJob<K2, V2>>,
                                          tx:Sender<(Vec<(K3, V3)>, i32)>, id: i32) {
    info!("reduce_worker_{} started", id);
    let mut stop = false;
    while !stop {
        let rcv_msg = rx.recv().unwrap();
        match rcv_msg {
            ReduceJob::Stop => {
                stop = true;
            }
            ReduceJob::Work(data) => {
                info!("reduce_worker_{} working", id);
                let mut result: Vec<(K3, V3)> = Vec::new();
                for item in data {
                    let mut emit = |k3, v3| {result.push((k3, v3));};
                    reduce(item.0, item.1, &mut emit)
                }
                tx.send((result, id)).unwrap();
            }
        }
    }
    info!("reduce_worker_{} finished", id);
}

fn init_log_system() {
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Warn, Config::default()).unwrap(),
            WriteLogger::new(LevelFilter::Info, Config::default(), File::create("my_rust_binary.log").unwrap()),
        ]
    ).unwrap();
}