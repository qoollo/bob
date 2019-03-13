
extern crate clap;

use clap::{App, Arg};

use grpc::ClientStubExt;
use grpc::RequestOptions;

use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

use bob::api::bob_grpc::{BobApi, BobApiClient};
use bob::api::bob::{PutRequest/*, GetRequest*/, BlobKey, Blob};

#[derive(Debug, Clone)]
struct NetConfig {
    port: u16,
    target: String
}

#[derive(Debug, Clone, Copy)]
struct TaskConfig {
    low_idx: u64,
    high_idx: u64,
    payload_size: u64
}

struct Stat {
    put_total: AtomicU64
}

fn stat_worker(stop_token: Arc<AtomicBool>, period_ms: u64, stat: Arc<Stat>) {
    let pause = time::Duration::from_millis(period_ms);
    let mut last_put_count = stat.put_total.load(Ordering::Relaxed);

    while !stop_token.load(Ordering::Relaxed) {
        thread::sleep(pause);
        let cur_put_count = stat.put_total.load(Ordering::Relaxed);
        let put_count_spd = (cur_put_count-last_put_count)/(period_ms/1000);
        last_put_count = cur_put_count;
        println!("put: {:5} rps", put_count_spd);
    }
}

fn bench_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Stat>) {
    let client = BobApiClient::new_plain(net_conf.target.as_str(),
                                            net_conf.port, Default::default()).unwrap();


    for i in task_conf.low_idx..task_conf.high_idx {
        let mut put_req = PutRequest::new();
        put_req.set_key({
                            let mut bk = BlobKey::new();
                            bk.set_key(i);
                            bk
                        });
        put_req.set_data({
                            let mut blob = Blob::new();
                            blob.set_data(vec![0; task_conf.payload_size as usize]);
                            blob
                        });
        let res = client.put(RequestOptions::new(), put_req).wait();
        stat.put_total.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = res {
            println!("Error: {:?}", e);
        }

        // let get_req = GetRequest::new();
        // let get_resp = client.get(RequestOptions::new(), get_req);

        // println!("GET: {:?}", get_resp.wait());
    }
}

fn main() {
    let matches = App::new("MyApp")
                    .arg(Arg::with_name("host")
                                    .help("ip or hostname of bob")
                                    .takes_value(true)
                                    .short("h")
                                    .long("host")
                                    .default_value("127.0.0.1"))
                    .arg(Arg::with_name("port")
                                    .help("port of bob")
                                    .takes_value(true)
                                    .short("p")
                                    .long("port")
                                    .default_value("20000"))
                    .arg(Arg::with_name("payload")
                                    .help("payload size")
                                    .takes_value(true)
                                    .short("l")
                                    .long("payload")
                                    .default_value("100000"))
                    .arg(Arg::with_name("threads")
                                    .help("worker thread count")
                                    .takes_value(true)
                                    .short("t")
                                    .long("threads")
                                    .default_value("4"))
                    .arg(Arg::with_name("count")
                                    .help("count of records to proceed")
                                    .takes_value(true)
                                    .short("c")
                                    .long("count")
                                    .default_value("1000000"))
                    .get_matches();

    let net_conf = NetConfig {
        port: matches.value_of("port").unwrap_or_default().parse().unwrap(),
        target: matches.value_of("host").unwrap_or_default().to_string()
    };

    let task_conf = TaskConfig {
        low_idx: 1,
        high_idx: matches.value_of("count").unwrap_or_default().parse().unwrap(),
        payload_size: matches.value_of("payload").unwrap_or_default().parse().unwrap()
    };

    let workers_count: u64 = matches.value_of("threads").unwrap_or_default().parse().unwrap();

    println!("Bob will be benchmarked now");
    println!("target: {}:{} workers_count: {} payload size: {} total records: {}",
            net_conf.target, net_conf.port, workers_count, task_conf.payload_size, task_conf.high_idx-task_conf.low_idx+1);




    let stat = Arc::new(Stat {
        put_total: AtomicU64::new(0)
    });

    let stop_token: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let stat_cln = stat.clone();
    
    let stat_thread = {
        let stop_token = stop_token.clone();
        thread::spawn(move || {    
            stat_worker(stop_token, 1000, stat_cln);
        })
    };
    let mut workers = vec![];
    let task_size = (task_conf.high_idx - task_conf.low_idx) / workers_count;
    for i in 0..workers_count {
        let nc = net_conf.clone();
        let stat_inner = stat.clone();
        let tc = TaskConfig {
            low_idx: task_conf.low_idx + task_size * i,
            high_idx: task_conf.low_idx + task_size * (i+1),
            payload_size: task_conf.payload_size
        };
        workers.push(thread::spawn(move || {    
            bench_worker(nc, tc, stat_inner);
        }));
    }

    for worker in workers {
        let _ = worker.join();
    }
    stop_token.store(true, Ordering::Relaxed);
    
    stat_thread.join().unwrap();
}