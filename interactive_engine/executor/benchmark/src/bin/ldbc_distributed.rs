use std::fs::File;
use std::io::{self, BufRead};
use std::os::unix::thread;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

use itertools::Itertools;
// #[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;
use mcsr::graph_db::GlobalCsrTrait;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_benchmark::queries;
use pegasus_network::config::{NetworkConfig, ServerAddr};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use jemalloc_sys::mallctl;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::thread::sleep;
use std::time::Duration;

// #[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

pub fn spawn_dump(secs: u64) {
    let thread = std::thread::Builder::new().name("spawn_dump".to_owned());
    thread
        .spawn(move || loop {
            let r = dump();
            println!("---------------dump ret: {} -------------------", r);
            sleep(Duration::from_secs(secs));
        })
        .unwrap();
}

fn dump() -> c_int {
    unsafe {
        mallctl(
            "prof.dump\0".as_bytes() as *const _ as *const c_char,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            0,
        )
    }
}


#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "b", long = "batch_size")]
    batch_size: u32,
    #[structopt(short = "c", long = "batch_count")]
    batch_count: u32,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(long = "port", default_value = "0")]
    port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Result {
    query_type: String,
    query_input: Vec<String>,
    query_result: Vec<Vec<String>>,
}

fn main() {
    // spawn_dump(5);
    let config: Config = Config::from_args();

    pegasus_common::logs::init_log();

    crate::queries::ldbc::graph::CSR.get_current_partition();

    let mut server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    if config.port != 0 {
        if let Some(mut network) = server_conf.network.take() {
            if let Some(mut servers) = network.servers.take() {
                for i in 0..servers.len() {
                    let addr = servers[i].take();
                    servers[i] =
                        Some(ServerAddr::new(addr.unwrap().get_hostname().to_string(), config.port));
                }
                network.servers = Some(servers);
            }
            server_conf.network = Some(network);
        }
    }

    pegasus::startup(server_conf).ok();
    pegasus::wait_servers_ready(&ServerConf::All);

    let query_start = Instant::now();
    let mut index = 0i32;
    for batch_id in 0..config.batch_count {
        let mut result_set = vec![];
        for i in 0..config.batch_size {
            let mut conf = JobConf::new(index.to_string());
            let result = queries::vertex_count(conf);
            result_set.push(result);
            index += 1;
        }
        for result in result_set {
            for x in result {
                let final_result = x.unwrap();
            }
        }
        // std::thread::sleep(std::time::Duration::from_secs(1));
        println!("Finished run batch {:?}", batch_id);
    }
    pegasus::shutdown_all();
    // dump();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
}