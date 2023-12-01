use std::fs::File;
use std::io::{self, BufRead};
use std::os::raw::{c_char, c_int};
use std::os::unix::thread;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use itertools::Itertools;
use mcsr::graph_db::GlobalCsrTrait;
use pegasus::api::{Fold, Limit, Map, Sink, SortLimitBy};
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_benchmark::queries;
use pegasus_network::config::{NetworkConfig, ServerAddr};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

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

fn main() {
    let config: Config = Config::from_args();

    pegasus_common::logs::init_log();

    lazy_static::initialize(&crate::queries::ldbc::graph::CSR);
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

    let mut conf = JobConf::new("limit-test");
    conf.set_workers(config.workers);
    conf.plan_print = true;

    let workers_num = conf.workers;
    let result = pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0i64])?;
            let worker_id = input.get_worker_index();
            stream
                .flat_map(move |_| {
                    Ok(crate::queries::ldbc::graph::CSR
                        .get_partitioned_vertices(None, worker_id, workers_num)
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .limit(1)?
                .sink_into(output)
        }
    })
    .expect("submit test job failure");

    for _ in result {}

    pegasus::shutdown_all();
}
