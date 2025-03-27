use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::future;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::io::{self, BufRead, Write};

#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
use shm_graph::graph_modifier::*;
use shm_graph::schema::CsrGraphSchema;
use shm_graph::graph_db::GraphDB;
use pegasus::{Configuration, ServerConf};
use rpc_server::queries::register::{QueryApi, QueryRegister};
use rpc_server::queries::rpc::RPCServerConfig;
use serde::Deserialize;
use structopt::StructOpt;

#[cfg(feature = "use_mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "use_mimalloc_rust")]
use mimalloc_rust::*;
use pegasus_network::{get_msg_sender, get_recv_register, Server};

#[cfg(feature = "use_mimalloc_rust")]
#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers_config")]
    servers_config: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "p", long = "partition_id", default_value = "")]
    partition_id: usize,
    #[structopt(short = "o", long = "port_offset", default_value = "0")]
    offset: u16,
    #[structopt(short = "e", long = "executor_index", default_value = "0")]
    executor_index: u32,
    #[structopt(short = "g", long = "graph_data_dir")]
    graph_data_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PegasusConfig {
    pub worker_num: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub network_config: Option<Configuration>,
    pub pegasus_config: Option<PegasusConfig>,
}

fn main() {
    println!("Start a run query");
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let name = "/SHM_GRAPH_STORE";
    let default_mmap_path = format!("{}/graph_data_bin/partition_{}", config.graph_data_dir, config.partition_id);

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");
    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str()).unwrap();
    let mut server_conf =
        if let Some(servers) = servers_conf.network_config { servers } else { Configuration::singleton() };

    println!("Read servers file");
    let workers = servers_conf
        .pegasus_config
        .expect("Could not read pegasus config")
        .worker_num
        .expect("Could not read worker num");

    let port_offset = config.offset;
    let executor_index = config.executor_index;
    let mut job_id = executor_index as u64 * 150;


    println!("Create server config");
    let mut servers = vec![];
    if let Some(mut network) = server_conf.network.clone() {
        for i in 0..network.servers_size {
            let addr = network.get_server_addr(i as u64).unwrap();
            let mut new_addr = addr.clone();
            new_addr.set_port(addr.get_port() + port_offset);
            network.set_server_addr(i as u64, new_addr);
            servers.push(i as u64);
        }
        server_conf.network = Some(network);
    }
    let servers_len = servers.len();

    println!("Read query config");
    let file = File::open(config.queries_config.clone()).unwrap();
    let queries_config: rpc_server::request::QueriesConfig =
        serde_yaml::from_reader(file).expect("Could not read values");
    let mut input_info = HashMap::<String, Vec<String>>::new();
    if let Some(queries) = queries_config.queries {
        for query in queries {
            let query_name = query.name;
            if let Some(params) = query.params {
                let mut inputs = vec![];
                for param in params {
                    inputs.push(param.name);
                }
                input_info.insert(query_name, inputs);
            }
        }
    }

    let mut query_register = QueryRegister::new();
    println!("Start load lib");
    query_register.load(&PathBuf::from(config.queries_config));
    println!("Finished load libs");

    pegasus::startup(server_conf.clone()).ok();
    pegasus::wait_servers_ready(&ServerConf::All);
    let output_path = format!("/tmp/output{}", executor_index);
    let mut ready_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_path).expect("Failed to open");
    write!(ready_file, "Ready").unwrap();
    drop(ready_file);


    let msg_sender_map = get_msg_sender();
    let recv_register_map = get_recv_register();

    let mut shm_graph = None;
    while true {
        let file_path = format!("/tmp/input{}", executor_index);
        if let Ok(inputs_string) = fs::read_to_string(file_path.clone()) {
            if !inputs_string.is_empty() {
                if shm_graph.is_none() {
                    shm_graph = Some(Arc::new(RwLock::new(GraphDB::<usize, usize>::open(name, Some(&default_mmap_path), config.partition_id))));
                }
                println!("Get input {}", inputs_string);
                let inputs: Vec<String> = inputs_string.split('|').map(|s| s.to_string()).collect();
                let query_name = inputs[0].clone();
                if query_name == "switch" {
                    let mut shared_graph = shm_graph.as_ref().unwrap().write().unwrap();
                    shared_graph.apply_delete_neighbors();
                    drop(shared_graph);
                    let output_path = format!("/tmp/output{}", executor_index);
                    let mut file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(output_path).expect("Failed to open");
                    write!(file, "Finished");
                }
                let mut params = HashMap::<String, String>::new();
                let mut param_index = 1;
                while inputs.len() > param_index + 1 {
                    params.insert(inputs[param_index].clone(), inputs[param_index + 1].clone());
                    param_index += 2;
                }
                let mut file = OpenOptions::new().write(true).truncate(true).open(file_path).unwrap();
                drop(file);
                println!("try run query {}", query_name);
                for (k, v) in params.iter() {
                    println!("{} {}", k, v);
                }
                if let Some((queries, query_type)) = query_register.get_new_query(&query_name) {
                    if query_type == "READ_WRITE" || query_type == "READ" {
                        let mut shared_graph = shm_graph.as_ref().unwrap().write().unwrap();
                        shared_graph.apply_delete_neighbors();
                        drop(shared_graph);
                    }
                    let mut conf = pegasus::JobConf::new(query_name.clone());
                    conf.reset_servers(ServerConf::Partial(servers.clone()));
                    conf.set_workers(workers);
                    job_id += 1;
                    conf.job_id = job_id;
                    for query in queries.iter() {
                        let shared_graph = shm_graph.as_ref().unwrap().read().expect("unknown graph");
                        println!("start run query {}", query_name.clone());
                        let results = {
                            pegasus::run(
                                conf.clone(),
                                || {
                                    query.Query(
                                        conf.clone(),
                                        &shared_graph,
                                        params.clone(),
                                        msg_sender_map.clone(),
                                        recv_register_map.clone(),
                                    )
                                },
                            )
                                .expect("submit query failure")
                        };
                        let output_path = format!("/tmp/output{}", executor_index);
                        let mut file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(output_path).expect("Failed to open");
                        let mut write_operations = vec![];
                        for result in results {
                            if let Ok((worker_id, alias_datas, write_ops, mut query_result)) = result {
                                if let Some(write_ops) = write_ops {
                                    for write_op in write_ops {
                                        write_operations.push(write_op);
                                    }
                                }
                                if let Some(mut query_result) = query_result {
                                    println!("get result {}", String::from_utf8(query_result.clone()).unwrap());
                                    writeln!(file, "{}", String::from_utf8(query_result).unwrap()).unwrap();
                                }
                            }
                        }
                        drop(shared_graph);
                        println!("Write operations size {}", write_operations.len());
                        if write_operations.len() > 0 {
                            let mut shared_graph = shm_graph.as_ref().unwrap().write().unwrap();
                            apply_write_operations(&mut shared_graph, write_operations, servers_len);
                            drop(shared_graph);
                        }
                        println!("Finished run query");
                        write!(file, "Finished").unwrap();
                    }
                }
            } else {
                std::thread::sleep(Duration::from_millis(200));
            }
        } else {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    pegasus::shutdown_all();
    println!("run_query exits");
}
