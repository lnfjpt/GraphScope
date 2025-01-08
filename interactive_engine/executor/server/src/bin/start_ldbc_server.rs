use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use shm_graph::graph_db::GraphDB;
#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
use pegasus::{Configuration, ServerConf};
use rpc_server::queries;
use rpc_server::queries::register::QueryRegister;
use rpc_server::queries::rpc::RPCServerConfig;
use serde::Deserialize;
use structopt::StructOpt;

#[cfg(feature = "use_mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "use_mimalloc_rust")]
use mimalloc_rust::*;

#[cfg(feature = "use_mimalloc_rust")]
#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data")]
    graph_data: PathBuf,
    #[structopt(short = "s", long = "servers_config")]
    servers_config: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "p", long = "partition_id", default_value = "0")]
    partition_id: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PegasusConfig {
    pub worker_num: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub network_config: Option<Configuration>,
    pub rpc_server: Option<RPCServerConfig>,
    pub pegasus_config: Option<PegasusConfig>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let graph_data_str = config.graph_data.to_str().unwrap();

    // let shared_graph = Arc::new(RwLock::new(GraphDB::<usize, usize>::open(graph_data_str, config.partition_id)));
    let name = "/SHM_GRAPH_STORE";
    GraphDB::<usize, usize>::load(graph_data_str, config.partition_id, name);
    let shared_graph =
        Arc::new(RwLock::new(GraphDB::<usize, usize>::open(graph_data_str, config.partition_id)));

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");

    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str())?;
    let server_conf =
        if let Some(servers) = servers_conf.network_config { servers } else { Configuration::singleton() };

    let workers = servers_conf
        .pegasus_config
        .expect("Could not read pegasus config")
        .worker_num
        .expect("Could not read worker num");

    let mut servers = vec![];
    if let Some(network) = &server_conf.network {
        for i in 0..network.servers_size {
            servers.push(i as u64);
        }
    }

    let mut query_register = QueryRegister::new();
    println!("Start load lib");
    query_register.load(&PathBuf::from(config.queries_config));
    println!("Finished load libs");

    let rpc_config = servers_conf
        .rpc_server
        .expect("Rpc config not set");
    pegasus::startup(server_conf.clone()).ok();
    pegasus::wait_servers_ready(&ServerConf::All);
    queries::rpc::start_all(
        rpc_config,
        server_conf,
        query_register,
        0,
        workers,
        servers,
        None,
        config.partition_id
    )
    .await?;
    Ok(())
}
