use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use actix_web::{web, App, HttpServer};
#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
use pegasus::{Configuration, ServerConf};
use rpc_server::proxy::*;
use rpc_server::queries;
use rpc_server::queries::register::QueryRegister;
use rpc_server::queries::rpc::RPCServerConfig;
use rpc_server::request::JobClient;
use serde::Deserialize;
use shm_graph::graph_db::GraphDB;
// use shm_graph::traverse::traverse;
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
    graph_data: Option<PathBuf>,
    #[structopt(short = "s", long = "servers_config")]
    servers_config: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "p", long = "partition_id", default_value = "0")]
    partition_id: usize,
    #[structopt(short = "t", long = "pool_size", default_value = "0")]
    pool_size: u32
}

#[derive(Debug, Deserialize, Clone)]
pub struct PegasusConfig {
    pub worker_num: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpServerConfig {
    pub http_host: Option<String>,
    pub http_port: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub network_config: Option<Configuration>,
    pub rpc_server: Option<RPCServerConfig>,
    pub http_server: Option<HttpServerConfig>,
    pub pegasus_config: Option<PegasusConfig>,
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let graph_data_str = config.graph_data.to_str().unwrap();
    let pool_size = config.pool_size;

    let start = Instant::now();
    let name = "/SHM_GRAPH_STORE";
    // let mut is_load = false;
    if let Some(graph_data_path) = config.graph_data {
        let graph_data_str = graph_data_path.to_str().unwrap();
        GraphDB::<usize, usize>::load(graph_data_str, config.partition_id, name);
        println!("load graph takes: {} s", start.elapsed().as_secs_f64());
        // is_load = true;
    }
    let start = Instant::now();
    println!("open graph takes: {} s", start.elapsed().as_secs_f64());

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");

    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str())?;
    let server_conf =
        if let Some(servers) = servers_conf.network_config { servers } else { Configuration::singleton() };
    let proxy_endpoint = if let Some(http_server) = servers_conf.http_server {
        Some(http_server.http_host.unwrap() + ":" + &http_server.http_port.unwrap().to_string())
    } else {
        None
    };

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

    let rpc_config = servers_conf
        .rpc_server
        .expect("Rpc config not set");
    let rpc_endpoint =
        format!("https://{}:{}", rpc_config.rpc_host.as_ref().unwrap(), rpc_config.rpc_port.unwrap());
    // pegasus::startup(server_conf.clone()).ok();
    // pegasus::wait_servers_ready(&ServerConf::All);
    let _rpc_server_handler = tokio::spawn(queries::rpc::start_all(
        rpc_config,
        server_conf,
        query_register,
        pool_size,
        workers,
        servers,
        Some(shared_graph),
    ));

    if let Some(proxy_endpoint) = proxy_endpoint {
        let rpc_client = JobClient::new(rpc_endpoint, proxy_endpoint.clone(), input_info, workers).await?;
        let shared_client = web::Data::new(Mutex::new(rpc_client));
        let index = web::Data::new(AtomicU64::new(0));
        let _http_server_handler = tokio::spawn(
            HttpServer::new(move || {
                App::new()
                    .app_data(index.clone())
                    .app_data(shared_client.clone())
                    .service(get_status)
                    .service(submit_query)
            })
            .bind(&proxy_endpoint)?
            .run(),
        );
    }
    let shutdown_handle = tokio::spawn(async {
        shutdown_signal().await;
    });
    shutdown_handle.await?;
    Ok(())
}
