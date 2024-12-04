use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::fs::OpenOptions;
use std::future;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
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
    #[structopt(short = "n", long = "query_name")]
    query_name: String,
    #[structopt(short = "p", long = "params")]
    parameters: String,
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

fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");
    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str()).unwrap();
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

    let query_name = config.query_name;
    let parameters = config.parameters;
    let msg_sender_map = get_msg_sender();
    let recv_register_map = get_recv_register();
    if let Some(queries) = query_register.get_new_query(&query_name) {
        let mut conf = pegasus::JobConf::new(query_name);
        conf.reset_servers(ServerConf::Partial(servers.clone()));
        conf.set_workers(workers);
        for query in queries.iter() {
            let params = HashMap::<String, String>::new();
            let results = {
                pegasus::run(
                    conf.clone(),
                    || {
                        query.Query(
                            conf.clone(),
                            params.clone(),
                            msg_sender_map.clone(),
                            recv_register_map.clone(),
                        )
                    },
                )
                    .expect("submit query failure")
            };
            for result in results {}
        }
    }
    pegasus::shutdown_all();
}