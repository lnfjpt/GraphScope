#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::fs::File;
use std::io::{self, BufRead};
use std::time::Instant;

use serde::Deserialize;
use structopt::StructOpt;

use bmcsr::graph_db::GraphDB;
use graph_index::GraphIndex;
use pegasus::{Configuration, ServerConf, JobConf};

use rpc_server::queries;
use rpc_server::queries::register::QueryRegister;
use rpc_server::queries::rpc::RPCServerConfig;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data")]
    graph_data: PathBuf,
    #[structopt(short = "s", long = "servers_config")]
    servers_config: PathBuf,
    #[structopt(short = "q", long = "query")]
    query_path: String,
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

lazy_static! {
    pub static ref DATA_PATH: String = std::env::var("CSR_PATH")
            .map(|s| s.parse::<String>().unwrap_or("".to_string())).unwrap_or("".to_string());
    pub static ref GRAPH: GraphDB<usize, usize> = _init_graph();
}

fn _init_graph() -> GraphDB<usize, usize> {
    println!("Read the graph data from {:?} for demo.", *DATA_PATH);
    GraphDB::<usize, usize>::deserialize(&DATA_PATH, 0, None).unwrap()
}



fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    lazy_static::initialize(&GRAPH);
    let shared_graph_index = Arc::new(RwLock::new(GraphIndex::new(0)));

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

    let query_path = config.query_path;
    let mut queries = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    let mut header = vec![];
    for (i, line) in lines.enumerate() {
        if i == 0 {
            let line = line.unwrap();
            let mut split = line.trim().split("|").collect::<Vec<&str>>();
            for head in split.drain(1..) {
                header.push(head.to_string());
            }
            continue;
        }
        queries.push(line.unwrap());
    }

    let mut index = 0i32;
    for query in queries {
        let mut params = HashMap::new();
        let mut split = query.trim().split("|").collect::<Vec<&str>>();
        let query_name = split[0].clone();
        for (i, param) in split.drain(1..).enumerate() {
            params.insert(header[i].clone(), param.to_string());
        }
        let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
        conf.set_workers(workers);
        conf.reset_servers(ServerConf::All);
        match split[0] {
            "bi9" => {
                println!("Start run query \"BI 9\"");
                let graph_index = shared_graph_index.read().unwrap();
                let result = queries::bi9(conf, &GRAPH, &graph_index, params);
                let mut result_list = vec![];
                for x in result {
                    let ret =
                        x.unwrap();
                    result_list.push(String::from_utf8(ret).unwrap());
                }
                println!("{:?}", result_list);
                ()
            }
            _ => println!("Unknown query")
        }
    }
    Ok(())
}
