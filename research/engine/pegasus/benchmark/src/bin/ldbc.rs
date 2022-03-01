use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::configure_with_default;
use pegasus::{Configuration, JobConf};
use pegasus_benchmark::queries;
use std::path::Path;
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "d", long = "data_path")]
    data_path: String,
    #[structopt(short = "p", long = "partition", default_value = "0")]
    partition: usize,
    #[structopt(short = "q", long = "query")]
    query: String,
}

fn load_graph(data_path: String, partition_id: usize) -> LargeGraphDB<DefaultId, InternalId> {
    println!("Read the graph data from {:?} for demo.", data_path);
    GraphDBConfig::default()
        .root_dir(data_path.clone())
        .partition(partition_id)
        .schema_file(
            &(data_path.as_ref() as &Path)
                .join(DIR_GRAPH_SCHEMA)
                .join(FILE_SCHEMA),
        )
        .open()
        .expect("Open graph error")
}

fn main() {
    let config: Config = Config::from_args();

    let data_path = config.data_path;
    let partition = config.partition;
    let graph = load_graph(data_path.clone(), partition);

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let query_name = config.query;
    let mut conf = JobConf::new(query_name.clone());
    conf.set_workers(config.workers);

    let query_start = Instant::now();
    match query_name.as_str() {
        "is1" => {
            println!("Start run query \"Interactive Short 1\"");
            queries::is1(conf, graph, 0);
            ()
        }
        "ic1" => println!("Start run query \"Interactive Complex 1\""),
        _ => println!("Unknown query"),
    }
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
}
