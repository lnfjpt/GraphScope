use graph_store::prelude::*;
use pegasus::{Configuration, JobConf};
use pegasus_benchmark::queries;
use std::fs::File;
use std::io::{self, BufRead};
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "q", long = "query")]
    query_path: String,
}

fn main() {
    let config: Config = Config::from_args();

    crate::queries::ldbc::graph::GRAPH.get_current_partition();

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let query_name = "is";
    let mut conf = JobConf::new(query_name.clone());
    conf.set_workers(config.workers);

    let query_path = config.query_path;
    let mut queries = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        queries.push(line.unwrap());
    }
    let query_start = Instant::now();
    for query in queries {
        let split = query.trim().split(",").collect::<Vec<&str>>();
        match split[0] {
            "is1" => {
                println!("Start run query \"Interactive Short 1\"");
                queries::is1(conf.clone(), split[1].parse::<u64>().unwrap());
                ()
            }
            "is3" => {
                println!("Start run query \"Interactive Short 3\"");
                queries::is3(conf.clone(), split[1].parse::<u64>().unwrap());
                ()
            }
            "is4" => {
                println!("Start run query \"Interactive Short 4\"");
                queries::is4(conf.clone(), split[1].parse::<u64>().unwrap());
                ()
            }
            "is5" => {
                println!("Start run query \"Interactive Short 5\"");
                queries::is5(conf.clone(), split[1].parse::<u64>().unwrap());
                ()
            }
            "ic1" => println!("Start run query \"Interactive Complex 1\""),
            _ => println!("Unknown query"),
        }
    }
    pegasus::shutdown_all();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
}
