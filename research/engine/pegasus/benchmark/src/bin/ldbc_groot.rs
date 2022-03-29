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
    #[structopt(short = "p", long = "print")]
    print_result: bool,
}

fn main() {
    let config: Config = Config::from_args();

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let query_path = config.query_path;
    let mut queries = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        queries.push(line.unwrap());
    }
    let query_start = Instant::now();
    let mut index = 0;
    for query in queries {
        let split = query.trim().split(",").collect::<Vec<&str>>();
        let query_name = split[0].clone();
        let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
        conf.set_workers(config.workers);
        match split[0] {
            "is1" => {
                println!("Start run query \"Interactive Short 1\"");
                queries::is1_groot(conf, split[1].parse::<i64>().unwrap());
                ()
            }
            "is2" => {
                println!("Start run query \"Interactive Short 2\"");
                queries::is2_groot(conf, split[1].parse::<i64>().unwrap());
                ()
            }
            "is3" => {
                println!("Start run query \"Interactive Short 3\"");
                queries::is3_groot(conf, split[1].parse::<i64>().unwrap());
                ()
            }
            "ic2" => {
                println!("Start run query \"Interactive Complex 2\"");
                let result = queries::ic2_groot(conf, split[1].parse::<i64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, message_id, content, create_date) =
                            x.unwrap();
                        println!(
                            "{} {} {} {} {} {}",
                            friend_id, first_name, last_name, message_id, content, create_date
                        );
                    }
                }
                ()
            }
            _ => println!("Unknown query"),
        }
        index += 1;
    }
    pegasus::shutdown_all();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
}
