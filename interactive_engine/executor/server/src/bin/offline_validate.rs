use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;

use pegasus::JobConf;
use serde::Deserialize;
use serde_json::json;
use structopt::StructOpt;

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
use bmcsr::schema::InputSchema;
use graph_index::GraphIndex;
use rpc_server::queries::register::QueryRegister;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data", default_value = "")]
    graph_data: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "p", long = "parameters", default_value = "")]
    parameters: PathBuf,
    #[structopt(short = "r", long = "graph_raw_data", default_value = "")]
    graph_raw: PathBuf,
    #[structopt(short = "b", long = "batch_update_configs", default_value = "")]
    batch_update_configs: PathBuf,
    #[structopt(short = "o", long = "output_dir", default_value = "")]
    output_dir: PathBuf,
    #[structopt(short = "w", long = "worker_num", default_value = "8")]
    worker_num: u32,
}

fn parse_input(path: &PathBuf) -> Vec<(String, HashMap<String, String>)> {
    let mut ret = vec![];

    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);

    for result in reader.lines() {
        let record = result.unwrap();
        let parts: Vec<String> = record
            .split('|')
            .map(|s| s.to_string())
            .collect();
        assert_eq!(parts.len(), 2);
        let query_name = format!("bi{}", parts[0]);
        let parsed: HashMap<String, String> = serde_json::from_str(parts[1].as_str()).unwrap();

        println!("{}: {:?}", query_name, parsed);

        ret.push((query_name, parsed))
    }

    ret
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let graph_data_str = config.graph_data.to_str().unwrap();
    let mut graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let mut graph_index = GraphIndex::new(0);

    let mut query_register = QueryRegister::new();
    query_register.load(&PathBuf::from(&config.queries_config));

    let graph_raw = config.graph_raw;
    let batch_configs = config.batch_update_configs;

    let batch_id = "2012-11-29";

    let insert_file_name = format!("insert-{}.json", &batch_id);
    let insert_schema_file_path = batch_configs.join(insert_file_name);
    let delete_file_name = format!("delete-{}.json", &batch_id);
    let delete_schema_file_path = batch_configs.join(delete_file_name);

    let mut graph_modifier = GraphModifier::new(&graph_raw);
    graph_modifier.skip_header();
    let insert_schema = InputSchema::from_json_file(insert_schema_file_path, &graph.graph_schema).unwrap();
    graph_modifier
        .insert(&mut graph, &insert_schema)
        .unwrap();
    let mut delete_generator = DeleteGenerator::new(&graph_raw);
    delete_generator.skip_header();
    delete_generator.generate(&mut graph, &batch_id);

    let delete_schema = InputSchema::from_json_file(delete_schema_file_path, &graph.graph_schema).unwrap();
    graph_modifier
        .delete(&mut graph, &delete_schema)
        .unwrap();

    let input_parameters = parse_input(&config.parameters);

    if !config.queries_config.is_empty() {
        query_register.run_precomputes(&graph, &mut graph_index, config.worker_num);
    }

    for (query_name, input) in input_parameters.iter() {
        let query = query_register
            .get_query(query_name)
            .expect("Could not find query");
        let mut conf = JobConf::new(query_name.clone());
        conf.set_workers(config.worker_num);
        conf.reset_servers(pegasus::ServerConf::Partial(vec![0]));
        let result = {
            pegasus::run(conf.clone(), || query.Query(conf.clone(), &graph, &graph_index, HashMap::new()))
                .expect("submit query failure")
        };
        let mut index = 0;
        for x in result {
            let ret = x.expect("Fail to get result");
            let ret = String::from_utf8(ret).unwrap();
            println!("{} - {:?}: {} -> {}", query_name, input, index, ret);
            index += 1;
        }
        break;
    }

    Ok(())
}
