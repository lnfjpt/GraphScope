use clap::{App, Arg};
use std::ops::Index;
use std::path::PathBuf;

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::GraphModifier;
use bmcsr::traverse::traverse;
use bmcsr::types::{DefaultId, LabelId, DIR_BINARY_DATA, NAME, VERSION};

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("graph_data_dir")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("output_dir")
                .short("o")
                .long_help("The directory to place output files")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("input_dir")
                .short("i")
                .long_help("The directory to place input files")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("graph_schema_file")
                .long_help("The graph schema file")
                .required(true)
                .takes_value(true)
                .index(4),
            Arg::with_name("insert_schema_file")
                .long_help("The insert schema file")
                .required(true)
                .takes_value(true)
                .index(5),
            Arg::with_name("batch_id")
                .short("b")
                .long_help("The batch id")
                .required(true)
                .takes_value(true)
                .index(6),
        ])
        .get_matches();

    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let output_dir = matches
        .value_of("output_dir")
        .unwrap()
        .to_string();
    let input_dir = matches
        .value_of("input_dir")
        .unwrap()
        .to_string();
    let graph_schema_file = matches
        .value_of("graph_schema_file")
        .unwrap()
        .to_string();
    let insert_schema_file = matches
        .value_of("insert_schema_file")
        .unwrap()
        .to_string();
    let batch_id = matches
        .value_of("batch_id")
        .unwrap()
        .to_string();

    let mut graph = GraphDB::deserialize(&graph_data_dir, 0, None).unwrap();

    let init_output = output_dir.to_string().clone() + "/init";
    std::fs::create_dir_all(&init_output).unwrap();
    traverse(&graph, &init_output);

    let insert_schema_file_path = PathBuf::from(&insert_schema_file);

    let mut graph_modifier = GraphModifier::<DefaultId>::new(input_dir, graph_schema_file, 0, 1);
    let batch = format!("batch_id={}", batch_id);
    graph_modifier
        .modify(&mut graph, batch.as_str(), &insert_schema_file_path)
        .unwrap();

    let modified_output = output_dir.to_string().clone() + "/modified";
    std::fs::create_dir_all(&modified_output).unwrap();
    traverse(&graph, &modified_output);
}
