use clap::{App, Arg};
use std::ops::Index;
use std::path::{Path, PathBuf};

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
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
            Arg::with_name("insert_schema_file")
                .long_help("The insert schema file")
                .required(true)
                .takes_value(true)
                .index(4),
            Arg::with_name("delete_schema_file")
                .long_help("The delete schema file")
                .required(true)
                .takes_value(true)
                .index(5),
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
    let insert_schema_file = matches
        .value_of("insert_schema_file")
        .unwrap()
        .to_string();
    let delete_schema_file = matches
        .value_of("delete_schema_file")
        .unwrap()
        .to_string();

    let mut graph = GraphDB::<usize, usize>::deserialize(&graph_data_dir, 0, None).unwrap();

    let batches = ["2012-11-29", "2012-11-30", "2012-12-01", "2012-12-02", "2012-12-03", "2012-12-04", "2012-12-05", "2012-12-06", "2012-12-07", "2012-12-08", "2012-12-09", "2012-12-10", "2012-12-11", "2012-12-12", "2012-12-13", "2012-12-14", "2012-12-15", "2012-12-16", "2012-12-17", "2012-12-18", "2012-12-19", "2012-12-20", "2012-12-21", "2012-12-22", "2012-12-23", "2012-12-24", "2012-12-25", "2012-12-26", "2012-12-27", "2012-12-28", "2012-12-29", "2012-12-30", "2012-12-31"];
    // let batches = ["2012-11-29"];
    for batch_id in batches.iter() {
        let batch_id = batch_id.to_string();
        let insert_schema_file_path = PathBuf::from(insert_schema_file.clone() + "-" + batch_id.as_str() + ".json");
        let delete_schema_file_path = PathBuf::from(delete_schema_file.clone() + "-" + batch_id.as_str() + ".json");

        let mut graph_modifier = GraphModifier::new(&input_dir);
        graph_modifier.skip_header();
        graph_modifier.insert(&mut graph, &insert_schema_file_path).unwrap();

        let mut delete_generator = DeleteGenerator::new(PathBuf::from(&input_dir));
        delete_generator.skip_header();
        delete_generator.generate(&mut graph, batch_id.as_str());

        graph_modifier.delete(&mut graph, &delete_schema_file_path).unwrap();
        // let modified_output = output_dir.to_string().clone() + "/" + batch_id.as_str();
        // std::fs::create_dir_all(&modified_output).unwrap();
        // traverse(&graph, &modified_output);
    }
    // let init_output = output_dir.to_string().clone() + "/init";
    // std::fs::create_dir_all(&init_output).unwrap();
    // traverse(&graph, &init_output);

    // let insert_schema_file_path = PathBuf::from(&insert_schema_file);

    // let mut graph_modifier = GraphModifier::new(&input_dir);
    // graph_modifier.skip_header();
    // graph_modifier.insert(&mut graph, &insert_schema_file_path).unwrap();

    // let modified_output = output_dir.to_string().clone() + "/modified";
    // std::fs::create_dir_all(&modified_output).unwrap();
    // traverse(&graph, &modified_output);

    // let mut delete_generator = DeleteGenerator::new(PathBuf::from(&input_dir));
    // delete_generator.skip_header();
    // let batch_id = "2012-11-29";
    // delete_generator.generate(&mut graph, batch_id);

    // graph_modifier.delete(&mut graph, &PathBuf::from(delete_schema_file)).unwrap();

    // let modified2_output = output_dir.to_string().clone() + "/modified2";
    // std::fs::create_dir_all(&modified2_output).unwrap();
    // traverse(&graph, &modified2_output);
}
