use shm_graph::graph_db::GraphDB;
use clap::{App, Arg};
use shm_graph::types::*;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use shm_graph::schema::{CsrGraphSchema, Schema};
use std::collections::HashMap;
use std::fs::File;
use shm_graph::ldbc_parser::LDBCVertexParser;
use shm_graph::columns::DataType;
use std::io::Write;

fn get_partition_num(graph_data_dir: &String) -> usize {
    let root_dir = PathBuf::from_str(graph_data_dir.as_str()).unwrap();
    let partitions_dir = root_dir.join(DIR_BINARY_DATA);
    let mut index = 0_usize;
    loop {
        let partition_dir = partitions_dir.join(format!("partition_{}", index));
        let b = Path::new(partition_dir.to_str().unwrap()).is_dir();
        if b {
            index += 1;
        } else {
            return index;
        }
    }
}

fn output_vertices(graph: &GraphDB, output_dir: &String, files: &mut HashMap<LabelId, File>) {
    let vertex_label_names = graph.graph_schema.vertex_label_names();
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    for n in vertex_label_names.iter() {
        if let Some(v_label) = graph
            .graph_schema
            .get_vertex_label_id(n.as_str())
        {
            println!("outputing vertex-{}, size {}", n, graph.get_vertices_num(v_label));
            let header = graph
                .graph_schema
                .get_vertex_header(v_label)
                .unwrap();
            if !files.contains_key(&v_label) {
                let file = File::create(output_dir_path.join(n.as_str())).unwrap();
                files.insert(v_label.clone(), file);
            }
            let file = files.get_mut(&v_label).unwrap();

            for v in graph.get_all_vertices(v_label) {
                let global_id = graph.vertex_map.get_global_id(v_label, v.get_index()).unwrap();
                let id = LDBCVertexParser::<DefaultId>::get_original_id(global_id);
                write!(file, "\"{}\"", id.to_string()).unwrap();
                for c in header {
                    if c.1 != DataType::ID {
                        write!(
                            file,
                            "|\"{}\"",
                            graph.vertex_prop_table[v_label as usize].get_item(c.0.as_str(), v.get_index()).unwrap().to_string()
                        )
                        .unwrap();
                    }
                }
                writeln!(file).unwrap();
            }
        }
    }
}

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
            Arg::with_name("graph_name")
                .short("n")
                .long_help("The name of graph")
                .required(true).takes_value(true)
                .index(2),
            Arg::with_name("output_dir")
                .short("o")
                .long_help("The directory to place output files")
                .required(true)
                .takes_value(true)
                .index(3),
        ])
        .get_matches();

    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let graph_name = matches
        .value_of("graph_name")
        .unwrap()
        .to_string();
    let output_dir = matches
        .value_of("output_dir")
        .unwrap()
        .to_string();

    let partition_num = get_partition_num(&graph_data_dir);

    let schema_path = PathBuf::from_str(graph_data_dir.as_str()).unwrap().join(DIR_GRAPH_SCHEMA).join(FILE_SCHEMA);
    let graph_schema = CsrGraphSchema::from_json_file(schema_path).unwrap();

    let mut v_files = HashMap::<LabelId, File>::new();
    for i in 0..partition_num {
        let db = GraphDB::<usize, usize>::open(graph_name.as_str(), graph_schema.clone(), i);
        output_vertices(&db, &output_dir, &mut v_files);
    }
}