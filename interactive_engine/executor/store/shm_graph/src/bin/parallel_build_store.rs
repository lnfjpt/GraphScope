use std::fmt::format;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::io::{BufReader, BufRead};

use clap::{App, Arg};
use env_logger;
use shm_graph::graph_loader_beta::{load_graph, load_graph_no_corner};
use shm_graph::shuffler::Shuffler;
use shm_graph::types::*;
use shm_graph::schema::{CsrGraphSchema, InputSchema};
use shm_graph::vertex_loader::load_vertex;

fn read_hostfile(file_path: &str) -> Vec<String> {
    const SERVER_PORT: u32 = 12355;
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);

    let lines: Vec<String> = reader.lines()
        .filter_map(Result::ok)
        .collect();

    let results: Vec<String> = lines.iter().map(|v| format!("{}:{}", v, SERVER_PORT)).collect();

    results
}

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("raw_data_dir")
                .short("r")
                .long_help("The directory to the raw data")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("graph_data_dir")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("input_schema_file")
                .long_help("The input schema file")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("graph_schema_file")
                .long_help("The graph schema file")
                .required(true)
                .takes_value(true)
                .index(4),
            Arg::with_name("partition")
                .short("p")
                .long_help("The number of partitions")
                .required(true)
                .takes_value(true)
                .index(5),
            Arg::with_name("index")
                .short("i")
                .long_help("The index of partitions")
                .required(true)
                .takes_value(true)
                .index(6),
            Arg::with_name("hostfile")
                .long("hostfile")
                .long_help("hostfile")
                .required(true)
                .takes_value(true)
                .index(7),
            Arg::with_name("read_concurrency")
                .long("read_concurrency")
                .long_help("read concurrency")
                .required(true)
                .takes_value(true)
                .index(8),
            Arg::with_name("offset")
                .long("offset")
                .long_help("offset of files read by this worker")
                .required(true)
                .takes_value(true)
                .index(9),
            Arg::with_name("delimiter")
                .short("t")
                .long_help(
                    "The delimiter of the raw data [comma|semicolon|pipe]. pipe (|) is the default option",
                )
                .takes_value(true),
            Arg::with_name("skip_header")
                .long("skip_header")
                .long_help("Whether skip the first line in input file")
                .takes_value(false),
            Arg::with_name("no_corner")
                .long("no_corner")
                .long_help("Whether shm graph data has corner vertex")
                .takes_value(false),
        ])
        .get_matches();

    let raw_data_dir = matches
        .value_of("raw_data_dir")
        .unwrap()
        .to_string();
    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let input_schema_file = matches
        .value_of("input_schema_file")
        .unwrap()
        .to_string();
    let graph_schema_file = matches
        .value_of("graph_schema_file")
        .unwrap()
        .to_string();
    let partition_num = matches
        .value_of("partition")
        .unwrap_or("1")
        .parse::<usize>()
        .expect(&format!("Specify invalid partition number"));
    let partition_index = matches
        .value_of("index")
        .unwrap_or("0")
        .parse::<usize>()
        .expect(&format!("Specify invalid partition number"));

    let delimiter_str = matches
        .value_of("delimiter")
        .unwrap_or("pipe")
        .to_uppercase();

    let skip_header = matches.is_present("skip_header");

    let no_corner = matches.is_present("no_corner");

    let delimiter = if delimiter_str.as_str() == "COMMA" {
        b','
    } else if delimiter_str.as_str() == "SEMICOLON" {
        b';'
    } else {
        b'|'
    };

    let hostfile = matches.value_of("hostfile").unwrap().to_string();
    let reader_num = matches.value_of("read_concurrency").unwrap().parse::<usize>().expect("read concurrency should be a number");
    let offset = matches.value_of("offset").unwrap().parse::<usize>().expect("offset should be a number");

    let addrs = read_hostfile(&hostfile);
    let input_dir = PathBuf::from(raw_data_dir);
    let output_dir = PathBuf::from(graph_data_dir);
    let input_schema_file = PathBuf::from(input_schema_file);
    let graph_schema_file = PathBuf::from(graph_schema_file);
    if no_corner {
        load_graph_no_corner(
            input_dir,
            output_dir,
            input_schema_file,
            graph_schema_file,
            addrs,
            partition_index,
            partition_num,
            delimiter,
            skip_header,
            reader_num,
            offset);
    } else {
        load_graph(
            input_dir,
            output_dir,
            input_schema_file,
            graph_schema_file,
            addrs,
            partition_index,
            partition_num,
            delimiter,
            skip_header,
            reader_num,
            offset);
    }
}