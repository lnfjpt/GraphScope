use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{App, Arg};
use storage::graph_db::GraphDB;
use storage::traverse::traverse;
use storage::types::*;

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
            Arg::with_name("mmap_dir")
                .short("m")
                .long_help("The directory to place mmap files")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("no_corner")
                .long("no_corner")
                .long_help("Whether shm graph data has corner vertex")
                .takes_value(false),
        ])
        .get_matches();

    let graph_data_dir = matches.value_of("graph_data_dir").unwrap().to_string();
    let output_dir = matches.value_of("output_dir").unwrap().to_string();
    let mmap_dir = matches.value_of("mmap_dir").unwrap().to_string();
    let no_corner = matches.is_present("no_corner");

    let partition_num = get_partition_num(&graph_data_dir);

    let name = "/SHM_GRAPH_STORE_SF10";
    for i in 0..partition_num {
        GraphDB::<usize, usize>::load(graph_data_dir.as_str(), i, name, Some(&mmap_dir));
        let db = GraphDB::<usize, usize>::open(name, Some(&mmap_dir), i);

        if no_corner {
            traverse(&db, format!("{}/part_{}/", output_dir, i).as_str(), false);
        } else {
            traverse(&db, format!("{}/part_{}/", output_dir, i).as_str(), true);
        }
    }
}
