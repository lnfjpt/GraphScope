extern crate chrono;

use self::chrono::Datelike;
use chrono::offset::{TimeZone, Utc};
use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::configure_with_default;
use std::path::Path;

lazy_static! {
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = _init_graph();
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
}

fn _init_graph() -> LargeGraphDB<DefaultId, InternalId> {
    println!("Read the graph data from {:?} for demo.", *DATA_PATH);
    GraphDBConfig::default()
        .root_dir(&(*DATA_PATH))
        .partition(*PARTITION_ID)
        .schema_file(
            &(DATA_PATH.as_ref() as &Path)
                .join(DIR_GRAPH_SCHEMA)
                .join(FILE_SCHEMA),
        )
        .open()
        .expect("Open graph error")
}

/// Typical symbols that split a string-format of a time data.
fn is_time_splitter(c: char) -> bool {
    c == '-' || c == ':' || c == ' ' || c == 'T' || c == 'Z' || c == '.'
}

pub fn parse_datetime(val: &str) -> GDBResult<u64> {
    let mut dt_str = val;
    #[allow(unused_assignments)]
    let mut s = String::new();
    let mut is_millis = false;
    if let Ok(millis) = dt_str.parse::<i64>() {
        if let Some(dt) = Utc.timestamp_millis_opt(millis).single() {
            if dt.year() > 1970 && dt.year() < 2030 {
                s = dt.to_rfc3339();
                dt_str = s.as_ref();
                is_millis = true;
            }
        }
    }
    let mut _time = String::with_capacity(dt_str.len());
    for c in dt_str.chars() {
        if c == '+' {
            // "2012-07-21T07:59:14.322+000", skip the content after "."
            break;
        } else if is_time_splitter(c) {
            continue; // replace any time splitter with void
        } else {
            _time.push(c);
        }
    }

    if is_millis {
        // pad '0' if not 'yyyyMMddHHmmssSSS'
        while _time.len() < 17 {
            // push the SSS to fill the datetime as the required format
            _time.push('0');
        }
    }
    Ok(_time.parse::<u64>()?)
}

pub fn get_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}
