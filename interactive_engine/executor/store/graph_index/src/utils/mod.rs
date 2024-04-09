use csv::{ReaderBuilder, StringRecord};
use rayon::prelude::*;
use bmcsr::columns::{Item, DataType};
use bmcsr::error::GDBResult;
use bmcsr::ldbc_parser::LDBCVertexParser;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use rust_htslib::bgzf::Reader as GzReader;
use std::fs::File;
use crate::types::ArrayData;

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

pub fn get_2d_partition(id_hi: u64, id_low: u64, workers: usize, num_servers: usize) -> u64 {
    let server_id = id_hi % num_servers as u64;
    let worker_id = id_low % workers as u64;
    server_id * workers as u64 + worker_id
}

fn create_array_data(headers: &[(String, DataType)]) -> Vec<ArrayData> {
    let mut array_datas = vec![];
    for (_, data_type) in headers {
        match data_type {
            DataType::Int32 => array_datas.push(ArrayData::Int32Array(vec![])),
            DataType::UInt64 => array_datas.push(ArrayData::Uint64Array(vec![])),
            _ => panic!("Unsupport data type")
        }
    }
    array_datas
}

pub fn parse_vertex_file(path: &PathBuf, skip_header: bool, delim: u8, parser: LDBCVertexParser<usize>, mappings: Vec<i32>,
                         headers: &[(String, DataType)], id: u32, parallel: u32) -> (Vec<usize>, Option<Vec<ArrayData>>) {
    let path_str = path.clone().to_str().map(|s| s.to_string()).unwrap();
    let mut global_ids = vec![];
    let mut properties = create_array_data(headers);
    if path_str.ends_with(".csv.gz") {
        if let Ok(gz_reader) = GzReader::from_path(&path) {
            let mut rdr = ReaderBuilder::new()
                .delimiter(delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(skip_header)
                .from_reader(gz_reader);
            for (index, result) in rdr.records().enumerate() {
                if index % (id as usize) != 0 {
                    continue;
                }
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    global_ids.push(vertex_meta.global_id);
                    if let Ok(mut property) =
                        parse_properties(&record, headers, mappings.as_slice()) {
                        for (i, data) in property.drain(..).enumerate() {
                            properties[i].push_item(data);
                        }
                    }
                }
            }
        }
    } else if path_str.ends_with(".csv") {
        if let Ok(file) = File::open(&path) {
            let reader = BufReader::new(file);
            let mut rdr = ReaderBuilder::new()
                .delimiter(delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(skip_header)
                .from_reader(reader);
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    global_ids.push(vertex_meta.global_id);
                    if let Ok(mut property) =
                        parse_properties(&record, headers, mappings.as_slice()) {
                        for (i, data) in property.drain(..).enumerate() {
                            properties[i].push_item(data);
                        }
                    }
                }
            }
        }
    }
    if properties.is_empty() {
        (global_ids, None)
    } else {
        (global_ids, Some(vec![]))
    }
}

pub fn parse_properties(
    record: &StringRecord, header: &[(String, DataType)], selected: &[i32],
) -> GDBResult<Vec<Item>> {
    let mut properties = Vec::new();
    for (index, val) in record.iter().enumerate() {
        if selected[index] > 0 {
            match header[index].1 {
                DataType::Int32 => {
                    properties.push(Item::Int32(val.parse::<i32>()?));
                }
                DataType::UInt32 => {
                    properties.push(Item::UInt32(val.parse::<u32>()?));
                }
                DataType::Int64 => {
                    properties.push(Item::Int64(val.parse::<i64>()?));
                }
                DataType::UInt64 => {
                    properties.push(Item::UInt64(val.parse::<u64>()?));
                }
                DataType::String => {
                    properties.push(Item::String(val.to_string()));
                }
                DataType::Date => {
                    properties.push(Item::Date(bmcsr::date::parse_date(val)?));
                }
                DataType::DateTime => {
                    properties.push(Item::DateTime(bmcsr::date_time::parse_datetime(val)));
                }
                DataType::Double => {
                    properties.push(Item::Double(val.parse::<f64>()?));
                }
                DataType::NULL => {
                    error!("Unexpected field type");
                }
                DataType::ID => {}
                DataType::LCString => {
                    properties.push(Item::String(val.to_string()));
                }
            }
        }
    }
    Ok(properties)
}