use std::collections::HashSet;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Mutex, Arc, mpsc};
use std::thread;
use std::fs::File;

use csv::{ReaderBuilder, Reader, StringRecord};
use rust_htslib::bgzf::Reader as GzReader;
use serde::{Deserialize, Serialize};

use crate::columns::DataType;
use crate::ldbc_parser::LDBCVertexParser;
use crate::record_batch::{RecordBatch, RecordBatchWriter};
use crate::shuffler::{Message, Shuffler};
use crate::graph::IndexType;
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::LabelId;
use crate::graph_loader::get_files_list;

#[derive(Serialize, Deserialize)]
struct VertexBatch {
    id: Vec<usize>,
    properties: RecordBatch,
}

impl VertexBatch {
    pub fn new(cols: &[DataType]) -> Self {
        Self {
            id: Vec::new(),
            properties: RecordBatch::new(cols),
        }
    }
}

fn get_partition(vid: usize, part_num: usize) -> usize {
    vid % part_num
}

fn reader_routine(
    reader_id: usize,
    files: Vec<PathBuf>,
    vertex_type: LabelId,
    id_col: usize,
    cols: Vec<usize>,
    col_types: Vec<DataType>,
    delim: u8,
    has_header: bool,
    part_id: usize,
    part_num: usize,
    tx: mpsc::Sender<(usize, Message)>,
    table_tx: mpsc::Sender<VertexBatch>,
) {
    let mut batches = vec![];
    for i in 0..part_num {
        batches.push(VertexBatch::new(&col_types));
    }
    let (serialize_tx, serialize_rx) = mpsc::channel();
    let serialze_handle = thread::spawn(move || {
        while let Ok((target, batch)) = serialize_rx.recv() {
            let serialized = bincode::serialize(&batch).unwrap();
            let message = Message { content: serialized };
            tx.send((target, message)).unwrap();
        }
    });
    let parser = LDBCVertexParser::new(vertex_type, id_col);
    for file in files.into_iter() {
        let file_string = file.as_os_str().to_str().unwrap();
        if file_string.ends_with(".csv.gz") {
            let mut rdr = ReaderBuilder::new()
                .delimiter(delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(has_header)
                .from_reader(BufReader::new(GzReader::from_path(file).unwrap()));
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    let target = get_partition(vertex_meta.global_id, part_num);
                    batches[target].id.push(vertex_meta.global_id);
                    batches[target].properties.append(&record, &cols);
                }
            }
        } else if file_string.ends_with(".csv") {
            let mut rdr = ReaderBuilder::new()
                .delimiter(delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(has_header)
                .from_reader(BufReader::new(File::open(file).unwrap()));
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    let target = get_partition(vertex_meta.global_id, part_num);
                    batches[target].id.push(vertex_meta.global_id);
                    batches[target].properties.append(&record, &cols);
                }
            }
        } else {
            info!("file format not support: {}", file_string);
        }

        for (i, batch) in batches.iter_mut().enumerate() {
            if batch.id.len() >= 4096 {
                let to_send = std::mem::replace(batch, VertexBatch::new(&col_types));
                if i == part_id {
                    table_tx.send(to_send).unwrap();
                } else {
                    serialize_tx.send((i, to_send)).unwrap();
                }
            }
        }
    }
    for (i, batch) in batches.into_iter().enumerate() {
        if !batch.id.is_empty() {
            if i == part_id {
                table_tx.send(batch).unwrap();
            } else {
                serialize_tx.send((i, batch)).unwrap();
            }
        }
    }

    drop(serialize_tx);
    serialze_handle.join().unwrap();
}

fn deserialize_routine(mut rx: mpsc::Receiver<Message>, tx: mpsc::Sender<VertexBatch>) {
    while let Ok(message) = rx.recv() {
        let deserialized: VertexBatch = bincode::deserialize(&message.content).unwrap();
        tx.send(deserialized).unwrap();
    }
}

pub fn load_all_vertex(
    input_dir: &PathBuf, 
    output_path: &PathBuf,
    input_schema: &InputSchema,
    graph_schema: &CsrGraphSchema,
    vertex_type: LabelId,

    delim: u8,
    has_header: bool,
) -> Vec<usize> {
    info!("enter load all vertex - {}", vertex_type as i32);
    let input_header = input_schema.get_vertex_header(vertex_type).unwrap();
    let graph_header = graph_schema.get_vertex_header(vertex_type).unwrap();
    let mut keep_set = HashSet::new();
    for pair in graph_header {
        keep_set.insert(pair.0.clone());
    }

    let mut cols = vec![];
    let mut col_types = vec![];
    let mut id_col_id = 0;
    for (index, (n, t)) in input_header.iter().enumerate() {
        if keep_set.contains(n) {
            cols.push(index);
            col_types.push(*t);
        }
        if n == "id" {
            id_col_id = index;
        }
    }

    let vertex_file_strings = input_schema.get_vertex_file(vertex_type).unwrap();
    let mut vertex_files = get_files_list(input_dir, vertex_file_strings).unwrap();
    vertex_files.sort();

    let parser = LDBCVertexParser::new(vertex_type, id_col_id);
    let mut batch = VertexBatch::new(&col_types);
    for file in vertex_files.into_iter() {
        let file_string = file.as_os_str().to_str().unwrap();
        if file_string.ends_with(".csv.gz") {
            let mut rdr = ReaderBuilder::new()
                .delimiter(delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(has_header)
                .from_reader(BufReader::new(GzReader::from_path(file).unwrap()));
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    batch.id.push(vertex_meta.global_id);
                    batch.properties.append(&record, &cols);
                }
            }
        } else {
            let mut rdr = ReaderBuilder::new()
                .delimiter(delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(has_header)
                .from_reader(BufReader::new(File::open(file).unwrap()));
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    batch.id.push(vertex_meta.global_id);
                    batch.properties.append(&record, &cols);
                }
            }
        }
    }
    let output_dir = output_path.as_os_str().to_str().unwrap().to_string();
    let output_prefix = format!("{}/vp_{}", output_dir, vertex_type as i32);
    let mut writer = RecordBatchWriter::new(&output_prefix, &col_types);
    writer.write(&batch.properties);
    info!("leave load all vertex - {}", vertex_type as i32);
    batch.id
}

pub fn load_vertex(
    input_dir: &PathBuf, 
    output_path: &PathBuf,
    input_schema: &InputSchema,
    graph_schema: &CsrGraphSchema,
    vertex_type: LabelId,
    shuffler: &mut Shuffler,

    delim: u8,
    has_header: bool,

    reader_num: usize,
    offset: usize,
) -> Vec<usize> {
    info!("enter load vertex - {}", vertex_type as i32);
    let input_header = input_schema.get_vertex_header(vertex_type).unwrap();
    let graph_header = graph_schema.get_vertex_header(vertex_type).unwrap();
    let part_num = shuffler.worker_num;
    let part_id = shuffler.worker_id;
    let mut keep_set = HashSet::new();
    for pair in graph_header {
        keep_set.insert(pair.0.clone());
    }

    let mut cols = vec![];
    let mut col_types = vec![];
    let mut id_col_id = 0;
    for (index, (n, t)) in input_header.iter().enumerate() {
        if keep_set.contains(n) {
            cols.push(index);
            col_types.push(*t);
        }
        if n == "id" {
            id_col_id = index;
        }
    }

    let vertex_file_strings = input_schema.get_vertex_file(vertex_type).unwrap();
    let mut vertex_files = get_files_list(input_dir, vertex_file_strings).unwrap();
    vertex_files.sort();

    let mut picked_files = vec![vec![]; reader_num];
    let mut reader_idx = 0_usize;
    for (i, path) in vertex_files.iter().enumerate() {
        if (i + offset) % part_num == part_id {
            picked_files[reader_idx].push(path.clone());
            reader_idx = (reader_idx + 1) % reader_num;
        }
    }

    let (tx, rx) = shuffler.shuffle_start();
    let (tx_table, rx_table) = mpsc::channel::<VertexBatch>();
    let output_dir = output_path.as_os_str().to_str().unwrap().to_string();
    let output_prefix = format!("{}/vp_{}", output_dir, vertex_type as i32);
    let col_types_w = col_types.clone();
    let writer_handle = thread::spawn(move || {
        let mut result: Vec<usize> = vec![];
        let mut writer = RecordBatchWriter::new(&output_prefix, &col_types_w);
        while let Ok(batch) = rx_table.recv() {
            writer.write(&batch.properties);
            result.extend_from_slice(&batch.id);
        }
        writer.flush();
        result
    });

    let reader_handles: Vec<_> = (0..reader_num).map(|i| {
        let tx_clone = tx.clone();
        let tx_table_clone = tx_table.clone();
        let cols = cols.clone();
        let col_types = col_types.clone();
        let files = picked_files[i].clone();
        thread::spawn(move || {
            reader_routine(i, files, vertex_type, id_col_id, cols, col_types, delim, has_header, part_id, part_num, tx_clone, tx_table_clone);
        })
    }).collect();

    let deserializer_handle = thread::spawn(move || {
        deserialize_routine(rx, tx_table);
    });

    for handle in reader_handles {
        handle.join().unwrap();
    }
    drop(tx);
    shuffler.shuffle_end();
    deserializer_handle.join().unwrap();
    info!("leave load vertex - {}", vertex_type as i32);
    writer_handle.join().unwrap()
}