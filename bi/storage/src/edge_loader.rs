use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;

use crate::shm_container::SharedVec;
use csv::ReaderBuilder;
use rayon::prelude::*;
use rust_htslib::bgzf::Reader as GzReader;
use serde::{Deserialize, Serialize};

use crate::columns::DataType;
use crate::csr_trait::SafePtr;
use crate::graph_loader::get_files_list;
use crate::graph_loader_beta::RTVertexMap;
use crate::ldbc_parser::LDBCEdgeParser;
use crate::record_batch::{RecordBatch, RecordBatchWriter};
use crate::shuffler::{Message, Shuffler};
use crate::types::LabelId;

use crate::schema::{CsrGraphSchema, InputSchema, LoadStrategy, Schema};

#[derive(Serialize, Deserialize)]
struct EdgeBatch {
    pub src: Vec<usize>,
    pub dst: Vec<usize>,
    pub properties: RecordBatch,
}

impl EdgeBatch {
    pub fn new(cols: &[DataType]) -> Self {
        Self {
            src: vec![],
            dst: vec![],
            properties: RecordBatch::new(cols),
        }
    }
}

fn deserialize_routine(mut rx: mpsc::Receiver<Message>, tx: mpsc::Sender<EdgeBatch>) {
    let mut total_deserialized = 0_usize;
    while let Ok(message) = rx.recv() {
        let deserialized: EdgeBatch = bincode::deserialize(&message.content).unwrap();
        total_deserialized += deserialized.src.len();
        tx.send(deserialized).unwrap();
    }
}

fn reader_routine(
    reader_id: usize,
    files: Vec<PathBuf>,
    src_label: LabelId,
    dst_label: LabelId,
    edge_label: LabelId,
    src_col: usize,
    dst_col: usize,
    cols: Vec<usize>,
    col_types: Vec<DataType>,
    delim: u8,
    has_header: bool,
    part_id: usize,
    part_num: usize,
    is_src_static: bool,
    is_dst_static: bool,
    load_strategy: LoadStrategy,
    tx: mpsc::Sender<(usize, Message)>,
    table_tx: mpsc::Sender<EdgeBatch>,
) {
    let mut batches = vec![];
    for i in 0..part_num {
        batches.push(EdgeBatch::new(&col_types));
    }
    let (serialize_tx, serialize_rx) = mpsc::channel();
    let serialze_handle = thread::spawn(move || {
        while let Ok((target, batch)) = serialize_rx.recv() {
            let serialized = bincode::serialize(&batch).unwrap();
            let message = Message {
                content: serialized,
            };
            tx.send((target, message)).unwrap();
        }
    });
    let mut total_to_self = 0_usize;
    let mut parser = LDBCEdgeParser::<usize>::new(src_label, dst_label, edge_label);
    parser.with_endpoint_col_id(src_col, dst_col);
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
                    let edge_meta = parser.parse_edge_meta(&record);
                    let src_part_id = edge_meta.src_global_id % part_num;
                    let dst_part_id = edge_meta.dst_global_id % part_num;

                    if is_src_static && is_dst_static {
                        batches[part_id].src.push(edge_meta.src_global_id);
                        batches[part_id].dst.push(edge_meta.dst_global_id);
                        batches[part_id].properties.append(&record, &cols);
                    } else if (is_src_static && !is_dst_static)
                        || load_strategy == LoadStrategy::OnlyIn
                    {
                        batches[dst_part_id].src.push(edge_meta.src_global_id);
                        batches[dst_part_id].dst.push(edge_meta.dst_global_id);
                        batches[dst_part_id].properties.append(&record, &cols);
                    } else if (!is_src_static && is_dst_static)
                        || load_strategy == LoadStrategy::OnlyOut
                    {
                        batches[src_part_id].src.push(edge_meta.src_global_id);
                        batches[src_part_id].dst.push(edge_meta.dst_global_id);
                        batches[src_part_id].properties.append(&record, &cols);
                    } else {
                        batches[src_part_id].src.push(edge_meta.src_global_id);
                        batches[src_part_id].dst.push(edge_meta.dst_global_id);
                        batches[src_part_id].properties.append(&record, &cols);
                        if src_part_id != dst_part_id {
                            batches[dst_part_id].src.push(edge_meta.src_global_id);
                            batches[dst_part_id].dst.push(edge_meta.dst_global_id);
                            batches[dst_part_id].properties.append(&record, &cols);
                        }
                    }
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
                    let edge_meta = parser.parse_edge_meta(&record);
                    let src_part_id = edge_meta.src_global_id % part_num;
                    let dst_part_id = edge_meta.dst_global_id % part_num;

                    if is_src_static && is_dst_static {
                        batches[part_id].src.push(edge_meta.src_global_id);
                        batches[part_id].dst.push(edge_meta.dst_global_id);
                        batches[part_id].properties.append(&record, &cols);
                    } else if (is_src_static && !is_dst_static)
                        || load_strategy == LoadStrategy::OnlyIn
                    {
                        batches[dst_part_id].src.push(edge_meta.src_global_id);
                        batches[dst_part_id].dst.push(edge_meta.dst_global_id);
                        batches[dst_part_id].properties.append(&record, &cols);
                    } else if (!is_src_static && is_dst_static)
                        || load_strategy == LoadStrategy::OnlyOut
                    {
                        batches[src_part_id].src.push(edge_meta.src_global_id);
                        batches[src_part_id].dst.push(edge_meta.dst_global_id);
                        batches[src_part_id].properties.append(&record, &cols);
                    } else {
                        batches[src_part_id].src.push(edge_meta.src_global_id);
                        batches[src_part_id].dst.push(edge_meta.dst_global_id);
                        batches[src_part_id].properties.append(&record, &cols);
                        if src_part_id != dst_part_id {
                            batches[dst_part_id].src.push(edge_meta.src_global_id);
                            batches[dst_part_id].dst.push(edge_meta.dst_global_id);
                            batches[dst_part_id].properties.append(&record, &cols);
                        }
                    }
                }
            }
        } else {
            info!("file format not support: {}", file_string);
        }

        for (i, batch) in batches.iter_mut().enumerate() {
            if batch.src.len() >= 4096 {
                let to_send = std::mem::replace(batch, EdgeBatch::new(&col_types));
                if i == part_id {
                    total_to_self += to_send.src.len();
                    table_tx.send(to_send).unwrap();
                } else {
                    serialize_tx.send((i, to_send)).unwrap();
                }
            }
        }
    }

    for (i, batch) in batches.into_iter().enumerate() {
        if !batch.src.is_empty() {
            if i == part_id {
                total_to_self += batch.src.len();
                table_tx.send(batch).unwrap();
            } else {
                serialize_tx.send((i, batch)).unwrap();
            }
        }
    }

    drop(serialize_tx);
    serialze_handle.join().unwrap();
}

pub fn load_raw_edge(
    input_dir: &PathBuf,
    input_schema: &InputSchema,
    graph_schema: &CsrGraphSchema,
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,
    shuffler: &mut Shuffler,
    delim: u8,
    has_header: bool,
    reader_num: usize,
    offset: usize,
) -> (Vec<usize>, Vec<usize>, RecordBatch) {
    let input_header = input_schema
        .get_edge_header(src_label, edge_label, dst_label)
        .unwrap();
    let graph_header = graph_schema
        .get_edge_header(src_label, edge_label, dst_label)
        .unwrap();
    let is_src_static = graph_schema.is_static_vertex(src_label);
    let is_dst_static = graph_schema.is_static_vertex(dst_label);
    let load_strategy = graph_schema.get_edge_load_strategy(src_label, edge_label, dst_label);

    let part_num = shuffler.worker_num;
    let part_id = shuffler.worker_id;

    let mut keep_set = HashSet::new();
    for pair in graph_header {
        keep_set.insert(pair.0.clone());
    }

    let mut cols = vec![];
    let mut col_types = vec![];
    let mut src_col_id = 0_usize;
    let mut dst_col_id = 0_usize;
    for (index, (n, t)) in input_header.iter().enumerate() {
        if keep_set.contains(n) {
            cols.push(index);
            col_types.push(*t);
        }
        if n == "start_id" {
            src_col_id = index;
        }
        if n == "end_id" {
            dst_col_id = index;
        }
    }
    println!("cols: {:?}", &cols);
    println!("col_types: {:?}", &col_types);

    let edge_file_strings = input_schema
        .get_edge_file(src_label, edge_label, dst_label)
        .unwrap();
    let mut edge_files = get_files_list(input_dir, edge_file_strings).unwrap();
    edge_files.sort();

    let static_edges = is_src_static && is_dst_static;

    let mut picked_files = vec![vec![]; reader_num];
    let mut reader_idx = 0_usize;
    for (i, path) in edge_files.iter().enumerate() {
        if (i + offset) % part_num == part_id || static_edges {
            picked_files[reader_idx].push(path.clone());
            reader_idx = (reader_idx + 1) % reader_num;
        }
    }

    let (tx, rx) = shuffler.shuffle_start();
    let (tx_table, rx_table) = mpsc::channel::<EdgeBatch>();
    let writer_handle = thread::spawn({
        let col_types = col_types.clone();
        move || {
            let mut src_list = vec![];
            let mut dst_list = vec![];
            let mut properties = RecordBatch::new(&col_types);

            while let Ok(batch) = rx_table.recv() {
                let batch_len = batch.src.len();
                src_list.extend(batch.src);
                dst_list.extend(batch.dst);
                properties.append_rb(batch.properties);
            }

            (src_list, dst_list, properties)
        }
    });

    let reader_handles: Vec<_> = (0..reader_num)
        .map(|i| {
            let tx_clone = tx.clone();
            let tx_table_clone = tx_table.clone();
            let cols = cols.clone();
            let col_types = col_types.clone();
            let files = picked_files[i].clone();
            thread::spawn(move || {
                reader_routine(
                    i,
                    files,
                    src_label,
                    dst_label,
                    edge_label,
                    src_col_id,
                    dst_col_id,
                    cols,
                    col_types,
                    delim,
                    has_header,
                    part_id,
                    part_num,
                    is_src_static,
                    is_dst_static,
                    load_strategy,
                    tx_clone,
                    tx_table_clone,
                );
            })
        })
        .collect();

    let deserializer_handle = thread::spawn(move || {
        deserialize_routine(rx, tx_table);
    });

    for handle in reader_handles {
        handle.join().unwrap();
    }
    drop(tx);
    shuffler.shuffle_end();
    deserializer_handle.join().unwrap();

    writer_handle.join().unwrap()
}

pub fn load_edge(
    input_dir: &PathBuf,
    input_schema: &InputSchema,
    graph_schema: &CsrGraphSchema,
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,
    vertex_maps: &mut Vec<RTVertexMap>,
    shuffler: &mut Shuffler,
    delim: u8,
    has_header: bool,
    reader_num: usize,
    offset: usize,
) -> (Vec<usize>, Vec<usize>, Vec<i32>, Vec<i32>, RecordBatch) {
    let src_num = vertex_maps[src_label as usize].native_vertex_num();
    let dst_num = vertex_maps[dst_label as usize].native_vertex_num();

    let mut odegree = vec![0_i32; src_num];
    let mut idegree = vec![0_i32; dst_num];

    let (mut src_list, mut dst_list, properties) = load_raw_edge(
        input_dir,
        input_schema,
        graph_schema,
        src_label,
        edge_label,
        dst_label,
        shuffler,
        delim,
        has_header,
        reader_num,
        offset,
    );

    let is_src_static = graph_schema.is_static_vertex(src_label);
    let is_dst_static = graph_schema.is_static_vertex(dst_label);

    if !is_src_static {
        let start = Instant::now();
        let mut src_list_cloned: Vec<usize> = src_list.par_iter().cloned().collect();
        src_list_cloned.par_sort();
        src_list_cloned.dedup();
        let src_vm = &vertex_maps[src_label as usize];
        let src_corner: Vec<usize> = src_list_cloned
            .par_iter()
            .filter(|v| src_vm.get_internal_id(**v).is_none())
            .cloned()
            .collect();
        vertex_maps[src_label as usize].add_corner_vertices2(src_corner);
        info!("reduce src: {:.2} s", start.elapsed().as_secs_f64());
    }
    if !is_dst_static {
        let start = Instant::now();
        let mut dst_list_cloned: Vec<usize> = dst_list.par_iter().cloned().collect();
        dst_list_cloned.par_sort();
        dst_list_cloned.dedup();
        let dst_vm = &vertex_maps[dst_label as usize];
        let dst_corner: Vec<usize> = dst_list_cloned
            .par_iter()
            .filter(|v| dst_vm.get_internal_id(**v).is_none())
            .cloned()
            .collect();
        vertex_maps[dst_label as usize].add_corner_vertices2(dst_corner);
        info!("reduce dst: {:.2} s", start.elapsed().as_secs_f64());
    }

    let local_src_counts: Vec<HashMap<usize, i32>> = src_list
        .par_iter_mut()
        .filter_map(|x| {
            *x = vertex_maps[src_label as usize].get_internal_id(*x).unwrap();
            if *x < src_num {
                Some(*x)
            } else {
                None
            }
        })
        .fold(
            || HashMap::new(),
            |mut acc, x| {
                *acc.entry(x).or_insert(0) += 1;
                acc
            },
        )
        .collect();
    let local_dst_counts: Vec<HashMap<usize, i32>> = dst_list
        .par_iter_mut()
        .filter_map(|x| {
            *x = vertex_maps[dst_label as usize].get_internal_id(*x).unwrap();
            if *x < dst_num {
                Some(*x)
            } else {
                None
            }
        })
        .fold(
            || HashMap::new(),
            |mut acc, x| {
                *acc.entry(x).or_insert(0) += 1;
                acc
            },
        )
        .collect();
    for local_count in local_src_counts {
        for (key, count) in local_count {
            odegree[key] += count;
        }
    }
    for local_count in local_dst_counts {
        for (key, count) in local_count {
            idegree[key] += count;
        }
    }
    (src_list, dst_list, odegree, idegree, properties)
}

fn prefix_sum(deg: &Vec<i32>) -> (Vec<usize>, usize) {
    let mut ret = Vec::with_capacity(deg.len());
    let mut cur = 0_usize;
    for v in deg.iter() {
        ret.push(cur);
        cur += *v as usize;
    }
    (ret, cur)
}

fn generate_degree(v: &Vec<usize>, vm: &RTVertexMap) -> Vec<i32> {
    let vnum = vm.native_vertex_num();
    let mut degree = vec![0; vnum];

    let local_counts: Vec<HashMap<usize, i32>> = v
        .par_iter()
        .filter_map(|x| vm.get_internal_id(*x))
        .fold(
            || HashMap::new(),
            |mut acc, x| {
                *acc.entry(x).or_insert(0) += 1;
                acc
            },
        )
        .collect();

    for local_count in local_counts {
        for (key, count) in local_count {
            degree[key] += count;
        }
    }

    degree
}

fn dump_pod_vec<T: Copy + Sized>(path: &str, v: &Vec<T>) {
    SharedVec::<T>::dump_vec(path, v);
}

pub fn dump_edge_without_properties(
    src_list: Vec<usize>,
    dst_list: Vec<usize>,
    odegree: Vec<i32>,
    idegree: Vec<i32>,
    oe_prefix: String,
    ie_prefix: String,
    is_single_oe: bool,
    is_single_ie: bool,
    load_strategy: LoadStrategy,
) {
    let src_num = odegree.len();
    let dst_num = idegree.len();
    if !is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num) = prefix_sum(&odegree);
        let (mut ie_offsets, ie_num) = prefix_sum(&idegree);

        dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
        dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
        dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);
        dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);

        let mut oe_nbrs = vec![usize::MAX; oe_num];
        let mut ie_nbrs = vec![usize::MAX; ie_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                let cur_offset = oe_offsets[src];
                oe_offsets[src] += 1;

                oe_nbrs[cur_offset] = dst;
            }
            if dst < dst_num {
                let cur_offset = ie_offsets[dst];
                ie_offsets[dst] += 1;

                ie_nbrs[cur_offset] = src;
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
    } else if is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num) = prefix_sum(&odegree);
        dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
        dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);

        let mut oe_nbrs = vec![usize::MAX; oe_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                let cur_offset = oe_offsets[src];
                oe_offsets[src] += 1;

                oe_nbrs[cur_offset] = dst;
            }
            if dst < dst_num {
                ie_nbrs[dst] = src;
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", ie_prefix).as_str(),
            &vec![dst_num, dst_num],
        );
    } else if !is_single_ie && is_single_oe {
        let (mut ie_offsets, ie_num) = prefix_sum(&idegree);

        dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
        dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);

        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; ie_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                oe_nbrs[src] = dst;
            }
            if dst < dst_num {
                let cur_offset = ie_offsets[dst];
                ie_offsets[dst] += 1;

                ie_nbrs[cur_offset] = src;
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", oe_prefix).as_str(),
            &vec![src_num, src_num],
        );
        SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
    } else {
        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                oe_nbrs[src] = dst;
            }
            if dst < dst_num {
                ie_nbrs[dst] = src;
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", oe_prefix).as_str(),
            &vec![src_num, src_num],
        );
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", ie_prefix).as_str(),
            &vec![dst_num, dst_num],
        );
    }
}

pub fn dump_edge_no_corner_without_properties(
    vertex_maps: Arc<Vec<RTVertexMap>>,
    src_list: Vec<usize>,
    dst_list: Vec<usize>,
    src_label: LabelId,
    dst_label: LabelId,
    oe_prefix: String,
    ie_prefix: String,
    is_single_oe: bool,
    is_single_ie: bool,
    load_strategy: LoadStrategy,
) {
    let src_vm = &vertex_maps[src_label as usize];
    let dst_vm = &vertex_maps[dst_label as usize];
    let src_num = src_vm.native_vertex_num();
    let dst_num = dst_vm.native_vertex_num();

    if !is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num, mut oe_nbrs) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                let odegree = generate_degree(&src_list, src_vm);
                let (mut oe_offsets, oe_num) = prefix_sum(&odegree);
                dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
                dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);
                let mut oe_nbrs = vec![usize::MAX; oe_num];
                (oe_offsets, oe_num, oe_nbrs)
            } else {
                (vec![], 0, vec![])
            };
        let (mut ie_offsets, ie_num, mut ie_nbrs) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                let idegree = generate_degree(&dst_list, dst_vm);
                let (mut ie_offsets, ie_num) = prefix_sum(&idegree);
                dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
                dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);
                let mut ie_nbrs = vec![usize::MAX; ie_num];
                (ie_offsets, ie_num, ie_nbrs)
            } else {
                (vec![], 0, vec![])
            };

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    let cur_offset = oe_offsets[src_lid];
                    oe_offsets[src_lid] += 1;

                    oe_nbrs[cur_offset] = dst;
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    let cur_offset = ie_offsets[dst_lid];
                    ie_offsets[dst_lid] += 1;

                    ie_nbrs[cur_offset] = src;
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
        }
    } else if is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                let odegree = generate_degree(&src_list, src_vm);
                let (mut oe_offsets, oe_num) = prefix_sum(&odegree);

                dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
                dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);
                (oe_offsets, oe_num)
            } else {
                (vec![], 0)
            };

        let mut oe_nbrs = vec![usize::MAX; oe_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    let cur_offset = oe_offsets[src_lid];
                    oe_offsets[src_lid] += 1;

                    oe_nbrs[cur_offset] = dst;
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    ie_nbrs[dst_lid] = src;
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", ie_prefix).as_str(),
                &vec![dst_num, dst_num],
            );
        }
    } else if !is_single_ie && is_single_oe {
        let (mut ie_offsets, ie_num) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                let idegree = generate_degree(&dst_list, dst_vm);
                let (mut ie_offsets, ie_num) = prefix_sum(&idegree);

                dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
                dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);
                (ie_offsets, ie_num)
            } else {
                (vec![], 0)
            };

        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; ie_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    oe_nbrs[src_lid] = dst;
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    let cur_offset = ie_offsets[dst_lid];
                    ie_offsets[dst_lid] += 1;

                    ie_nbrs[cur_offset] = src;
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", oe_prefix).as_str(),
                &vec![src_num, src_num],
            );
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
        }
    } else {
        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    oe_nbrs[src_lid] = dst;
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    ie_nbrs[dst_lid] = src;
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", oe_prefix).as_str(),
                &vec![src_num, src_num],
            );
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", ie_prefix).as_str(),
                &vec![dst_num, dst_num],
            );
        }
    }
}

pub fn dump_properties(
    properties: RecordBatch,
    oe_order: Vec<usize>,
    oe_prefix: String,
    oe_num: usize,
    ie_order: Vec<usize>,
    ie_prefix: String,
    ie_num: usize,
) {
    if properties.columns.is_empty() {
        return;
    }
    let col_types = properties.col_types();
    {
        let mut oe_properties = RecordBatch::new(&col_types);
        oe_properties.resize(oe_num);
        oe_properties.set_indexed_rb(&oe_order, &properties);

        let mut oe_writer = RecordBatchWriter::new(&oe_prefix, &col_types);
        oe_writer.write(&oe_properties);
        oe_writer.flush();
    }
    {
        let mut ie_properties = RecordBatch::new(&col_types);
        ie_properties.resize(ie_num);
        ie_properties.set_indexed_rb(&ie_order, &properties);

        let mut ie_writer = RecordBatchWriter::new(&ie_prefix, &col_types);
        ie_writer.write(&ie_properties);
        ie_writer.flush();
    }
}

pub fn dump_edge_with_properties(
    src_list: Vec<usize>,
    dst_list: Vec<usize>,
    odegree: Vec<i32>,
    idegree: Vec<i32>,
    properties: RecordBatch,
    oe_prefix: String,
    ie_prefix: String,
    oep_prefix: String,
    iep_prefix: String,
    is_single_oe: bool,
    is_single_ie: bool,
    load_strategy: LoadStrategy,
) {
    let src_num = odegree.len();
    let dst_num = idegree.len();

    let edge_num = src_list.len();
    let mut oe_prop_index = Vec::with_capacity(edge_num);
    let mut ie_prop_index = Vec::with_capacity(edge_num);

    let mut oe_num_x = 0_usize;
    let mut ie_num_x = 0_usize;

    if !is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num) = prefix_sum(&odegree);
        let (mut ie_offsets, ie_num) = prefix_sum(&idegree);
        oe_num_x = oe_num;
        ie_num_x = ie_num;

        dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
        dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
        dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);
        dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);

        let mut oe_nbrs = vec![usize::MAX; oe_num];
        let mut ie_nbrs = vec![usize::MAX; ie_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                let cur_offset = oe_offsets[src];
                oe_prop_index.push(cur_offset);
                oe_offsets[src] += 1;

                oe_nbrs[cur_offset] = dst;
            } else {
                oe_prop_index.push(usize::MAX);
            }
            if dst < dst_num {
                let cur_offset = ie_offsets[dst];
                ie_prop_index.push(cur_offset);
                ie_offsets[dst] += 1;

                ie_nbrs[cur_offset] = src;
            } else {
                ie_prop_index.push(usize::MAX);
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
    } else if is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num) = prefix_sum(&odegree);
        dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
        dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);

        let mut oe_nbrs = vec![usize::MAX; oe_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];
        oe_num_x = oe_num;
        ie_num_x = dst_num;

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                let cur_offset = oe_offsets[src];
                oe_prop_index.push(cur_offset);
                oe_offsets[src] += 1;

                oe_nbrs[cur_offset] = dst;
            } else {
                oe_prop_index.push(usize::MAX);
            }
            if dst < dst_num {
                ie_nbrs[dst] = src;
                ie_prop_index.push(dst);
            } else {
                ie_prop_index.push(usize::MAX);
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", ie_prefix).as_str(),
            &vec![dst_num, dst_num],
        );
    } else if !is_single_ie && is_single_oe {
        let (mut ie_offsets, ie_num) = prefix_sum(&idegree);

        dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
        dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);

        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; ie_num];
        oe_num_x = src_num;
        ie_num_x = ie_num;

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                oe_nbrs[src] = dst;
                oe_prop_index.push(src);
            } else {
                oe_prop_index.push(usize::MAX);
            }
            if dst < dst_num {
                let cur_offset = ie_offsets[dst];
                ie_prop_index.push(cur_offset);
                ie_offsets[dst] += 1;

                ie_nbrs[cur_offset] = src;
            } else {
                ie_prop_index.push(usize::MAX);
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", oe_prefix).as_str(),
            &vec![src_num, src_num],
        );
        SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
    } else {
        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];

        oe_num_x = src_num;
        ie_num_x = dst_num;

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if src < src_num {
                oe_nbrs[src] = dst;
                oe_prop_index.push(src);
            } else {
                oe_prop_index.push(usize::MAX);
            }
            if dst < dst_num {
                ie_nbrs[dst] = src;
                ie_prop_index.push(dst);
            } else {
                ie_prop_index.push(usize::MAX);
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", oe_prefix).as_str(),
            &vec![src_num, src_num],
        );
        SharedVec::<usize>::dump_vec(
            format!("{}_meta", ie_prefix).as_str(),
            &vec![dst_num, dst_num],
        );
    }
    dump_properties(
        properties,
        oe_prop_index,
        oep_prefix,
        oe_num_x,
        ie_prop_index,
        iep_prefix,
        ie_num_x,
    );
}

pub fn dump_edge_no_corner_with_properties(
    vertex_maps: Arc<Vec<RTVertexMap>>,
    src_list: Vec<usize>,
    dst_list: Vec<usize>,
    properties: RecordBatch,
    src_label: LabelId,
    dst_label: LabelId,
    oe_prefix: String,
    ie_prefix: String,
    oep_prefix: String,
    iep_prefix: String,
    is_single_oe: bool,
    is_single_ie: bool,
    load_strategy: LoadStrategy,
) {
    let src_vm = &vertex_maps[src_label as usize];
    let dst_vm = &vertex_maps[dst_label as usize];
    let src_num = src_vm.native_vertex_num();
    let dst_num = dst_vm.native_vertex_num();

    let edge_num = src_list.len();
    let mut oe_prop_index = Vec::with_capacity(edge_num);
    let mut ie_prop_index = Vec::with_capacity(edge_num);

    let mut oe_num_x = 0_usize;
    let mut ie_num_x = 0_usize;

    if !is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num, mut oe_nbrs) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                let odegree = generate_degree(&src_list, src_vm);
                let (mut oe_offsets, oe_num) = prefix_sum(&odegree);
                dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
                dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);
                let mut oe_nbrs = vec![usize::MAX; oe_num];
                (oe_offsets, oe_num, oe_nbrs)
            } else {
                (vec![], 0, vec![])
            };
        let (mut ie_offsets, ie_num, mut ie_nbrs) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                let idegree = generate_degree(&dst_list, dst_vm);
                let (mut ie_offsets, ie_num) = prefix_sum(&idegree);
                dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
                dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);
                let mut ie_nbrs = vec![usize::MAX; ie_num];
                (ie_offsets, ie_num, ie_nbrs)
            } else {
                (vec![], 0, vec![])
            };
        oe_num_x = oe_num;
        ie_num_x = ie_num;

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    let cur_offset = oe_offsets[src_lid];
                    oe_prop_index.push(cur_offset);
                    oe_offsets[src_lid] += 1;

                    oe_nbrs[cur_offset] = dst;
                } else {
                    oe_prop_index.push(usize::MAX);
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    let cur_offset = ie_offsets[dst_lid];
                    ie_prop_index.push(cur_offset);
                    ie_offsets[dst_lid] += 1;

                    ie_nbrs[cur_offset] = src;
                } else {
                    ie_prop_index.push(usize::MAX);
                }
            }
        }

        dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
        dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
        SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
    } else if is_single_ie && !is_single_oe {
        let (mut oe_offsets, oe_num) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                let odegree = generate_degree(&src_list, src_vm);
                let (mut oe_offsets, oe_num) = prefix_sum(&odegree);

                dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &odegree);
                dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offsets);
                (oe_offsets, oe_num)
            } else {
                (vec![], 0)
            };

        let mut oe_nbrs = vec![usize::MAX; oe_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];
        oe_num_x = oe_num;
        ie_num_x = dst_num;

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    let cur_offset = oe_offsets[src_lid];
                    oe_prop_index.push(cur_offset);
                    oe_offsets[src_lid] += 1;

                    oe_nbrs[cur_offset] = dst;
                } else {
                    oe_prop_index.push(usize::MAX);
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    ie_nbrs[dst_lid] = src;
                    ie_prop_index.push(dst_lid);
                } else {
                    ie_prop_index.push(usize::MAX);
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![oe_num]);
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", ie_prefix).as_str(),
                &vec![dst_num, dst_num],
            );
        }
    } else if !is_single_ie && is_single_oe {
        let (mut ie_offsets, ie_num) =
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                let idegree = generate_degree(&dst_list, dst_vm);
                let (mut ie_offsets, ie_num) = prefix_sum(&idegree);

                dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &idegree);
                dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offsets);
                (ie_offsets, ie_num)
            } else {
                (vec![], 0)
            };

        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; ie_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    oe_nbrs[src_lid] = dst;
                    oe_prop_index.push(src_lid);
                } else {
                    oe_prop_index.push(usize::MAX);
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    let cur_offset = ie_offsets[dst_lid];
                    ie_prop_index.push(cur_offset);
                    ie_offsets[dst_lid] += 1;

                    ie_nbrs[cur_offset] = src;
                } else {
                    ie_prop_index.push(usize::MAX);
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", oe_prefix).as_str(),
                &vec![src_num, src_num],
            );
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![ie_num]);
        }
    } else {
        let mut oe_nbrs = vec![usize::MAX; src_num];
        let mut ie_nbrs = vec![usize::MAX; dst_num];

        for i in 0..src_list.len() {
            let src = src_list[i];
            let dst = dst_list[i];
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
                if let Some(src_lid) = src_vm.get_internal_id(src) {
                    oe_nbrs[src_lid] = dst;
                    oe_prop_index.push(src_lid);
                } else {
                    oe_prop_index.push(usize::MAX);
                }
            }
            if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
                if let Some(dst_lid) = dst_vm.get_internal_id(dst) {
                    ie_nbrs[dst_lid] = src;
                    ie_prop_index.push(dst_lid);
                } else {
                    ie_prop_index.push(usize::MAX);
                }
            }
        }

        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", oe_prefix).as_str(),
                &vec![src_num, src_num],
            );
        }
        if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", ie_prefix).as_str(),
                &vec![dst_num, dst_num],
            );
        }
    }
    dump_properties(
        properties,
        oe_prop_index,
        oep_prefix,
        oe_num_x,
        ie_prop_index,
        iep_prefix,
        ie_num_x,
    );
}

pub fn dump_empty_edges(
    vertex_maps: Arc<Vec<RTVertexMap>>,
    src_label: LabelId,
    dst_label: LabelId,
    oe_prefix: String,
    ie_prefix: String,
    is_single_oe: bool,
    is_single_ie: bool,
    load_strategy: LoadStrategy,
) {
    let src_vm = &vertex_maps[src_label as usize];
    let dst_vm = &vertex_maps[dst_label as usize];
    let src_num = src_vm.native_vertex_num();
    let dst_num = dst_vm.native_vertex_num();
    if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
        if !is_single_oe {
            let mut oe_degrees = vec![0; src_num];
            let mut oe_offset = vec![0; src_num];
            dump_pod_vec(format!("{}_degree", oe_prefix).as_str(), &oe_degrees);
            dump_pod_vec(format!("{}_offsets", oe_prefix).as_str(), &oe_offset);
            let mut oe_nbrs: Vec<usize> = vec![];
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", oe_prefix).as_str(),
                &vec![src_num, src_num],
            );
        } else {
            let mut oe_nbrs = vec![usize::MAX; src_num];
            dump_pod_vec(format!("{}_nbrs", oe_prefix).as_str(), &oe_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", oe_prefix).as_str(), &vec![src_num]);
        }
    }
    if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
        let mut ie_nbrs = vec![usize::MAX; dst_num];
        if !is_single_ie {
            let mut ie_degrees = vec![0; src_num];
            let mut ie_offset = vec![0; src_num];
            dump_pod_vec(format!("{}_degree", ie_prefix).as_str(), &ie_degrees);
            dump_pod_vec(format!("{}_offsets", ie_prefix).as_str(), &ie_offset);
            let mut ie_nbrs: Vec<usize> = vec![];
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(
                format!("{}_meta", ie_prefix).as_str(),
                &vec![dst_num, dst_num],
            );
        } else {
            let mut ie_nbrs = vec![usize::MAX; dst_num];
            dump_pod_vec(format!("{}_nbrs", ie_prefix).as_str(), &ie_nbrs);
            SharedVec::<usize>::dump_vec(format!("{}_meta", ie_prefix).as_str(), &vec![dst_num]);
        }
    }
}
