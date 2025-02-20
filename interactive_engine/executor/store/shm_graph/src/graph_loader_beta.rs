use std::collections::HashSet;
use std::time::Instant;
use std::{thread, usize};
use std::{fs::create_dir_all, path::PathBuf};
use std::sync::{Arc, Mutex};

use crate::edge_loader::{dump_edge_without_properties, load_edge, dump_edge_with_properties};
use crate::indexer::Indexer;
use crate::types::LabelId;
use libc::glob;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use shm_container::SharedVec;

use crate::types::DIR_BINARY_DATA;

use crate::vertex_loader::{load_vertex, load_all_vertex};
use crate::{schema::InputSchema, shuffler::Shuffler, types::{DIR_GRAPH_SCHEMA, FILE_SCHEMA}};

use crate::schema::{CsrGraphSchema, Schema};
use dashmap::DashMap;

#[derive(Debug)]
pub struct RTVertexMap {
    vertex_list: Vec<(usize, usize)>,
    native_num: usize,
    corner_num: usize,
}

impl RTVertexMap {
    pub fn new(native_list: Vec<usize>) -> Self {
        let native_num = native_list.len();
        let mut vertex_list : Vec<(usize, usize)> = native_list.into_par_iter().enumerate().map(|(idx, v)| {
            (v, idx)
        }).collect();
        vertex_list.par_sort_by_key(|&(key, _)| key);
        // vertex_list.sort_by_key(|&(key, _)| key);

        Self {
            vertex_list,
            native_num,
            corner_num: 0,
        }
    }

    pub fn add_corner_vertices(&mut self, global_ids: HashSet<usize>) {
        let mut cur_vertex_id = usize::MAX - self.corner_num - 1;
        self.corner_num += global_ids.len();
        for v in global_ids {
            self.vertex_list.push((v, cur_vertex_id));
            cur_vertex_id -= 1;
        }
        // self.vertex_list.sort_by_key(|&(key, _)| key);
        self.vertex_list.par_sort_by_key(|&(key, _)| key);
    }

    pub fn add_corner_vertices2(&mut self, global_ids: Vec<usize>) {
        let mut cur_vertex_id = usize::MAX - self.corner_num - 1;
        self.corner_num += global_ids.len();
        for v in global_ids {
            self.vertex_list.push((v, cur_vertex_id));
            cur_vertex_id -= 1;
        }
        // self.vertex_list.sort_by_key(|&(key, _)| key);
        self.vertex_list.par_sort_by_key(|&(key, _)| key);
    }

    pub fn get_internal_id(&self, global_id: usize) -> Option<usize> {
        match self.vertex_list.binary_search_by_key(&global_id, |&(key, _)| key) {
            Ok(index) => Some(self.vertex_list[index].1),
            Err(e) => None,
        }
    }

    pub fn native_vertex_num(&self) -> usize {
        self.native_num
    }

    pub fn dump(&self, vm_prefix: String, vmc_prefix: String, vm_tomb: String) {
        let mut native_list = vec![usize::MAX; self.native_num];
        let mut corner_list = vec![usize::MAX; self.corner_num];
        for (key, val) in self.vertex_list.iter() {
            if *val < self.native_num {
                native_list[*val] = *key;
            } else {
                let idx = usize::MAX - *val - 1;
                corner_list[idx] = *key;
            }
        }

        Indexer::dump(&vm_prefix, &native_list, true);
        Indexer::dump(&vmc_prefix, &corner_list, true);
        // SharedVec::<u8>::dump_vec(&vm_tomb, &vec![0_u8; self.native_num]);
    }
}


// pub struct RTVertexMap {
//     native_list: Vec<usize>,
//     table: DashMap<usize, usize>,
//     corner_list: Vec<usize>,
// }
// 
// impl RTVertexMap {
//     pub fn new(native_list: Vec<usize>) -> Self {
//         let mut table = DashMap::<usize, usize>::new();
//         native_list.par_iter().enumerate().for_each(|(index, value)| {
//             table.insert(*value, index);
//         });
//         Self {
//             native_list,
//             table,
//             corner_list: vec![],
//         }
//     }
// 
//     pub fn add_corner_vertex(&mut self, global_id: usize) -> usize {
//         if let Some(v) = self.table.get(&global_id) {
//             *v
//         } else {
//             let v = usize::MAX - self.corner_list.len() - 1;
//             self.corner_list.push(global_id);
//             self.table.insert(global_id, v);
//             v
//         }
//     }
// 
//     pub fn add_corner_vertices(&mut self, global_ids: HashSet<usize>) {
//         let cur_corner_num = self.corner_list.len();
//         for v in global_ids.into_iter() {
//             self.corner_list.push(v);
//         }
//         let new_corner_num = self.corner_list.len();
//         (cur_corner_num..new_corner_num).into_par_iter().for_each(|idx| {
//             let global_id = self.corner_list[idx];
//             let internal_id = usize::MAX - idx - 1;
//             self.table.insert(global_id, internal_id);
//         });
//     }
// 
//     pub fn get_internal_id(&self, global_id: usize) -> Option<usize> {
//         if let Some(val) = self.table.get(&global_id) {
//             Some(*val)
//         } else {
//             None
//         }
//     }
// 
//     pub fn get_global_id(&self, internal_id: usize) -> Option<usize> {
//         if internal_id < self.native_list.len() {
//             Some(self.native_list[internal_id])
//         } else {
//             let idx = usize::MAX - internal_id - 1;
//             if idx < self.corner_list.len() {
//                 Some(self.corner_list[idx])
//             } else {
//                 None
//             }
//         }
//     }
// 
//     pub fn native_vertex_num(&self) -> usize {
//         self.native_list.len()
//     }
// 
//     pub fn dump(&self, vm_prefix: String, vmc_prefix: String, vm_tomb: String) {}
// }

pub fn load_graph(
    input_dir: PathBuf,
    output_dir: PathBuf,
    input_schema_file: PathBuf,
    graph_schema_file: PathBuf,
    hosts: Vec<String>,
    part_id: usize,
    part_num: usize,

    delim: u8,
    has_header: bool,
    reader_num: usize,
    offset: usize,
) {
    let schema_dir = output_dir.clone().join(DIR_GRAPH_SCHEMA);
    if !schema_dir.exists() {
        create_dir_all(&schema_dir).expect("Create graph schema directory failed...");
    }
    info!("load graph schema from file: {:?}", &graph_schema_file);
    let graph_schema = CsrGraphSchema::from_json_file(graph_schema_file).expect("Read graph schema failed...");
    graph_schema.to_json_file(&schema_dir.join(FILE_SCHEMA)).expect("Write graph schema failed...");

    info!("input graph schema from file: {:?}", &input_schema_file);
    let input_schema = InputSchema::from_json_file(input_schema_file, &graph_schema).expect("Read input schema failed...");

    let partition_dir = output_dir.clone().join(DIR_BINARY_DATA).join(format!("partition_{}", part_id));
    if !partition_dir.exists() {
        create_dir_all(&partition_dir).expect("Create partition directory failed...");
    }

    let mut shuffler = Shuffler::new(&hosts, part_id, part_num);

    let v_label_num = graph_schema.vertex_type_to_id.len() as LabelId;
    let very_start = Instant::now();
    let mut vertex_maps = vec![];
    for v_label_i in 0..v_label_num {
        let start = Instant::now();
        let vertex_ids = if graph_schema.is_static_vertex(v_label_i) {
            load_all_vertex(
                &input_dir,
                &partition_dir, 
                &input_schema, 
                &graph_schema, 
                v_label_i, 
                delim, 
                has_header)
        } else {
            load_vertex(
                &input_dir,
                &partition_dir, 
                &input_schema, 
                &graph_schema, 
                v_label_i, 
                &mut shuffler, 
                delim, 
                has_header, 
                reader_num, 
                offset)
        };
        info!("loader: loading vertex - {} takes: {:.2} s", v_label_i as i32, start.elapsed().as_secs_f64());
        let start = Instant::now();
        vertex_maps.push(RTVertexMap::new(vertex_ids));
        info!("loader: mapping vertex - {} takes: {:.2} s", v_label_i as i32, start.elapsed().as_secs_f64());
    }

    let vertex_maps = Arc::new(Mutex::new(vertex_maps));

    let e_label_num = graph_schema.edge_type_to_id.len() as LabelId;
    let mut dump_handles = vec![];
    for edge_label in 0..e_label_num {
        let edge_label_name = graph_schema.edge_label_names()[edge_label as usize].clone();
        for src_label in 0..v_label_num {
            for dst_label in 0..v_label_num {
                if input_schema.get_edge_header(src_label, edge_label, dst_label).is_some() &&
                    graph_schema.get_edge_header(src_label, edge_label, dst_label).is_some() {
                    let start = Instant::now();
                    info!("start loading edges {} - {} - {}", src_label as i32, edge_label as i32, dst_label as i32);
                    let batches = load_edge(
                        &input_dir, 
                        &input_schema, 
                        &graph_schema, 
                        src_label, 
                        edge_label, 
                        dst_label, 
                        vertex_maps.clone(),
                        &mut shuffler, 
                        delim, 
                        has_header, 
                        reader_num, 
                        offset);
                    info!("finished loading {} edges, takes {:.2} s", batches.0.len(), start.elapsed().as_secs_f64());
                    let is_single_oe = graph_schema.is_single_oe(src_label, edge_label, dst_label);
                    let is_single_ie = graph_schema.is_single_ie(src_label, edge_label, dst_label);
                    let prefix = partition_dir.as_os_str().to_str().unwrap().to_string();
                    if graph_schema.get_edge_header(src_label, edge_label, dst_label).unwrap().len() == 0 {
                        dump_handles.push(thread::spawn(move || {
                            dump_edge_without_properties(
                                batches.0, 
                                batches.1, 
                                batches.2,
                                batches.3,
                                format!("{}/oe_{}_{}_{}", &prefix, src_label as i32, edge_label as i32, dst_label as i32),
                                format!("{}/ie_{}_{}_{}", &prefix, src_label as i32, edge_label as i32, dst_label as i32),
                                is_single_oe, 
                                is_single_ie);
                        }));
                    } else {
                        dump_handles.push(thread::spawn(move || {
                            dump_edge_with_properties(
                                batches.0, 
                                batches.1, 
                                batches.2,
                                batches.3,
                                batches.4,
                                format!("{}/oe_{}_{}_{}", &prefix, src_label as i32, edge_label as i32, dst_label as i32),
                                format!("{}/ie_{}_{}_{}", &prefix, src_label as i32, edge_label as i32, dst_label as i32),
                                format!("{}/oep_{}_{}_{}", &prefix, src_label as i32, edge_label as i32, dst_label as i32),
                                format!("{}/iep_{}_{}_{}", &prefix, src_label as i32, edge_label as i32, dst_label as i32),
                                is_single_oe, 
                                is_single_ie);
                        }));
                    }
                }
            }
        }
    }

    let locked = Arc::try_unwrap(vertex_maps).unwrap();
    let vertex_maps = locked.into_inner().unwrap();
    for (label, vm) in vertex_maps.into_iter().enumerate() {
        let prefix = partition_dir.as_os_str().to_str().unwrap().to_string();
        dump_handles.push(thread::spawn(move || {
            vm.dump(
                format!("{}/vm_{}", &prefix, label),
                format!("{}/vmc_{}", &prefix, label),
                format!("{}/vm_{}_tomb", &prefix, label),
            );
        }));
    }

    let start = Instant::now();
    for handle in dump_handles {
        handle.join().unwrap();
    }
    info!("waiting for dump takes: {:.2} s", start.elapsed().as_secs_f64());
    info!("loading graph takes: {:.2} s", very_start.elapsed().as_secs_f64());
}