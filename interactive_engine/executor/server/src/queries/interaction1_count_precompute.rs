use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::net::SocketAddr;

use bmcsr::columns::*;
use bmcsr::csr::NbrIter;
use bmcsr::date_time::{DateTime, parse_datetime};
use bmcsr::graph_db::GraphDB;
use bmcsr::ldbc_parser::{LDBCVertexParser, LDBCEdgeParser};
use bmcsr::schema::Schema;
use bmcsr::sub_graph::{SingleSubGraph, SubGraph};
use bmcsr::graph_modifier::*;
use bmcsr::utils::get_partition;
use crossbeam_channel::Sender;
use crossbeam_utils::sync::ShardedLock;
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::{get_servers_len, JobConf};
use pegasus_network::{InboxRegister, NetData, set_msg_sender, set_recv_register};

#[no_mangle]
pub fn interaction1_count_precompute(
    conf: JobConf, graph: &'static GraphDB<usize, usize>,
    input_params: HashMap<String, String>, alias_data: Option<Arc<Mutex<HashMap<u32, Vec<AliasData>>>>>,
) -> Box<
    dyn Fn(
        &mut Source<Vec<AliasData>>,
        ResultSink<(u32, Option<Vec<AliasData>>, Option<Vec<WriteOperation>>, Option<Vec<u8>>)>,
    ) -> Result<(), BuildJobError>,
> {
    pegasus_common::logs::init_log();
    let workers = conf.workers;
    let server_len = conf.servers().len();
    let subgraph_1_8_1_in: SubGraph<'static, usize, usize> =
        graph.get_sub_graph(1, 8, 1, bmcsr::graph::Direction::Incoming);
    let subgraph_1_8_1_out: SubGraph<'static, usize, usize> =
        graph.get_sub_graph(1, 8, 1, bmcsr::graph::Direction::Outgoing);
    let subgraph_2_0_1_in: SubGraph<'static, usize, usize> =
        graph.get_sub_graph(1, 0, 2, bmcsr::graph::Direction::Incoming);
    let subgraph_2_3_2_out: SingleSubGraph<'static, usize, usize> =
        graph.get_single_sub_graph(2, 3, 2, bmcsr::graph::Direction::Outgoing);
    let subgraph_2_3_3_out: SingleSubGraph<'static, usize, usize> =
        graph.get_single_sub_graph(2, 3, 3, bmcsr::graph::Direction::Outgoing);
    let subgraph_3_0_1_out: SingleSubGraph<'static, usize, usize> =
        graph.get_single_sub_graph(3, 0, 1, bmcsr::graph::Direction::Outgoing);
    let subgraph_2_0_1_out: SingleSubGraph<'static, usize, usize> =
        graph.get_single_sub_graph(2, 0, 1, bmcsr::graph::Direction::Outgoing);
    Box::new(
        move |input: &mut Source<Vec<AliasData>>,
              output: ResultSink<(
                  u32,
                  Option<Vec<AliasData>>,
                  Option<Vec<WriteOperation>>,
                  Option<Vec<u8>>,
              )>| {
            let worker_id = input.get_worker_index() % workers;
            let input_data = if let Some(alias_data) = &alias_data {
                let alias = alias_data.lock()
                    .expect("Empty alias")
                    .remove(&worker_id).unwrap();
                alias
            } else { vec![] };
            let stream_0 = input.input_from(vec![input_data])?;
            let stream_1 = stream_0.flat_map(move |_| {
                let mut result_1 = vec![];
                let vertex_0_num = graph.get_vertices_num(1);
                let vertex_0_local_num = vertex_0_num / workers as usize + 1;
                let mut vertex_0_start = vertex_0_local_num * worker_id as usize;
                let mut vertex_0_end = vertex_0_local_num * (worker_id + 1) as usize;
                vertex_0_start = std::cmp::min(vertex_0_start, vertex_0_num);
                vertex_0_end = std::cmp::min(vertex_0_end, vertex_0_num);
                for i in vertex_0_start..vertex_0_end {
                    let vertex_global_id = graph.get_global_id(i, 1).unwrap() as u64;
                    if vertex_global_id != u64::MAX {
                        result_1.push(vertex_global_id);
                    }
                }
                Ok(result_1.into_iter())
            })?;
            let stream_2 = stream_1.repartition(move |input| {
                Ok(get_partition(&input, workers as usize, server_len))
            });
            let stream_3 = stream_2.flat_map(move |i0| {
                let mut result_3 = vec![];
                if i0 != u64::MAX {
                    if let Some((vertex_label, vertex_id)) = graph.vertex_map.get_internal_id(i0 as usize) {
                        if vertex_label == 1 {
                            if vertex_id < graph.get_vertices_num(1) {
                                if let Some(edges) = subgraph_2_0_1_in.get_adj_list(vertex_id) {
                                    for e in edges {
                                        result_3.push(graph.get_global_id(*e, 2).unwrap() as u64);
                                    }
                                }
                            }
                        }
                    }
                } else { result_3.push(u64::MAX); }
                Ok(result_3.into_iter().map(move |res| (i0, res)))
            })?;
            let stream_4 = stream_3.repartition(move |input| {
                Ok(get_partition(&input.1, workers as usize, server_len))
            });
            let stream_5 = stream_4;
            let stream_6 = stream_5.repartition(move |input| {
                Ok(get_partition(&input.1, workers as usize, server_len))
            });
            let stream_7 = stream_6.flat_map(move |(i0, i1)| {
                let mut result_7 = vec![];
                if i1 != u64::MAX {
                    if let Some((vertex_label, vertex_id)) = graph.vertex_map.get_internal_id(i1 as usize) {
                        if vertex_label == 2 {
                            if vertex_id < graph.get_vertices_num(2) {
                                if let Some(edges) = subgraph_2_3_2_out.get_adj_list(vertex_id) {
                                    for e in edges {
                                        result_7.push(graph.get_global_id(*e, 2).unwrap() as u64);
                                    }
                                }
                            }
                        }
                        if vertex_label == 2 {
                            if vertex_id < graph.get_vertices_num(2) {
                                if let Some(edges) = subgraph_2_3_3_out.get_adj_list(vertex_id) {
                                    for e in edges {
                                        result_7.push(graph.get_global_id(*e, 3).unwrap() as u64);
                                    }
                                }
                            }
                        }
                    }
                } else { result_7.push(u64::MAX); }
                Ok(result_7.into_iter().map(move |res| (i0, i1, res)))
            })?;
            let stream_8 = stream_7.repartition(move |input| {
                Ok(get_partition(&input.2, workers as usize, server_len))
            });
            let stream_9 = stream_8.flat_map(move |(i0, i1, i2)| {
                let mut result_9 = vec![];
                if i2 != u64::MAX {
                    if let Some((vertex_label, vertex_id)) = graph.vertex_map.get_internal_id(i2 as usize) {
                        if vertex_label == 2 {
                            if vertex_id < graph.get_vertices_num(2) {
                                if let Some(edges) = subgraph_2_0_1_out.get_adj_list(vertex_id) {
                                    for e in edges {
                                        result_9.push(graph.get_global_id(*e, 1).unwrap() as u64);
                                    }
                                }
                            }
                        }
                        if vertex_label == 3 {
                            if vertex_id < graph.get_vertices_num(3) {
                                if let Some(edges) = subgraph_3_0_1_out.get_adj_list(vertex_id) {
                                    for e in edges {
                                        result_9.push(graph.get_global_id(*e, 1).unwrap() as u64);
                                    }
                                }
                            }
                        }
                    }
                } else { result_9.push(u64::MAX); }
                Ok(result_9.into_iter().map(move |res| (i0, i1, i2, res)))
            })?;
            let stream_10 = stream_9.repartition(move |input| {
                Ok(get_partition(&input.3, workers as usize, server_len))
            })
                .fold_partition(HashMap::<u64, HashMap<u64, i64>>::new(), move || {
                    move |mut collect, (i0, i1, i2, i3)| {
                        let key0 = i3;
                        let key1 = i0;
                        let value0 = i0;
                        let key = (key0, key1);
                        if let Some(mut data) = collect.get_mut(&key0) {
                            if let Some(mut count) = data.get_mut(&key1) {
                                if value0 != u64::MAX {
                                    *count += 1;
                                }
                            } else {
                                data.insert(key1, if value0 != u64::MAX { 1 } else { 0 });
                            }
                        } else {
                            let mut data = HashMap::new();
                            data.insert(key1, if value0 != u64::MAX { 1 } else { 0 });
                            collect.insert(key0, data);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|mut map| {
                    let mut result = vec![];
                    for (key, value) in map.into_iter() {
                        let mut person_set = vec![];
                        for (k, v) in value.into_iter() {
                            person_set.push((k, v));
                        }
                        result.push((key, person_set));
                    }
                    Ok(result.into_iter())
                })?;
            let stream_11 = stream_10.flat_map(move |(i0, mut i1)| {
                let mut results = vec![];
                let mut target_map = HashMap::new();
                if i0 != u64::MAX {
                    if let Some((vertex_label, vertex_id)) = graph.vertex_map.get_internal_id(i0 as usize) {
                        if vertex_label == 1 {
                            if vertex_id < graph.get_vertices_num(1) {
                                if let Some(edges) = subgraph_1_8_1_in.get_adj_list_with_offset(vertex_id) {
                                    for (e, offset) in edges {
                                        target_map.insert(graph.get_global_id(e, 1).unwrap() as u64, (offset as u64, 0));
                                    }
                                }
                                if let Some(edges) = subgraph_1_8_1_out.get_adj_list_with_offset(vertex_id) {
                                    for (e, offset) in edges {
                                        target_map.insert(graph.get_global_id(e, 1).unwrap() as u64, (offset as u64, 1));
                                    }
                                }
                            }
                        }
                    }
                }
                for (k, v) in i1.into_iter() {
                    if let Some(data) = target_map.get(&k) {
                        results.push((i0, *data, v))
                    }
                }
                Ok(results.into_iter())
            })?;
            let stream_12 = stream_11.repartition(move |input| {
                Ok(get_partition(&input.0, workers as usize, server_len))
            }).fold_partition((vec![], vec![], vec![], vec![]), move || {
                move |(mut in_index_set, mut in_data_set, mut out_index_set, mut out_data_set), (i0, i1, i2)| {
                    if i1.1 == 0 {
                        in_index_set.push(i1.0 as usize);
                        in_data_set.push(i2);
                    } else {
                        out_index_set.push(i1.0 as usize);
                        out_data_set.push(i2);
                    }
                    Ok((in_index_set, in_data_set, out_index_set, out_data_set))
                }
            })?
                .unfold(move |(in_index_set, in_data_set, out_index_set, out_data_set)| {
                    let mut write_operation = vec![];
                    let mut src_column_mappings = vec![];
                    let mut dst_column_mappings = vec![];
                    dst_column_mappings.push(ColumnMappings::new(0, "in_offset".to_string(), DataType::ID, "in_offset".to_string()));
                    let mut column_mappings = vec![];
                    column_mappings.push(ColumnMappings::new(1, "interaction1_count".to_string(), DataType::Int64, "interaction1_count".to_string()));
                    let mut data_frame = DataFrame::new_edges_ids(in_index_set);
                    data_frame.add_column(ColumnMetadata::new(Box::new(Int64Column::from(in_data_set)), "interaction1_count".to_string(), DataType::Int64));
                    let inputs = vec![Input::memory(data_frame)];
                    let edge_mappings = EdgeMappings::new(1, 8, 1, inputs, src_column_mappings, dst_column_mappings, column_mappings);
                    write_operation.push(WriteOperation::set_edges(edge_mappings));
                    let mut src_column_mappings = vec![];
                    let mut dst_column_mappings = vec![];
                    src_column_mappings.push(ColumnMappings::new(0, "out_offset".to_string(), DataType::ID, "out_offset".to_string()));
                    let mut column_mappings = vec![];
                    column_mappings.push(ColumnMappings::new(1, "interaction1_count".to_string(), DataType::Int64, "interaction1_count".to_string()));
                    let mut data_frame = DataFrame::new_edges_ids(out_index_set);
                    data_frame.add_column(ColumnMetadata::new(Box::new(Int64Column::from(out_data_set)), "interaction1_count".to_string(), DataType::Int64));
                    let inputs = vec![Input::memory(data_frame)];
                    let edge_mappings = EdgeMappings::new(1, 8, 1, inputs, src_column_mappings, dst_column_mappings, column_mappings);
                    write_operation.push(WriteOperation::set_edges(edge_mappings));
                    Ok(Some((worker_id, None, Some(write_operation), None)).into_iter())
                })?;
            stream_12.sink_into(output)
        })
}
