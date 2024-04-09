use bmcsr::columns::*;
use bmcsr::csr::NbrIter;
use bmcsr::date_time::parse_datetime;
use bmcsr::graph_db::GraphDB;
use bmcsr::ldbc_parser::LDBCVertexParser;
use bmcsr::sub_graph::{SingleSubGraph, SubGraph};
use graph_index::types::DataType as IndexDataType;
use graph_index::types::{ArrayData, Item, LabelId};
use graph_index::utils::*;
use graph_index::GraphIndex;
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::{ResultSink, ResultStream};
use pegasus::{get_servers_len, JobConf};
use std::collections::{HashMap, HashSet};

pub fn bi9(
    conf: JobConf, graph: &'static GraphDB<usize, usize>, graph_index: &GraphIndex,
    input_params: HashMap<String, String>,
) -> ResultStream<Vec<u8>> {
    let workers = conf.workers;
    let server_len = conf.servers().len();
    let property_firstName_1 = &graph.vertex_prop_table[1 as usize]
        .get_column_by_name("firstName")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;
    let property_lastName_1 = &graph.vertex_prop_table[1 as usize]
        .get_column_by_name("lastName")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;
    let property_creationDate_2 = &graph.vertex_prop_table[2 as usize]
        .get_column_by_name("creationDate")
        .as_any()
        .downcast_ref::<DateTimeColumn>()
        .unwrap()
        .data;
    let property_creationDate_3 = &graph.vertex_prop_table[3 as usize]
        .get_column_by_name("creationDate")
        .as_any()
        .downcast_ref::<DateTimeColumn>()
        .unwrap()
        .data;
    let subgraph_3_0_1_in: SubGraph<'static, usize, usize> =
        graph.get_sub_graph(1, 0, 3, bmcsr::graph::Direction::Incoming);
    let subgraph_2_3_3_in: SubGraph<'static, usize, usize> =
        graph.get_sub_graph(3, 3, 2, bmcsr::graph::Direction::Incoming);
    let subgraph_2_3_2_in: SubGraph<'static, usize, usize> =
        graph.get_sub_graph(2, 3, 2, bmcsr::graph::Direction::Incoming);
    let startDate = parse_datetime(input_params.get("startDate").unwrap());
    let endDate = parse_datetime(input_params.get("endDate").unwrap());
    pegasus::run(conf, || {
        move |input, output| {
            let worker_id = input.get_worker_index() % workers;
            let stream_0 = input.input_from(vec![0])?;
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
                        result_1.push(LDBCVertexParser::<usize>::encode_local_id(i, 1) as u64);
                    }
                }
                Ok(result_1.into_iter())
            })?;
            let stream_2 =
                stream_1.repartition(move |input| Ok(get_partition(&input, workers as usize, server_len)));
            let stream_3 = stream_2.flat_map(move |i0| {
                let mut result_3 = vec![];
                let (vertex_label, vertex_id) = LDBCVertexParser::<usize>::decode_local_id(i0 as usize);
                if let Some(edges) = subgraph_3_0_1_in.get_adj_list(vertex_id) {
                    for e in edges {
                        let var0 = property_creationDate_3[*e];
                        if var0 >= startDate && var0 <= endDate {
                            result_3.push(LDBCVertexParser::<usize>::encode_local_id(*e, 3) as u64);
                        }
                    }
                }
                Ok(result_3.into_iter().map(move |res| (i0, res)))
            })?;
            let stream_4 =
                stream_3.repartition(move |input| Ok(get_partition(&input.1, workers as usize, server_len)));
            let stream_10 = stream_4.flat_map(move |(i0, i1)| {
                let mut result_7 = vec![];
                let mut count = 0;
                let mut input = vec![i1];
                let mut temp = vec![];
                for i in 0..1000 {
                    if input.is_empty() {
                        break;
                    }
                    for j in input.drain(0..) {
                        let (vertex_label, vertex_id) = LDBCVertexParser::<usize>::decode_local_id(j as usize);
                        let vertex_label = LDBCVertexParser::<usize>::get_label_id(j as usize);
                        let condition = {
                            let var0 = if vertex_label == 2 {
                                property_creationDate_2[vertex_id]
                            } else if vertex_label == 3 {
                                property_creationDate_3[vertex_id]
                            } else {
                                panic!("Unexpected label: {}", vertex_label)
                            };
                            let var1 = if vertex_label == 2 {
                                property_creationDate_2[vertex_id]
                            } else if vertex_label == 3 {
                                property_creationDate_3[vertex_id]
                            } else {
                                panic!("Unexpected label: {}", vertex_label)
                            };

                            var0 >= startDate && var1 <= endDate
                        };
                        if condition {
                            count += 1;
                        }
                        if vertex_label == 2 {
                            if let Some(edges) = subgraph_2_3_2_in.get_adj_list(vertex_id) {
                                for e in edges {
                                    temp.push(LDBCVertexParser::<usize>::encode_local_id(*e, 2) as u64);
                                }
                            }
                        }
                        if vertex_label == 3 {
                            if let Some(edges) = subgraph_2_3_3_in.get_adj_list(vertex_id) {
                                for e in edges {
                                    temp.push(LDBCVertexParser::<usize>::encode_local_id(*e, 2) as u64);
                                }
                            }
                        }
                    }
                    std::mem::swap(&mut input, &mut temp);
                }
                result_7.push((i0, i1, count));
                Ok(result_7.into_iter())
            })?;
            let stream_5 =
                stream_10.repartition(move |input| Ok(get_partition(&input.0, workers as usize, server_len)));
            let stream_11 = stream_5
                .fold_partition(HashMap::<u64, (i32, i32)>::new(), || {
                    |mut collect, (i0, i1, i2)| {
                        if let Some(mut data) = collect.get_mut(&i0) {
                            data.0 += 1;
                            data.1 += i2;
                        } else {
                            collect.insert(i0, (1, i2));
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut result = vec![];
                    for (key, value) in map.iter() {
                        result.push((*key, value.0, value.1));
                    }
                    Ok(result.into_iter())
                })?;
            let stream_12 = stream_11.map(move |(i0, i1, i2)| {
                let (vertex_label, vertex_id) = LDBCVertexParser::<usize>::decode_local_id(i0 as usize);
                let vertex_global_id = graph
                    .get_global_id(vertex_id, vertex_label)
                    .unwrap();
                let var0 = LDBCVertexParser::<usize>::get_original_id(vertex_global_id as usize) as u64;
                let output0 = var0;
                let (vertex_label, vertex_id) = LDBCVertexParser::<usize>::decode_local_id(i0 as usize);
                let var0 = property_firstName_1[vertex_id].clone();
                let output1 = var0;
                let (vertex_label, vertex_id) = LDBCVertexParser::<usize>::decode_local_id(i0 as usize);
                let var0 = property_lastName_1[vertex_id].clone();
                let output2 = var0;
                let var0 = i1;

                let output3 = var0;
                let var0 = i2;

                let output4 = var0;
                Ok((output0, output1, output2, output3, output4))
            })?;
            let stream_13 = stream_12.sort_limit_by(100, |x, y| x.4.cmp(&y.4).reverse().then(x.0.cmp(&y.0)))?;
            let stream_14 = stream_13.map(|(i0, i1, i2, i3, i4)| {
                let result = format!("{}", i0)
                    + "|"
                    + &format!("{}", i1)
                    + "|"
                    + &format!("{}", i2)
                    + "|"
                    + &format!("{}", i3)
                    + "|"
                    + &format!("{}", i4);
                Ok(result.as_bytes().to_vec())
            })?;
            stream_14.sink_into(output)
        }
    }).expect("submit bi9 job failure")
}
