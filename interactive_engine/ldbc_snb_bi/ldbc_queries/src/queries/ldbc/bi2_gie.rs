use std::collections::HashMap;

use graph_proxy::adapters::csr_store::read_graph::to_runtime_vertex;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::*;
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::schema::Schema;
use mcsr::types::DefaultId;
use mcsr::{
    columns::{DateTimeColumn, StringColumn},
    date,
};
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;

pub fn bi2_gie(conf: JobConf, date: String, tag_class: String) -> ResultStream<(String, i32, i32, i32)> {
    let workers = conf.workers;
    let start_date = date::parse_date(&date).unwrap();
    let start_date_ts = start_date.to_i32();
    let first_window = start_date.add_days(100);
    let first_window_ts = first_window.to_i32();
    let second_window = first_window.add_days(100);
    let second_window_ts = second_window.to_i32();
    println!("three data is {} {} {}", start_date_ts, first_window_ts, second_window_ts);

    let schema = &CSR.graph_schema;
    let tagclass_label = schema.get_vertex_label_id("TAGCLASS").unwrap();
    let forum_label = schema.get_vertex_label_id("FORUM").unwrap();
    let hastype_label = schema.get_edge_label_id("HASTYPE").unwrap();
    let hastag_label = schema.get_edge_label_id("HASTAG").unwrap();

    pegasus::run(conf, || {
        let tag_class = tag_class.clone();
        move |input, output| {
            let worker_id = input.get_worker_index();
            let stream = input.input_from(vec![0])?;
            stream
                .flat_map(move |_source| {
                    let mut tag_id_list = vec![];
                    let tagclass_vertices = CSR.get_all_vertices(Some(&vec![tagclass_label]));
                    let tagclass_count = tagclass_vertices.count();
                    let partial_count = tagclass_count / workers as usize + 1;
                    for vertex in CSR
                        .get_all_vertices(Some(&vec![tagclass_label]))
                        .skip((worker_id % workers) as usize * partial_count)
                        .take(partial_count)
                    {
                        let tagclass_name = vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if tagclass_name == tag_class {
                            let tagclass_internal_id = vertex.get_id();
                            for tag_vertex in
                                CSR.get_in_vertices(tagclass_internal_id, Some(&vec![hastype_label]))
                            {
                                let tag_internal_id = tag_vertex.get_id() as u64;
                                tag_id_list.push(tag_internal_id);
                            }
                        }
                    }
                    Ok(tag_id_list.into_iter())
                })?
                .repartition(move |id| Ok(get_partition(id, workers as usize, pegasus::get_servers_len())))
                .flat_map(move |tag_internal_id| {
                    let mut message_id_list = vec![];
                    // TODO: is this for count when 0 message?
                    message_id_list.push((0, tag_internal_id));
                    for vertex in
                        CSR.get_in_vertices(tag_internal_id as DefaultId, Some(&vec![hastag_label]))
                    {
                        if vertex.get_label() == forum_label {
                            continue;
                        }
                        message_id_list.push((vertex.get_id() as u64, tag_internal_id));
                    }
                    Ok(message_id_list.into_iter())
                })?
                .repartition(move |(id, _)| {
                    Ok(get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .fold_partition(HashMap::<u64, (i32, i32)>::new(), move || {
                    move |mut collect, (message_internal_id, tag_internal_id)| {
                        if message_internal_id == 0 {
                            if let None = collect.get_mut(&tag_internal_id) {
                                collect.insert(tag_internal_id, (0, 0));
                            }
                        } else {
                            let message_vertex = CSR
                                .get_vertex(message_internal_id as DefaultId)
                                .unwrap();
                            let create_date = message_vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_datetime()
                                .unwrap()
                                .date_to_i32();
                            if create_date >= start_date_ts && create_date < first_window_ts {
                                if let Some(data) = collect.get_mut(&tag_internal_id) {
                                    data.0 += 1;
                                } else {
                                    collect.insert(tag_internal_id, (1, 0));
                                }
                            } else if create_date >= first_window_ts && create_date < second_window_ts {
                                if let Some(data) = collect.get_mut(&tag_internal_id) {
                                    data.1 += 1;
                                } else {
                                    collect.insert(tag_internal_id, (0, 1));
                                }
                            }
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| Ok(map.into_iter()))?
                .fold(HashMap::<u64, (i32, i32)>::new(), || {
                    |mut collect, (tag_internal_id, (count1, count2))| {
                        if let Some(data) = collect.get_mut(&tag_internal_id) {
                            data.0 += count1;
                            data.1 += count2;
                        } else {
                            collect.insert(tag_internal_id, (count1, count2));
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    Ok(map
                        .into_iter()
                        .map(|(tag_internal_id, (count1, count2))| {
                            (tag_internal_id, count1, count2, (count1 - count2).abs())
                        }))
                })?
                .repartition(move |(id, _, _, _)| {
                    Ok(get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .map(|(tag_internal_id, count1, count2, diff)| {
                    let tag_vertex = CSR
                        .get_vertex(tag_internal_id as DefaultId)
                        .unwrap();
                    let tag_name = tag_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((tag_name, count1, count2, diff))
                })?
                .sort_limit_by(100, |x, y| x.3.cmp(&y.3).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
