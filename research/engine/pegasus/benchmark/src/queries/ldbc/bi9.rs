use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{
    CorrelatedSubTask, EmitKind, Filter, Fold, HasAny, IterCondition, Iteration, Map, Sink, SortBy,
    SortLimitBy,
};
use pegasus::result::ResultStream;
use pegasus::{get_current_worker, JobConf};
use std::cmp::max;

pub fn bi9(
    conf: JobConf, start_date: String, end_date: String,
) -> ResultStream<(u64, String, String, i32, i32)> {
    let workers = conf.workers;
    let start_date = super::graph::parse_datetime(&start_date).unwrap();
    let end_date = super::graph::parse_datetime(&end_date).unwrap();
    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            let worker_id = input.get_worker_index();
            stream
                .flat_map(move |source| {
                    let forum_vertices = super::graph::GRAPH.get_all_vertices(Some(&vec![1]));
                    let forum_count = forum_vertices.count();
                    let partial_count = forum_count / workers as usize + 1;
                    Ok(super::graph::GRAPH
                        .get_all_vertices(Some(&vec![1]))
                        .skip((worker_id % workers) as usize * partial_count)
                        .take(partial_count)
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .flat_map(move |person_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(person_internal_id as DefaultId, Some(&vec![0]))
                        .filter(|vertex| vertex.get_label()[0] == 3)
                        .map(move |vertex| (person_internal_id, vertex.get_id() as u64)))
                })?
                .repartition(move |(_, id)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .filter_map(move |(person_internal_id, post_internal_id)| {
                    let post_vertex = super::graph::GRAPH
                        .get_vertex(post_internal_id as DefaultId)
                        .unwrap();
                    let post_date = post_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    if post_date >= start_date && post_date <= end_date {
                        Ok(Some((person_internal_id, post_internal_id)))
                    } else {
                        Ok(None)
                    }
                })?
                .apply(|sub| {
                    sub.iterate_emit_until(IterCondition::max_iters(u32::MAX), EmitKind::After, |start| {
                        start
                            .repartition(move |(_, id)| {
                                Ok(super::graph::get_partition(
                                    id,
                                    workers as usize,
                                    pegasus::get_servers_len(),
                                ))
                            })
                            .flat_map(|(person_internal_id, reply_internal_id)| {
                                Ok(super::graph::GRAPH
                                    .get_in_vertices(reply_internal_id as DefaultId, Some(&vec![3]))
                                    .map(move |vertex| (person_internal_id, vertex.get_id() as u64)))
                            })?
                            .repartition(move |(_, id)| {
                                Ok(super::graph::get_partition(
                                    id,
                                    workers as usize,
                                    pegasus::get_servers_len(),
                                ))
                            })
                            .filter_map(move |(person_internal_id, reply_internal_id)| {
                                let reply_vertex = super::graph::GRAPH
                                    .get_vertex(reply_internal_id as DefaultId)
                                    .unwrap();
                               let reply_date = reply_vertex
                                    .get_property("creationDate")
                                    .unwrap()
                                    .as_u64()
                                    .unwrap();
                                if reply_date >= start_date && reply_date <= end_date {
                                    Ok(Some((person_internal_id, reply_internal_id)))
                                } else {
                                    Ok(None)
                                }
                            })
                    })?
                    .repartition(move |(id, _)| {
                        Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                    })
                    .fold_partition(0, || |count, _| Ok(count + 1))
                })?
                .repartition(move |((id, _), _)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .fold_partition(HashMap::<u64, (i32, i32)>::new(), || {
                    |mut collect, ((person_internal_id, post_internal_id), count)| {
                        if let Some(data) = collect.get_mut(&person_internal_id) {
                            data.0 += 1;
                            data.1 += count;
                        } else {
                            collect.insert(person_internal_id, (1, count));
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut person_list = vec![];
                    for (person_internal_id, (thread_count, message_count)) in map {
                        let person_vertex = super::graph::GRAPH
                            .get_vertex(person_internal_id as DefaultId)
                            .unwrap();
                        let person_id = person_vertex
                            .get_property("id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let first_name = person_vertex
                            .get_property("firstName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let last_name = person_vertex
                            .get_property("lastName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        person_list.push((person_id, first_name, last_name, thread_count, message_count));
                    }
                    Ok(person_list.into_iter())
                })?
                .sort_limit_by(100, |x, y| x.4.cmp(&y.4).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi9 job failure")
}
