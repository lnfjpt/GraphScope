use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortBy, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::{get_current_worker, JobConf};
use std::cmp::max;

pub fn bi7(conf: JobConf, tag: String) -> ResultStream<(String, i32)> {
    let workers = conf.workers;
    pegasus::run(conf, || {
        let tag = tag.clone();
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            let worker_id = input.get_worker_index();
            stream
                .flat_map(move |source| {
                    let forum_vertices = super::graph::GRAPH.get_all_vertices(Some(&vec![7]));
                    let forum_count = forum_vertices.count();
                    let partial_count = forum_count / workers as usize + 1;
                    Ok(super::graph::GRAPH
                        .get_all_vertices(Some(&vec![4]))
                        .skip((worker_id % workers) as usize * partial_count)
                        .take(partial_count)
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .filter_map(move |tag_internal_id| {
                    let tag_vertex = super::graph::GRAPH
                        .get_vertex(tag_internal_id as DefaultId)
                        .unwrap();
                    let tag_name = tag_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if tag_name == tag {
                        Ok(Some(tag_internal_id))
                    } else {
                        Ok(None)
                    }
                })?
                .flat_map(|tag_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(tag_internal_id as DefaultId, Some(&vec![1]))
                        .map(move |vertex| (vertex.get_id() as u64, tag_internal_id)))
                })?
                .repartition(move |(id, _)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .map(|(message_internal_id, tag_internal_id)| {
                    let comment_internal_id = super::graph::GRAPH
                        .get_in_vertices(message_internal_id as DefaultId, Some(&vec![3]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((comment_internal_id, tag_internal_id))
                })?
                .repartition(move |(id, _)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .flat_map(|(comment_internal_id, tag_internal_id)| {
                    let mut has_tag = false;
                    let mut tag_list = vec![];
                    for tag_vertex in super::graph::GRAPH
                        .get_out_vertices(comment_internal_id as DefaultId, Some(&vec![1]))
                    {
                        if tag_vertex.get_id() as u64 == tag_internal_id {
                            has_tag = true;
                            break;
                        }
                        tag_list.push(tag_vertex.get_id() as u64);
                    }
                    if has_tag {
                        Ok(vec![].into_iter())
                    } else {
                        Ok(tag_list.into_iter())
                    }
                })?
                .repartition(move |id| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .fold_partition(HashMap::<u64, i32>::new(), || {
                    |mut collect, tag_internal_id| {
                        if let Some(data) = collect.get_mut(&tag_internal_id) {
                            *data += 1;
                        } else {
                            collect.insert(tag_internal_id, 1);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut tag_list = vec![];
                    for (tag_internal_id, count) in map {
                        let tag_vertex = super::graph::GRAPH
                            .get_vertex(tag_internal_id as DefaultId)
                            .unwrap();
                        let tag_name = tag_vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        tag_list.push((tag_name, count));
                    }
                    Ok(tag_list.into_iter())
                })?
                .sort_limit_by(100, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi7 job failure")
}
