use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortBy, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::{get_current_worker, JobConf};
use std::cmp::max;

pub fn bi6(conf: JobConf, tag: String) -> ResultStream<(u64, i32)> {
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
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .repartition(move |id| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .map(|message_internal_id| {
                    let person_internal_id = super::graph::GRAPH
                        .get_out_vertices(message_internal_id as DefaultId, Some(&vec![0]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((person_internal_id, message_internal_id))
                })?
                .repartition(move |(id, _)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .map(|(person_internal_id, message_internal_id)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let person_id = person_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    Ok((person_id, message_internal_id))
                })?
                .repartition(move |(_, id)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .flat_map(|(person_id, message_internal_id)| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(message_internal_id as DefaultId, Some(&vec![13]))
                        .map(move |vertex| (person_id, vertex.get_id() as u64)))
                })?
                .repartition(move |(_, id)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .flat_map(|(person_id, likes_person_id)| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(likes_person_id as DefaultId, Some(&vec![0]))
                        .map(move |vertex| (person_id, vertex.get_id() as u64)))
                })?
                .repartition(move |(_, id)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .map(|(person_id, message_internal_id)| {
                    let likes_count = super::graph::GRAPH
                        .get_in_vertices(message_internal_id as DefaultId, Some(&vec![13]))
                        .count() as i32;
                    Ok((person_id, likes_count))
                })?
                .repartition(move |(id, _)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .fold_partition(HashMap::<u64, i32>::new(), || {
                    |mut collect, (person_id, likes_count)| {
                        if let Some(data) = collect.get_mut(&person_id) {
                            *data += likes_count;
                        } else {
                            collect.insert(person_id, likes_count);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut person_list = vec![];
                    for (person_id, score) in map {
                        person_list.push((person_id, score));
                    }
                    Ok(person_list.into_iter())
                })?
                .sort_limit_by(100, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi6 job failure")
}
