use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortBy, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::{get_current_worker, JobConf};
use std::cmp::max;

pub fn bi5(conf: JobConf, tag: String) -> ResultStream<(u64, i32, i32, i32, i32)> {
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
                        .get_all_vertices(Some(&vec![7]))
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
                        .filter(|vertex| vertex.get_label()[0] == 2 || vertex.get_label()[0] == 3)
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
                    let likes_count = super::graph::GRAPH
                        .get_in_vertices(message_internal_id as DefaultId, Some(&vec![13]))
                        .count() as i32;
                    let reply_count = super::graph::GRAPH
                        .get_in_vertices(message_internal_id as DefaultId, Some(&vec![3]))
                        .count() as i32;
                    Ok((person_internal_id, likes_count, reply_count))
                })?
                .repartition(move |(id, _, _)| {
                    Ok(super::graph::get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .fold_partition(HashMap::<u64, (i32, i32, i32)>::new(), || {
                    |mut collect, (person_internal_id, likes_count, reply_count)| {
                        if let Some(data) = collect.get_mut(&person_internal_id) {
                            data.0 += 1;
                            data.1 += likes_count;
                            data.2 += reply_count;
                        } else {
                            collect.insert(person_internal_id, (1, likes_count, reply_count));
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut person_list = vec![];
                    for (person_internal_id, (message_count, likes_count, reply_count)) in map {
                        let person_vertex = super::graph::GRAPH
                            .get_vertex(person_internal_id as DefaultId)
                            .unwrap();
                        let person_id = person_vertex
                            .get_property("id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        person_list.push((
                            person_id,
                            reply_count,
                            likes_count,
                            message_count,
                            message_count + 2 * reply_count + 10 * likes_count,
                        ));
                    }
                    Ok(person_list.into_iter())
                })?
                .sort_limit_by(100, |x, y| x.4.cmp(&y.4).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi5 job failure")
}
