use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::{get_current_worker, JobConf};
use std::cmp::max;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn bi4(
    conf: JobConf, tag_class: String, country: String,
) -> ResultStream<(u64, String, u64, u64, i32)> {
    let workers = conf.workers;
    pegasus::run(conf, || {
        let tag_class = tag_class.clone();
        let country = country.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![0])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(|source| {
                    Ok(super::graph::GRAPH
                        .get_all_vertices(Some(&vec![4]))
                        .map(move |vertex| vertex.get_id() as u64))
                })?
                .repartition(move |id| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .map(|forum_internal_id| {
                    let person_internal_id = super::graph::GRAPH
                        .get_out_vertices(forum_internal_id as DefaultId, Some(&vec![7]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((forum_internal_id, person_internal_id))
                })?
                .repartition(move |(_, id)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .map(|(forum_internal_id, person_internal_id)| {
                    let city_internal_id = super::graph::GRAPH
                        .get_out_vertices(person_internal_id as DefaultId, Some(&vec![11]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((forum_internal_id, person_internal_id, city_internal_id))
                })?
                .repartition(move |(_, _, id)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .map(|(forum_internal_id, person_internal_id, city_internal_id)| {
                    let country_internal_id = super::graph::GRAPH
                        .get_out_vertices(city_internal_id as DefaultId, Some(&vec![17]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((forum_internal_id, person_internal_id, country_internal_id))
                })?
                .repartition(move |(_, _, id)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .filter_map(move |(forum_internal_id, person_internal_id, country_internal_id)| {
                    let country_vertex = super::graph::GRAPH
                        .get_vertex(country_internal_id as DefaultId)
                        .unwrap();
                    let country_name = country_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if country_name == country {
                        Ok(Some((forum_internal_id, person_internal_id)))
                    } else {
                        Ok(None)
                    }
                })?
                .repartition(move |(id, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .flat_map(|(forum_internal_id, person_internal_id)| {
                    Ok(super::graph::GRAPH
                        .get_out_vertices(forum_internal_id as DefaultId, Some(&vec![5]))
                        .map(move |vertex| (vertex.get_id() as u64, forum_internal_id, person_internal_id)))
                })?
                .repartition(move |(id, _, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .flat_map(|(post_internal_id, forum_internal_id, person_internal_id)| {
                    Ok(super::graph::GRAPH
                        .get_out_vertices(post_internal_id as DefaultId, Some(&vec![1]))
                        .map(move |vertex| {
                            (
                                vertex.get_id() as u64,
                                post_internal_id,
                                forum_internal_id,
                                person_internal_id,
                            )
                        }))
                })?
                .repartition(move |(id, _, _, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .map(|(tag_internal_id, post_internal_id, forum_internal_id, person_internal_id)| {
                    let tag_class_internal_id = super::graph::GRAPH
                        .get_out_vertices(tag_internal_id as DefaultId, Some(&vec![21]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((tag_class_internal_id, post_internal_id, forum_internal_id, person_internal_id))
                })?
                .repartition(move |(id, _, _, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .filter_map(
                    move |(tag_class_internal_id, post_internal_id, forum_internal_id, person_internal_id)| {
                        let tag_class_vertex = super::graph::GRAPH
                            .get_vertex(tag_class_internal_id as DefaultId)
                            .unwrap();
                        let tag_class_name = tag_class_vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if tag_class_name == tag_class {
                            Ok(Some((forum_internal_id, post_internal_id, person_internal_id)))
                        } else {
                            Ok(None)
                        }
                    },
                )?
                .repartition(move |(id, _, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .fold_partition(HashMap::<(u64, u64), HashSet<u64>>::new(), || {
                    |mut collect, (forum_internal_id, post_internal_id, person_internal_id)| {
                        if let Some(data) = collect.get_mut(&(forum_internal_id, person_internal_id)) {
                            data.insert(post_internal_id);
                        } else {
                            let mut set = HashSet::<u64>::new();
                            set.insert(post_internal_id);
                            collect.insert((forum_internal_id, person_internal_id), set);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut forum_list = vec![];
                    for ((forum_internal_id, person_internal_id), post_set) in map {
                        forum_list.push((forum_internal_id, person_internal_id, post_set.len() as i32));
                    }
                    Ok(forum_list.into_iter())
                })?
                .sort_limit_by(20, |x, y| x.2.cmp(&y.2).reverse().then(x.0.cmp(&y.0)))?
                .repartition(move |(_, id, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .map(|(forum_internal_id, person_internal_id, post_count)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let person_id = person_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    Ok((forum_internal_id, person_id, post_count))
                })?
                .repartition(move |(id, _, _)| {
                    Ok(super::graph::get_partition(
                        id,
                        workers as usize,
                        pegasus::get_servers_len(),
                    ))
                })
                .map(|(forum_internal_id, person_id, post_count)| {
                    let forum_vertex = super::graph::GRAPH
                        .get_vertex(forum_internal_id as DefaultId)
                        .unwrap();
                    let forum_id = forum_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let forum_title = forum_vertex
                        .get_property("title")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let forum_date = forum_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    Ok((forum_id, forum_title, forum_date, person_id, post_count))
                })?
                .sink_into(output)
        }
    })
    .expect("submit bi4 job failure")
}
