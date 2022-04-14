use std::collections::HashMap;

use graph_store::prelude::*;
use pegasus::api::{Filter, Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;
use std::cmp::max;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn bi2(
    conf: JobConf, start_date: String, end_date: String, country0: String, country1: String,
) -> ResultStream<(String, i32, String, i32, String, i32)> {
    let start_date = super::graph::parse_datetime(&start_date).unwrap();
    let end_date = super::graph::parse_datetime(&end_date).unwrap();
    pegasus::run(conf, || {
        let country0 = country0.clone();
        let country1 = country1.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![(start_date, end_date)])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(|(start_date, end_date)| {
                    Ok(super::graph::GRAPH
                        .get_all_vertices(Some(&vec![2, 3]))
                        .filter_map(move |vertex| {
                            let create_date = vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            if create_date >= start_date && create_date <= end_date {
                                Some(vertex.get_id() as u64)
                            } else {
                                None
                            }
                        }))
                })?
                .repartition(|id| Ok(*id))
                .map(|message_internal_id| {
                    let person_internal_id = super::graph::GRAPH
                        .get_out_vertices(message_internal_id as DefaultId, Some(&vec![0]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((person_internal_id, message_internal_id))
                })?
                .filter_map(move |(person_internal_id, message_internal_id)| {
                    let city_internal_id = super::graph::GRAPH
                        .get_out_vertices(person_internal_id as DefaultId, Some(&vec![11]))
                        .next()
                        .unwrap()
                        .get_id();
                    let country_internal_id = super::graph::GRAPH
                        .get_out_vertices(city_internal_id, Some(&vec![17]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    let country_vertex = super::graph::GRAPH
                        .get_vertex(country_internal_id as DefaultId)
                        .unwrap();
                    let country_name = country_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if country_name == country0 || country_name == country1 {
                        Ok(Some((country_internal_id, person_internal_id, message_internal_id)))
                    } else {
                        Ok(None)
                    }
                })?
                .map(|(country_internal_id, person_internal_id, message_internal_id)| {
                    let tag_internal_id = super::graph::GRAPH
                        .get_out_vertices(message_internal_id as DefaultId, Some(&vec![1]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_internal_id as DefaultId)
                        .unwrap();
                    let create_date = message_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let month = (create_date / 1000000000 % 100) as i32;
                    Ok((country_internal_id, person_internal_id, month, tag_internal_id))
                })?
                .map(|(country_internal_id, person_internal_id, month, tag_internal_id)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let birthday = person_vertex
                        .get_property("birthday")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let birth_group = ((2013 - birthday / 10000) / 5) as i32;
                    let gender = person_vertex
                        .get_property("gender")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if gender == "male" {
                        Ok((country_internal_id, month, true, birth_group, tag_internal_id))
                    } else {
                        Ok((country_internal_id, month, false, birth_group, tag_internal_id))
                    }
                })?
                .fold(HashMap::<(u64, i32, bool, i32, u64), i32>::new(), || {
                    |mut collect, (country_internal_id, month, gender, birth_group, tag_internal_id)| {
                        if let Some(data) = collect.get_mut(&(
                            country_internal_id,
                            month,
                            gender,
                            birth_group,
                            tag_internal_id,
                        )) {
                            *data += 1;
                        } else {
                            collect.insert(
                                (country_internal_id, month, gender, birth_group, tag_internal_id),
                                1,
                            );
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut group_list = vec![];
                    for ((country_internal_id, month, gender, birth_group, tag_internal_id), count) in map {
                        group_list.push((
                            country_internal_id,
                            month,
                            gender,
                            birth_group,
                            tag_internal_id,
                            count,
                        ));
                    }
                    Ok(group_list.into_iter())
                })?
                .map(|(country_internal_id, month, gender, birth_group, tag_internal_id, count)| {
                    let country_vertex = super::graph::GRAPH
                        .get_vertex(country_internal_id as DefaultId)
                        .unwrap();
                    let country_name = country_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let tag_vertex = super::graph::GRAPH
                        .get_vertex(tag_internal_id as DefaultId)
                        .unwrap();
                    let tag_name = tag_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if gender {
                        Ok((country_name, month, "male".to_string(), birth_group, tag_name, count))
                    } else {
                        Ok((country_name, month, "female".to_string(), birth_group, tag_name, count))
                    }
                })?
                .sort_limit_by(100, |x, y| {
                    x.5.cmp(&y.5)
                        .reverse()
                        .then(x.4.cmp(&y.4))
                        .then(x.3.cmp(&y.3))
                        .then(x.2.cmp(&y.2))
                        .then(x.1.cmp(&y.1))
                        .then(x.0.cmp(&y.0))
                })?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
