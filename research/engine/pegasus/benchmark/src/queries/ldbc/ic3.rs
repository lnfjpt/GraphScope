use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::sync::Arc;

use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, HasAny, HasKey, IterCondition,
    Iteration, Map, PartitionByKey, Sink, SortBy, SortLimitBy, Unary,
};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic3(
    conf: JobConf, person_id: u64, country_x: String, country_y: String, start_date: u64, duration: i32,
) -> ResultStream<(u64, String, String, i32, i32, i32)> {
    // Todo: parse timestamp here
    pegasus::run(conf, || {
        let end_date = start_date + duration as u64;
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .iterate_emit_until(IterCondition::max_iters(2), EmitKind::After, |start| {
                    start
                        .repartition(|id| Ok(*id))
                        .flat_map(move |person_id| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_id as DefaultId, Some(&vec![12]))
                                .map(|vertex| vertex.get_id() as u64)
                                .filter(move |id| {
                                    ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize != *id as usize
                                }))
                        })
                })?
                .dedup()?
                .repartition(|id| Ok(*id))
                .map(|person_internal_id| {
                    let mut city_id = 0;
                    for i in super::graph::GRAPH
                        .get_out_vertices(person_internal_id as DefaultId, Some(&vec![11]))
                    {
                        city_id = i.get_id();
                        break;
                    }
                    let mut country_id = 0;
                    for i in super::graph::GRAPH.get_out_vertices(city_id as DefaultId, Some(&vec![17])) {
                        country_id = i.get_id() as u64;
                        break;
                    }
                    Ok((person_internal_id, country_id))
                })?
                .filter_map(move |(person_internal_id, country_id)| {
                    let country_vertex = super::graph::GRAPH
                        .get_vertex(country_id as DefaultId)
                        .unwrap();
                    let country_name = country_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if country_name != country_x && country_name != country_y {
                        Ok(Some(person_internal_id))
                    } else {
                        Ok(None)
                    }
                })?
                .apply(|sub| {
                    sub.flat_map(|person_id| {
                        Ok(super::graph::GRAPH
                            .get_in_vertices(person_id as DefaultId, Some(&vec![0]))
                            .map(|vertex| vertex.get_id() as u64))
                    })?
                    .filter_map(|message_internal_id| {
                        let vertex = super::graph::GRAPH
                            .get_vertex(message_internal_id as DefaultId)
                            .unwrap();
                        let create_time = vertex
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        if create_time >= start_date && create_time < end_date {
                            Ok(Some(message_internal_id))
                        } else {
                            Ok(None)
                        }
                    })?
                    .flat_map(|message_internal_id| {
                        Ok(super::graph::GRAPH
                            .get_out_vertices(message_internal_id as DefaultId, Some(&vec![11]))
                            .map(move |vertex| (message_internal_id, vertex.get_id() as u64)))
                    })?
                    .filter_map(|(message_internal_id, country_id)| {
                        let country_vertex = super::graph::GRAPH
                            .get_vertex(country_id as DefaultId)
                            .unwrap();
                        let country = country_vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if country == country_x {
                            Ok(Some((1, 0)))
                        } else if country == country_y {
                            Ok(Some((0, 1)))
                        } else {
                            Ok(None)
                        }
                    })?
                    .fold((0, 0), || |a, b| Ok((a.0 + b.0, a.1 + b.1)))
                })?
                .filter_map(|(person_internal_id, count)| {
                    if count.0 > 0 && count.1 > 0 {
                        Ok(Some((person_internal_id, count)))
                    } else {
                        Ok(None)
                    }
                })?
                .sort_limit_by(20, |(id_x, count_x), (id_y, count_y)| {
                    (count_x.0 + count_x.1)
                        .cmp(&(count_y.0 + count_y.1))
                        .reverse()
                        .then(id_x.cmp(&id_y))
                })?
                .map(|(person_internal_id, count)| {
                    let vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let person_id = vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let first_name = vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let last_name = vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((person_id, first_name, last_name, count.0, count.1, count.0 + count.1))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic3 job failure")
}
