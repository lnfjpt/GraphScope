use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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

pub fn ic10(
    conf: JobConf, person_id: u64, month: i32,
) -> ResultStream<(u64, String, String, i32, String, String)> {
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .iterate_until(IterCondition::max_iters(2), |start| {
                    start
                        .repartition(|id| Ok(*id))
                        .flat_map(move |person_id| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_id as DefaultId, Some(&vec![12]))
                                .map(move |vertex| vertex.get_id() as u64)
                                .filter(move |id| {
                                    ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize != *id as usize
                                }))
                        })
                })?
                .dedup()?
                .filter_map(move |person_internal_id| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let mut birthday = person_vertex
                        .get_property("birthday")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    birthday = birthday % 10000;
                    if month < 12 {
                        if birthday >= month as u64 * 100 + 21 && birthday < (month + 1) as u64 * 100 + 22 {
                            return Ok(Some(person_internal_id));
                        }
                    } else if month == 12 {
                        if birthday >= month as u64 * 100 + 21 || birthday < 122 {
                            return Ok(Some(person_internal_id));
                        }
                    }
                    Ok(None)
                })?
                .filter_map(move |person_internal_id| {
                    let starter = ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize;
                    let friends =
                        super::graph::GRAPH.get_both_vertices(starter as DefaultId, Some(&vec![12]));
                    for i in friends {
                        if i.get_id() as u64 == person_internal_id {
                            return Ok(None);
                        }
                    }
                    Ok(Some((person_internal_id, starter as u64)))
                })?
                .flat_map(move |(person_internal_id, starter_internal_id)| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(person_internal_id as DefaultId, Some(&vec![0]))
                        .filter(|vertex| vertex.get_label()[0] == 3)
                        .map(move |vertex| {
                            (vertex.get_id() as u64, person_internal_id, starter_internal_id)
                        }))
                })?
                .map(|(post_internal_id, person_internal_id, starter_internal_id)| {
                    let tag_list = super::graph::GRAPH
                        .get_out_vertices(person_internal_id as DefaultId, Some(&vec![10]))
                        .map(|vertex| vertex.get_id() as u64).into_iter();
                    let post_tag = super::graph::GRAPH
                        .get_out_vertices(post_internal_id as DefaultId, Some(&vec![1]))
                        .map(|vertex| vertex.get_id() as u64).into_iter();
                    let mut id_list=vec![];
                    for i in tag_list {
                        id_list.push(i);
                    }
                    for i in post_tag {
                        if id_list.contains(&i) {
                            return Ok((person_internal_id, 1));
                        }
                    }
                    Ok((person_internal_id, -1))
                })?
                .fold(HashMap::<u64, i32>::new(), move || {
                    move |mut collect, (person_internal_id, interest)| {
                        if let Some(data) = collect.get_mut(&person_internal_id) {
                            *data += interest;
                        } else {
                            collect.insert(person_internal_id, interest);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut person_list = vec![];
                    for (person_internal_id, interests) in map {
                        person_list.push((person_internal_id, interests));
                    }
                    Ok(person_list.into_iter())
                })?
                .sort_limit_by(10, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .map(|(person_internal_id, interests)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let person_id = person_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let person_first_name = person_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let person_last_name = person_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let person_gender = person_vertex
                        .get_property("gender")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let city_id = super::graph::GRAPH
                        .get_out_vertices(person_internal_id as DefaultId, Some(&vec![11]))
                        .next()
                        .unwrap()
                        .get_id();
                    let city_vertex = super::graph::GRAPH.get_vertex(city_id).unwrap();
                    let city_name = city_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((
                        person_id,
                        person_first_name,
                        person_last_name,
                        interests,
                        person_gender,
                        city_name,
                    ))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic10 job failure")
}
