use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, HasAny, HasKey, IterCondition,
    Iteration, Limit, Map, PartitionByKey, Sink, SortBy, SortLimitBy, Unary,
};
use pegasus::resource::{DefaultParResource, PartitionedResource};
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic14(conf: JobConf, start_person_id: u64, end_person_id: u64) -> ResultStream<(Vec<u64>, f64)> {
    let mut id_map_list: Vec<HashMap<u64, i32>> = Vec::with_capacity(conf.workers as usize);
    for _ in 0..conf.workers {
        id_map_list.push(HashMap::<u64, i32>::new());
    }
    let resource = DefaultParResource::new(&conf, id_map_list).unwrap();
    pegasus::run_with_resources(conf, resource, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![start_person_id])
            } else {
                input.input_from(vec![])
            }?;
            let source_person_id = (((1 as usize) << LABEL_SHIFT_BITS) | start_person_id as usize) as u64;
            let end_person_id = (((1 as usize) << LABEL_SHIFT_BITS) | end_person_id as usize) as u64;
            let mut condition = IterCondition::new();
            condition.until(move |item: &(u64, i32)| Ok(item.0 == end_person_id));
            let mut condition2 = IterCondition::new();
            condition2
                .until(move |item: &(Vec<u64>, i32, f64)| Ok(*item.0.last().unwrap() == source_person_id));
            stream
                .map(|source| Ok(((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64, 0)))?
                .iterate_until(condition, |start| {
                    start
                        .repartition(|(id, _)| Ok(*id))
                        .filter_map(|(person_id, step)| {
                            let mut id_map =
                                pegasus::resource::get_resource_mut::<HashMap<u64, i32>>().unwrap();
                            if id_map.contains_key(&person_id) {
                                Ok(None)
                            } else {
                                id_map.insert(person_id, step);
                                Ok(Some((person_id, step)))
                            }
                        })?
                        .flat_map(move |(person_id, step)| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_id as DefaultId, Some(&vec![12]))
                                .map(move |vertex| (vertex.get_id() as u64, step + 1))
                                .filter(move |(id, _)| {
                                    ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize != *id as usize
                                }))
                        })
                })?
                .limit(1)?
                .repartition(|(person_id, _)| Ok(*person_id))
                .map(|(person_id, step)| {
                    let mut id_map = pegasus::resource::get_resource_mut::<HashMap<u64, i32>>().unwrap();
                    id_map.insert(person_id, step);
                    Ok((vec![person_id], step, 0 as f64))
                })?
                .iterate_until(condition2, |start| {
                    start
                        .repartition(|(path, _, _)| Ok(*path.last().unwrap()))
                        .filter_map(|(path, step, weight)| {
                            let mut id_map =
                                pegasus::resource::get_resource_mut::<HashMap<u64, i32>>().unwrap();
                            if let Some(step_need) = id_map.get(path.last().unwrap()) {
                                if *step_need == step {
                                    Ok(Some((path, step, weight)))
                                } else {
                                    Ok(None)
                                }
                            } else {
                                Ok(None)
                            }
                        })?
                        .map(|(path, step, weight)| {
                            if path.len() > 1 {
                                let first_person_id = path[path.len() - 1];
                                let second_person_id = path[path.len() - 2];
                                let mut weight_between = 0 as f64;
                                for i in super::graph::GRAPH
                                    .get_in_vertices(first_person_id as DefaultId, Some(&vec![0]))
                                {
                                    if i.get_label()[0] == 3 {
                                        continue;
                                    }
                                    let reply_vertex = super::graph::GRAPH
                                        .get_out_vertices(i.get_id(), Some(&vec![3]))
                                        .next()
                                        .unwrap();
                                    let reply_person = super::graph::GRAPH
                                        .get_out_vertices(reply_vertex.get_id(), Some(&vec![0]))
                                        .next()
                                        .unwrap();
                                    if reply_person.get_id() as u64 != second_person_id {
                                        continue;
                                    } else {
                                        if reply_vertex.get_label()[0] == 3 {
                                            weight_between += 1.0;
                                        } else if reply_vertex.get_label()[0] == 2 {
                                            weight_between += 0.5;
                                        }
                                    }
                                }
                                Ok((path, step, weight + weight_between))
                            } else {
                                Ok((path, step, weight))
                            }
                        })?
                        .flat_map(|(path, step, weight)| {
                            let mut path_list = vec![];
                            for i in super::graph::GRAPH
                                .get_both_vertices(*path.last().unwrap() as DefaultId, Some(&vec![12]))
                            {
                                let mut path_clone = path.clone();
                                path_clone.push(i.get_id() as u64);
                                path_list.push((path_clone, step - 1, weight));
                            }
                            Ok(path_list.into_iter())
                        })
                })?
                .sort_by(|x, y| x.2.partial_cmp(&y.2).unwrap().reverse())?
                .map(|(mut path, _, weight)| {
                    let mut path_result = vec![];
                    path.reverse();
                    for i in path {
                        let vertex = super::graph::GRAPH
                            .get_vertex(i as DefaultId)
                            .unwrap();
                        path_result.push(
                            vertex
                                .get_property("id")
                                .unwrap()
                                .as_u64()
                                .unwrap(),
                        );
                    }
                    Ok((path_result, weight))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic14 job failure")
}
