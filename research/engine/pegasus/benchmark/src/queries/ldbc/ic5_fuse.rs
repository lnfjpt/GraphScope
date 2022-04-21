use std::collections::HashMap;
use std::vec::Vec;

use graph_store::prelude::*;
use pegasus::api::{Dedup, EmitKind, Fold, IterCondition, Iteration, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic5(conf: JobConf, person_id: u64, start_date: String) -> ResultStream<(u64, u64)> {
    let start_date = super::graph::parse_datetime(&start_date).unwrap();
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(move |source| {
                    let source_internal_id = (((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64;
                    let mut friend_list = Vec::<u64>::new();
                    let mut current_list = vec![];
                    let mut temp_vec = vec![];
                    current_list.push(source_internal_id);
                    for _ in 0..2 {
                        for person_internal_id in current_list {
                            for vertex in super::graph::GRAPH
                                .get_both_vertices(person_internal_id as DefaultId, Some(&vec![12]))
                            {
                                if vertex.get_id() as u64 != source_internal_id {
                                    temp_vec.push(vertex.get_id() as u64);
                                }
                            }
                        }
                        current_list = temp_vec.clone();
                        friend_list.append(&mut temp_vec);
                    }
                    friend_list.sort();
                    friend_list.dedup();
                    let mut forum_list = vec![];
                    for person_id in friend_list {
                        for edge in super::graph::GRAPH.get_in_edges(person_id as DefaultId, Some(&vec![6]))
                        {
                            let join_date = edge
                                .get_property("joinDate")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            if join_date >= start_date {
                                continue;
                            }
                            forum_list.push((edge.get_src_id() as u64, person_id));
                        }
                    }
                    let mut forum_map = HashMap::<u64, Vec<u64>>::new();
                    for (forum_id, person_id) in forum_list {
                        if let Some(person_list) = forum_map.get_mut(&forum_id) {
                            person_list.push(person_id);
                        } else {
                            forum_map.insert(forum_id, vec![person_id]);
                        }
                    }
                    let mut result_list = vec![];
                    for (forum_internal_id, person_list) in forum_map {
                        let mut count = 0;
                        for post_id in super::graph::GRAPH
                            .get_out_vertices(forum_internal_id as DefaultId, Some(&vec![5]))
                            .map(|vertex| vertex.get_id())
                        {
                            let person_id = super::graph::GRAPH
                                .get_out_vertices(post_id, Some(&vec![0]))
                                .next()
                                .unwrap()
                                .get_id() as u64;
                            if person_list.contains(&person_id) {
                                count += 1;
                            }
                        }
                        let forum_vertex = super::graph::GRAPH
                            .get_vertex(forum_internal_id as DefaultId)
                            .unwrap();
                        let forum_id = forum_vertex
                            .get_property("id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        result_list.push((forum_id, count));
                    }
                    result_list.sort_by(|x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)));
                    if result_list.len() > 20 {
                        result_list.resize(20, (0, 0));
                    }
                    Ok(result_list.into_iter())
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic5 job failure")
}
