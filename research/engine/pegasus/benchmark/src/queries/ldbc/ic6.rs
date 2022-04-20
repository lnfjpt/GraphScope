use std::collections::HashMap;

use graph_store::prelude::*;
use pegasus::api::{
    CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, HasAny, IterCondition, Iteration, Map, Sink,
    SortLimitBy,
};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic6(conf: JobConf, person_id: u64, tag_name: String) -> ResultStream<(String, i32)> {
    pegasus::run(conf, || {
        let tag_name = tag_name.clone();
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
                .flat_map(|person_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(person_internal_id as DefaultId, Some(&vec![0]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .filter_map(|message_internal_id| {
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_internal_id as DefaultId)
                        .unwrap();
                    if message_vertex.get_label()[0] == 3 {
                        Ok(Some(message_internal_id))
                    } else {
                        Ok(None)
                    }
                })?
                .apply(|sub| {
                    let tag_name = tag_name.clone();
                    sub.flat_map(|post_internal_id| {
                        Ok(super::graph::GRAPH
                            .get_out_vertices(post_internal_id as DefaultId, Some(&vec![1]))
                            .map(|vertex| vertex.get_id() as u64))
                    })?
                    .filter_map(move |tag_internal_id| {
                        let vertex = super::graph::GRAPH
                            .get_vertex(tag_internal_id as DefaultId)
                            .unwrap();
                        let post_tag_name = vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if post_tag_name == tag_name {
                            Ok(Some(tag_internal_id))
                        } else {
                            Ok(None)
                        }
                    })?
                    .any()
                })?
                .filter_map(
                    |(post_internal_id, has_tag)| {
                        if has_tag {
                            Ok(Some(post_internal_id))
                        } else {
                            Ok(None)
                        }
                    },
                )?
                .flat_map(|post_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_out_vertices(post_internal_id as DefaultId, Some(&vec![1]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .filter_map(move |tag_internal_id| {
                    let tag_vertex = super::graph::GRAPH
                        .get_vertex(tag_internal_id as DefaultId)
                        .unwrap();
                    let post_tag_name = tag_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if tag_name != post_tag_name {
                        Ok(Some(post_tag_name))
                    } else {
                        Ok(None)
                    }
                })?
                .fold(HashMap::<String, i32>::new(), || {
                    |mut collect, tag_name| {
                        if let Some(data) = collect.get_mut(&tag_name) {
                            *data += 1;
                        } else {
                            collect.insert(tag_name, 1);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut tag_list = vec![];
                    for (tag_name, count) in map {
                        tag_list.push((tag_name, count));
                    }
                    Ok(tag_list.into_iter())
                })?
                .sort_limit_by(10, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit ic6 job failure")
}
