use std::collections::HashMap;
use std::convert::TryInto;

use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
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

pub fn ic6_groot(conf: JobConf, person_id: u64, tag_name: String) -> ResultStream<(String, i32)> {
    let person_vertices = super::groot_graph::GRAPH.get_all_vertices(
        MAX_SNAPSHOT_ID,
        &vec![4],
        None,
        None,
        None,
        usize::max_value(),
        &vec![0],
    );
    let mut person_inner_id = 0 as i64;
    for i in person_vertices {
        let inner_id = i.get_property(3).unwrap().get_long().unwrap();
        if inner_id == person_id {
            person_inner_id = inner_id;
            break;
        }
    }
    pegasus::run(conf, || {
        let tag_name = tag_name.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_inner_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .iterate_emit_until(IterCondition::max_iters(2), EmitKind::After, |start| {
                    start
                        .repartition(|id| Ok((*id).try_into().unwrap()))
                        .flat_map(move |person_id| {
                            Ok(super::groot_graph::GRAPH
                                .get_out_vertex_ids(
                                    MAX_SNAPSHOT_ID,
                                    vec![(0, vec![source_person])],
                                    &vec![22],
                                    None,
                                    None,
                                    usize::max_value(),
                                )
                                .next()
                                .unwrap()
                                .1
                                .map(|vertex| vertex.get_id())
                                .chain(
                                    super::groot_graph::GRAPH
                                        .get_in_vertex_ids(
                                            MAX_SNAPSHOT_ID,
                                            vec![(0, vec![person_id])],
                                            &vec![22],
                                            None,
                                            None,
                                            usize::max_value(),
                                        )
                                        .next()
                                        .unwrap()
                                        .1
                                        .map(|vertex| vertex.get_id()),
                                ))
                        })
                })?
                .dedup()?
                .repartition(|id| Ok((*id).try_into().unwrap()))
                .flat_map(|person_internal_id| {
                    Ok(super::groot_graph::GRAPH
                        .get_in_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![person_internal_id])],
                            &vec![9],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .map(|vertex| vertex.get_id()))
                })?
                .filter_map(|message_internal_id| {
                    let message_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(None, vec![message_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    if message_vertex.get_label_id() == 3 {
                        Ok(Some(message_internal_id))
                    } else {
                        Ok(None)
                    }
                })?
                .apply(|sub| {
                    let tag_name = tag_name.clone();
                    sub.flat_map(|post_internal_id| {
                        Ok(super::groot_graph::GRAPH
                            .get_out_vertex_ids(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![post_internal_id])],
                                &vec![11],
                                None,
                                None,
                                usize::max_value(),
                            )
                            .next()
                            .unwrap()
                            .1
                            .map(|vertex| vertex.get_id()))
                    })?
                    .filter_map(move |tag_internal_id| {
                        let vertex = super::groot_graph::GRAPH
                            .get_vertex_properties(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![(Some(7), vec![tag_internal_id])])],
                                None,
                            )
                            .next()
                            .unwrap();
                        let post_tag_name = vertex
                            .get_property(4)
                            .unwrap()
                            .get_string()
                            .unwrap()
                            .clone();
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
                    Ok(super::groot_graph::GRAPH
                        .get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![post_internal_id])],
                            &vec![11],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .map(|vertex| vertex.get_id()))
                })?
                .filter_map(move |tag_internal_id| {
                    let tag_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(7), vec![tag_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let post_tag_name = tag_vertex
                        .get_property(4)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
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
