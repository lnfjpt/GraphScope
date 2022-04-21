use std::collections::HashMap;

use graph_store::prelude::*;
use pegasus::api::{
    CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, HasAny, IterCondition, Iteration, Map, Sink,
    SortLimitBy,
};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 6 :
// g.V().hasLabel('PERSON').has('id', $personId).union(both('KNOWS'), both('KNOWS').both('KNOWS')).dedup()
// .has('id', neq($personId)).in('HASCREATOR').hasLabel('POST').as('_t')
// .out('HASTAG').has('name', eq('$tagName'))
// .select('_t').dedup().out('HASTAG').has('name', neq('$tagName'))
// .groupCount().order().by(select(values), desc).by(select(keys).values('name'), asc).limit(10)

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic6(conf: JobConf, person_id: u64, tag_name: String) -> ResultStream<(String, i32)> {
    pegasus::run(conf, || {
        let tag_name = tag_name.clone();
        let tag_name2 = tag_name.clone();
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

                    let mut post_list = vec![];
                    for person_id in friend_list {
                        for post_vertex in super::graph::GRAPH
                            .get_in_vertices(person_id as DefaultId, Some(&vec![0])) {
                            if post_vertex.get_label()[0] ==3 {
                                for tag_vertex in super::graph::GRAPH
                                    .get_out_vertices(post_vertex.get_id(), Some(&vec![1])) {
                                    let tag_property_vertex = super::graph::GRAPH
                                        .get_vertex(tag_vertex.get_id()).unwrap();
                                    let post_tag_name = tag_property_vertex
                                        .get_property("name")
                                        .unwrap()
                                        .as_str()
                                        .unwrap()
                                        .into_owned();
                                    if post_tag_name == tag_name {
                                        post_list.push(post_vertex.get_id() as u64);
                                    }
                                }
                            }
                        }
                    }
                    post_list.sort();
                    post_list.dedup();
                    let mut tag_name_list = vec![];
                    for post_id in post_list {
                        for tag_vertex in super::graph::GRAPH
                            .get_out_vertices(post_id as DefaultId, Some(&vec![1])) {
                            let tag_property_vertex = super::graph::GRAPH
                                .get_vertex(tag_vertex.get_id())
                                .unwrap();
                            let post_tag_name = tag_property_vertex
                                .get_property("name")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned();
                            if tag_name2 != post_tag_name {
                                tag_name_list.push(post_tag_name);
                            }
                        }
                    }
                    Ok(tag_name_list.into_iter())
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
