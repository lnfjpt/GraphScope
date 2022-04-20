use graph_store::prelude::*;
use pegasus::api::{Filter, Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;
use std::collections::HashMap;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic4(conf: JobConf, person_id: u64, start_date: String, duration: i32) -> ResultStream<(String, u64)> {
    pegasus::run(conf, || {
        let duration = duration as i64 * 24 * 3600 * 100 * 1000;
        let end_date = start_date.parse::<i64>().unwrap() + duration;
        let start_date = super::graph::parse_datetime(&start_date).unwrap();
        let end_date = super::graph::parse_datetime(&end_date.to_string()).unwrap();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(move |source| {
                    let source_internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | source as usize;
                    let mut message_list = vec![];
                    for friend_vertex in super::graph::GRAPH
                        .get_both_vertices(source_internal_id as DefaultId, Some(&vec![12])) {
                        let friend_internal_id = friend_vertex.get_id();
                        for message_vertex in super::graph::GRAPH
                            .get_both_vertices(friend_internal_id, Some(&vec![0])) {
                            if message_vertex.get_label()[0] != 3 {
                                continue;
                            }
                            let message_internal_id = message_vertex.get_id();
                            let message_property_vertex = super::graph::GRAPH.get_vertex(message_internal_id).unwrap();
                            let create_date = message_property_vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            if create_date < end_date {
                                message_list.push((message_internal_id, create_date));
                            }
                        }
                    }
                    let mut tag_list = vec![];
                    for (message_internal_id, create_date) in message_list{
                        for tag_vertex in super::graph::GRAPH
                            .get_out_vertices(message_internal_id as DefaultId, Some(&vec![1])) {
                            tag_list.push((tag_vertex.get_id() as u64, create_date));
                        }
                    }
                    Ok(tag_list.into_iter())
                })?
                .fold(HashMap::<u64, (u64, u64)>::new(), move || {
                    move |mut collect, tag_id| {
                        if let Some(data) = collect.get_mut(&tag_id.0) {
                            if tag_id.1 >= start_date {
                                data.1 += 1;
                            }
                            if data.0 > tag_id.1 {
                                data.0 = tag_id.1;
                            }
                        } else {
                            if tag_id.1 >= start_date {
                                collect.insert(tag_id.0, (1, tag_id.1));
                            } else {
                                collect.insert(tag_id.0, (0, tag_id.1));
                            }
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut tag_list = vec![];
                    for (tag_internal_id, (count, create_date)) in map {
                        tag_list.push((tag_internal_id, count, create_date));
                    }
                    Ok(tag_list.into_iter())
                })?
                .filter_map(move |(tag_internal_id, count, create_date)| {
                    if create_date >= start_date {
                        let tag_vertex = super::graph::GRAPH
                            .get_vertex(tag_internal_id as DefaultId)
                            .unwrap();
                        let tag_name = tag_vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        Ok(Some((tag_name, count)))
                    } else {
                        Ok(None)
                    }
                })?
                .sort_limit_by(10, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit ic4 job failure")
}
