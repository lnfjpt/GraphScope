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

pub fn ic4(
    conf: JobConf, person_id: u64, start_date: String, duration: i32,
) -> ResultStream<(String, u64)> {
    pegasus::run(conf, || {
        let duration = duration as i64 * 24 * 3600 * 1000;
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
                        .get_both_vertices(source_internal_id as DefaultId, Some(&vec![12]))
                    {
                        let friend_internal_id = friend_vertex.get_id();
                        for message_vertex in
                            super::graph::GRAPH.get_both_vertices(friend_internal_id, Some(&vec![0]))
                        {
                            if message_vertex.get_label()[0] != 3 {
                                continue;
                            }
                            let message_internal_id = message_vertex.get_id();
                            let message_property_vertex = super::graph::GRAPH
                                .get_vertex(message_internal_id)
                                .unwrap();
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
                    let mut tag_map = HashMap::<u64, (u64, u64)>::new();
                    for (message_internal_id, create_date) in message_list {
                        for tag_vertex in super::graph::GRAPH
                            .get_out_vertices(message_internal_id as DefaultId, Some(&vec![1]))
                        {
                            let tag_internal_id = tag_vertex.get_id() as u64;
                            if let Some(data) = tag_map.get_mut(&tag_internal_id) {
                                if create_date >= start_date {
                                    data.1 += 1;
                                }
                                if create_date < data.0 {
                                    data.0 = create_date;
                                }
                            } else{
                                if create_date >= start_date {
                                    tag_map.insert(tag_internal_id, (create_date, 1));
                                } else {
                                    tag_map.insert(tag_internal_id, (create_date, 0));
                                }
                            }
                        }
                    }
                    let mut result_list = vec![];
                    for (tag_internal_id, (create_date, count)) in tag_map {
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
                            result_list.push((tag_name, count));
                        }
                    }
                    result_list.sort_by(|x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)));
                    if result_list.len() > 10 {
                        result_list.resize(10, ("".to_string(), 0));
                    }
                    Ok(result_list.into_iter())
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic4 job failure")
}
