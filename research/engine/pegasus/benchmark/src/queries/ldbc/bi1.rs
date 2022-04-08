use std::collections::HashMap;

use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;
use std::cmp::max;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn bi1(conf: JobConf, max_date: String) -> ResultStream<(i32, bool, i32, i32, i32, i32)> {
    let max_date = super::graph::parse_datetime(&max_date).unwrap();
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![max_date])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(|max_date| {
                    Ok(super::graph::GRAPH
                        .get_all_vertices(Some(&vec![2, 3]))
                        .filter(move |vertex| {
                            vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap()
                                < max_date
                        })
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .repartition(|id| Ok(*id))
                .map(|message_internal_id| {
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_internal_id as DefaultId)
                        .unwrap();
                    let content_length = message_vertex
                        .get_property("length")
                        .unwrap()
                        .as_i32()
                        .unwrap();
                    let year = (message_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap()
                        / 100000000000) as i32;
                    if message_vertex.get_label()[0] == 2 {
                        Ok((true, content_length, year))
                    } else {
                        Ok((false, content_length, year))
                    }
                })?
                .fold(HashMap::<(i32, bool, i32), (i32, i32)>::new(), || {
                    |mut collect, (message_type, content_length, year)| {
                        let mut length_type = 0;
                        if content_length >= 0 && content_length < 40 {
                            length_type = 0;
                        } else if content_length >= 40 && content_length < 80 {
                            length_type = 1;
                        } else if content_length >= 80 && content_length < 160 {
                            length_type = 2;
                        } else if content_length >= 160 {
                            length_type = 3;
                        }
                        if let Some(data) = collect.get_mut(&(year, message_type, length_type)) {
                            data.0 += 1;
                            data.1 += content_length;
                        } else {
                            collect.insert((year, message_type, length_type), (1, content_length));
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut tag_list = vec![];
                    for ((year, message_type, length_type), count) in map {
                        tag_list.push((
                            year,
                            message_type,
                            length_type,
                            count.0,
                            count.1 / count.0,
                            count.1,
                        ));
                    }
                    Ok(tag_list.into_iter())
                })?
                .sink_into(output)
        }
    })
    .expect("submit bi1 job failure")
}
