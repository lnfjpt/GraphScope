use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, CorrelatedSubTask, IterCondition, Iteration, Limit, Map, Sink, Sort, SortBy, Unary,
};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

use std::cmp::Ordering;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is2(conf: JobConf, person_id: u64) -> ResultStream<(u64, String, u64, u64, u64, String, String)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_id]) } else { None };
        move |input, output| {
            let mut condition = IterCondition::new();
            condition.until(|item: &(u64, u64, u64)| Ok(((item.2 as usize) >> LABEL_SHIFT_BITS) == 3));
            input
                .input_from(start)?
                .flat_map(move |source| {
                    let vertex_id = source[0] as DefaultId;
                    let internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                    let message_vertices =
                        super::graph::GRAPH.get_in_vertices(internal_id, Some(&vec![2, 3]));
                    let mut lists = vec![];
                    for i in message_vertices {
                        let message_internal_id = i.get_id();
                        let create_date = i
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        lists.push((create_date, message_internal_id as u64, message_internal_id as u64));
                    }
                    Ok(lists.into_iter())
                })?
                .sort_by(|x, y| {
                    if x.0 > y.0 || (x.0 == y.0 && x.1 > y.1) {
                        return Ordering::Greater;
                    } else {
                        Ordering::Less
                    }
                })?
                .limit(10)?
                .iterate_until(condition, |start| {
                    start.map(|input| {
                        let mut reply_id = 0;
                        for i in super::graph::GRAPH.get_out_vertices(input.2 as DefaultId, Some(&vec![3]))
                        {
                            reply_id = i.get_id() as u64;
                            break;
                        }
                        Ok((input.0, input.1, reply_id))
                    })
                })?
                .map(move |source| {
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(source.1 as DefaultId)
                        .unwrap();
                    let message_id = message_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let create_date = source.0;
                    let post_id = (((3 as usize) << LABEL_SHIFT_BITS) ^ source.2 as usize) as u64;
                    let mut author_internal_id = 0;
                    for i in super::graph::GRAPH.get_out_vertices(source.2 as DefaultId, Some(&vec![0])) {
                        author_internal_id = i.get_id();
                        break;
                    }
                    let author_vertex = super::graph::GRAPH
                        .get_vertex(author_internal_id)
                        .unwrap();
                    let author_id = author_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let author_first_name = author_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let author_last_name = author_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    match message_vertex.get_label()[0] {
                        2 => Ok((
                            message_id,
                            message_vertex
                                .get_property("content")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned(),
                            create_date,
                            post_id,
                            author_id,
                            author_first_name,
                            author_last_name,
                        )),
                        3 => Ok((
                            message_id,
                            message_vertex
                                .get_property("imageFile")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned(),
                            create_date,
                            post_id,
                            author_id,
                            author_first_name,
                            author_last_name,
                        )),
                        _ => Ok((
                            message_id,
                            "".to_string(),
                            create_date,
                            post_id,
                            author_id,
                            author_first_name,
                            author_last_name,
                        )),
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
