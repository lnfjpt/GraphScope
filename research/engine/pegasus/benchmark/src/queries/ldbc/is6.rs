use graph_store::prelude::*;
use pegasus::api::{Binary, Branch, IterCondition, Iteration, Map, Sink, Sort, SortBy, Unary};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::{configure_with_default, JobConf};

use std::cmp::Ordering;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is6(conf: JobConf, person_id: u64) -> ResultStream<(u64, String, u64, String, String)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_id]) } else { None };
        move |input, output| {
            let stream = input.input_from(start)?.map(move |source| {
                let vertex_id = source[0] as DefaultId;
                let post_id = ((3 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                let comment_id = ((2 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                let vertex_option = if let Some(vertex) = super::graph::GRAPH.get_vertex(post_id) {
                    Some(vertex)
                } else if let Some(vertex) = super::graph::GRAPH.get_vertex(comment_id) {
                    Some(vertex)
                } else {
                    None
                };
                let vertex = vertex_option.unwrap();
                Ok(vertex.get_id() as u64)
            })?;
            let mut condition = IterCondition::new();
            condition.until(|item: &(u64)| Ok(((*item as usize) >> LABEL_SHIFT_BITS) == 3));
            condition.until(|item: &(u64)| Ok(((*item as usize) >> LABEL_SHIFT_BITS) == 3));
            stream
                .iterate_until(condition, |start| {
                    start.map(|message_id| {
                        let mut reply_id = 0;
                        for i in
                            super::graph::GRAPH.get_out_vertices(message_id as DefaultId, Some(&vec![3]))
                        {
                            reply_id = i.get_id() as u64;
                            break;
                        }
                        Ok(reply_id)
                    })
                })?
                .map(|message_id| {
                    let mut forum_internal_id = 0;
                    for i in super::graph::GRAPH.get_in_vertices(message_id as DefaultId, Some(&vec![5])) {
                        forum_internal_id = i.get_id();
                        break;
                    }
                    let forum_vertex = super::graph::GRAPH
                        .get_vertex(forum_internal_id)
                        .unwrap();
                    let forum_id = forum_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let forum_title = forum_vertex
                        .get_property("title")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let mut person_internal_id = 0;
                    for i in super::graph::GRAPH.get_out_vertices(forum_internal_id, Some(&vec![7])) {
                        person_internal_id = i.get_id();
                        break;
                    }
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id)
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
                    Ok((forum_id, forum_title, person_id, person_first_name, person_last_name))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
