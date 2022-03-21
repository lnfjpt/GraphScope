use graph_store::prelude::*;
use pegasus::api::{Map, Sink, SortBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use std::cmp::Ordering;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is7(conf: JobConf, message_id: u64) -> ResultStream<(u64, String, u64, u64, String, String, bool)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![message_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .flat_map(move |source| {
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
                    let mut origin_person_id = 0;
                    for i in super::graph::GRAPH.get_out_vertices(vertex.get_id(), Some(&vec![0])) {
                        origin_person_id = (((1 as usize) << LABEL_SHIFT_BITS) ^ i.get_id()) as u64;
                        break;
                    }
                    let reply_vertex = super::graph::GRAPH.get_in_vertices(vertex.get_id(), Some(&vec![3]));
                    let mut lists = vec![];
                    for i in reply_vertex {
                        let reply_id = i.get_id();
                        let reply_vertex = super::graph::GRAPH
                            .get_vertex(reply_id)
                            .unwrap();
                        let create_date = reply_vertex
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let mut reply_person_id = 0;
                        for i in super::graph::GRAPH.get_out_vertices(reply_id, Some(&vec![0])) {
                            reply_person_id = (((1 as usize) << LABEL_SHIFT_BITS) ^ i.get_id()) as u64;
                            break;
                        }
                        lists.push((create_date, reply_person_id, origin_person_id, reply_id as u64));
                    }
                    Ok(lists.into_iter())
                })?
                .sort_by(|x, y| {
                    if x.0 > y.0 || (x.0 == y.0 && x.1 < y.1) {
                        return Ordering::Greater;
                    } else {
                        Ordering::Less
                    }
                })?
                .map(move |source| {
                    let reply_vertex = super::graph::GRAPH
                        .get_vertex(source.3 as DefaultId)
                        .unwrap();
                    let comment_id = (((2 as usize) << LABEL_SHIFT_BITS) ^ source.3 as usize) as u64;
                    let comment_content = reply_vertex
                        .get_property("content")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let person_id = (((1 as usize) << LABEL_SHIFT_BITS) ^ source.1 as usize) as u64;
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(source.1 as DefaultId)
                        .unwrap();
                    let first_name = person_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let last_name = person_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let mut knows = false;
                    for i in super::graph::GRAPH.get_out_vertices(source.1 as DefaultId, Some(&vec![12])) {
                        if i.get_id() as u64 == source.2 {
                            knows = true;
                            break;
                        }
                    }
                    Ok((
                        comment_id,
                        comment_content,
                        source.0 as u64,
                        person_id,
                        first_name,
                        last_name,
                        knows,
                    ))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
