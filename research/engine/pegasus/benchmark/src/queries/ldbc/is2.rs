use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::api::{Binary, Branch, IterCondition, Iteration, Limit, Map, Sink, Sort, SortBy, Unary};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

use std::cmp::Ordering;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is2(conf: JobConf, person_id: u64) -> ResultStream<(u64, String, String, u64)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .flat_map(move |source| {
                    let vertex_id = source[0] as DefaultId;
                    let internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                    let message_vertices = super::graph::GRAPH.get_in_vertices(internal_id, Some(&vec![2, 3]));
                    let mut lists = vec![];
                    for i in message_vertices {
                        let message_internal_id = i.get_id();
                        let message_id = i.get_property("id").unwrap().as_u64().unwrap();
                        let create_date = i
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        lists.push((create_date, message_id, message_internal_id as u64));
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
                .map(move |source| {

                    let mv = super::graph::GRAPH
                        .get_vertex(source.2 as DefaultId)
                        .unwrap();
                    match mv.get_label()[0] {
                        _ => Ok((source.1, "1".to_string(), "1".to_string(), source.0)),
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
