use graph_store::prelude::*;
use pegasus::api::{Binary, Branch, IterCondition, Iteration, Map, Sink, Sort, SortBy, Unary};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

use std::cmp::Ordering;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is3(conf: JobConf, person_id: u64) -> ResultStream<(u64, String, String, u64)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .flat_map(move |source| {
                    let vertex_id = source[0] as DefaultId;
                    let internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                    let knows_edge = super::graph::GRAPH.get_out_edges(internal_id, Some(&vec![12]));
                    let mut lists = vec![];
                    for i in knows_edge {
                        let knows_date = i
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let person_inner_id = i
                            .get_property("end_id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let pv = super::graph::GRAPH
                            .get_vertex(person_inner_id as DefaultId)
                            .unwrap();
                        let person_id = pv.get_property("id").unwrap().as_u64().unwrap();
                        lists.push((knows_date, person_id));
                    }
                    Ok(lists.into_iter())
                })?
                .sort_by(|x, y| {
                    if (x.0 > y.0 || (x.0 == y.0 && x.1 < y.1)) {
                        return Ordering::Greater;
                    } else {
                        Ordering::Less
                    }
                })?
                .map(move |source| {
                    let internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | source.1 as usize;
                    let lv = super::graph::GRAPH
                        .get_vertex(internal_id)
                        .unwrap();
                    let first_name = lv
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let second_name = lv
                        .get_property("secondName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((source.1, first_name, second_name, source.0))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
