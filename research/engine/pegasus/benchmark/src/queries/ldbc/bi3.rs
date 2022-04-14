use std::collections::HashMap;

use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;
use std::cmp::max;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn bi3(conf: JobConf, year: i32, month: i32) -> ResultStream<(String, i32, i32, i32)> {
    let start_date = year * 100 + month;
    let end_date = if month == 12 { (year + 1) * 100 + 1 } else { year * 100 + month + 1 };
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![(start_date, end_date)])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(|(start_date, end_date)| {
                    Ok(super::graph::GRAPH
                        .get_all_vertices(Some(&vec![2, 3]))
                        .filter_map(move |vertex| {
                            let create_date = vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            let date = (create_date / 1000000000 % 1000000) as i32;
                            if date == start_date {
                                Some((vertex.get_id() as u64, 0))
                            } else if date == end_date {
                                Some((vertex.get_id() as u64, 0))
                            } else {
                                None
                            }
                        }))
                })?
                .repartition(|(id, date)| Ok(*id))
                .map(|(message_internal_id, date)| {
                    let tag_internal_id = super::graph::GRAPH
                        .get_out_vertices(message_internal_id as DefaultId, Some(&vec![1]))
                        .next()
                        .unwrap()
                        .get_id() as u64;
                    Ok((tag_internal_id, date))
                })?
                .fold(HashMap::<u64, (i32, i32)>::new(), || {
                    |mut collect, (tag_internal_id, date)| {
                        if let Some(data) = collect.get_mut(&tag_internal_id) {
                            if date == 0 {
                                data.0 += 1;
                            } else {
                                data.1 += 1;
                            }
                        } else {
                            if date == 0 {
                                collect.insert(tag_internal_id, (1, 0));
                            } else {
                                collect.insert(tag_internal_id, (0, 1));
                            }
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut group_list = vec![];
                    for (tag_internal_id, count) in map {
                        if count.0 > 0 {
                            group_list.push((tag_internal_id, count.0, count.1));
                        }
                    }
                    Ok(group_list.into_iter())
                })?
                .map(|(tag_internal_id, count0, count1)| {
                    let tag_vertex = super::graph::GRAPH
                        .get_vertex(tag_internal_id as DefaultId)
                        .unwrap();
                    let tag_name = tag_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((tag_name, count0, count1, (count0 - count1).abs()))
                })?
                .sort_limit_by(100, |x, y| x.3.cmp(&y.3).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi3 job failure")
}
