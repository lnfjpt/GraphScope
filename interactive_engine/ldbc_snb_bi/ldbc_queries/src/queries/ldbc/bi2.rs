use std::collections::HashMap;

use graph_store::common::LabelId;
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::{
    columns::{DateTimeColumn, StringColumn},
    date,
};
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

fn get_tag_list(tagclass: String) -> Vec<u64> {
    let tagclass_num = crate::queries::graph::CSR.get_vertices_num(6 as LabelId);
    let tagclass_name_col = &crate::queries::graph::CSR.vertex_prop_table[6_usize]
        .get_column_by_name("name")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;
    let mut tagclass_id = usize::MAX;
    for v in 0..tagclass_num {
        if tagclass_name_col[v] == tagclass {
            tagclass_id = v;
            break;
        }
    }
    if tagclass_id == usize::MAX {
        return vec![];
    }
    // info!("target tagclass id is {}", tagclass_id);

    let mut tag_list = vec![];
    if let Some(edges) = crate::queries::graph::TAG_HASTYPE_TAGCLASS_IN.get_adj_list(tagclass_id) {
        for e in edges {
            tag_list.push(
                crate::queries::graph::CSR
                    .get_global_id(e.neighbor, 7)
                    .unwrap() as u64,
            );
        }
    }

    tag_list
}

pub fn bi2(conf: JobConf, date: String, tag_class: String) -> ResultStream<(String, i32, i32, i32)> {
    let workers = conf.workers;
    let start_date = date::parse_date(&date).unwrap();
    let start_date_ts = start_date.to_i32();
    let first_window = start_date.add_days(100);
    let first_window_ts = first_window.to_i32();
    let second_window = first_window.add_days(100);
    let second_window_ts = second_window.to_i32();

    let comment_createdate_col = &crate::queries::graph::CSR.vertex_prop_table[2_usize]
        .get_column_by_name("creationDate")
        .as_any()
        .downcast_ref::<DateTimeColumn>()
        .unwrap()
        .data;
    let post_createdate_col = &crate::queries::graph::CSR.vertex_prop_table[3_usize]
        .get_column_by_name("creationDate")
        .as_any()
        .downcast_ref::<DateTimeColumn>()
        .unwrap()
        .data;
    let tag_name_col = &crate::queries::graph::CSR.vertex_prop_table[7_usize]
        .get_column_by_name("name")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;

    pegasus::run(conf, || {
        let tag_class = tag_class.clone();
        let tag_list = get_tag_list(tag_class);

        move |input, output| {
            let servers = pegasus::get_servers_len();
            let stream = if input.get_worker_index() % workers == 0 {
                input.input_from(vec![0_i32])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(move |_| Ok(tag_list.clone().into_iter()))?
                .repartition(move |id| Ok(crate::queries::graph::get_partition(id, workers as usize, servers)))
                .flat_map(move |tag_global_id| {
                    let mut result = vec![];
                    result.push((0, tag_global_id));
                    let tag_id = crate::queries::graph::CSR.get_internal_id(tag_global_id as usize);
                    if let Some(edges) = crate::queries::graph::COMMENT_HASTAG_TAG_IN.get_adj_list(tag_id) {
                        for e in edges {
                            result.push((
                                crate::queries::graph::CSR
                                    .get_global_id(e.neighbor, 2)
                                    .unwrap() as u64,
                                tag_global_id,
                            ));
                        }
                    }
                    if let Some(edges) = crate::queries::graph::POST_HASTAG_TAG_IN.get_adj_list(tag_id) {
                        for e in edges {
                            result.push((
                                crate::queries::graph::CSR
                                    .get_global_id(e.neighbor, 3)
                                    .unwrap() as u64,
                                tag_global_id,
                            ));
                        }
                    }

                    Ok(result.into_iter())
                })?
                .repartition(move |(id, _)| Ok(crate::queries::graph::get_partition(id, workers as usize, servers)))
                .filter_map(move |(message_global_id, tag_global_id)| {
                    if message_global_id == 0 {
                        return Ok(Some((tag_global_id, 0)));
                    }
                    let message_id = crate::queries::graph::CSR.get_internal_id(message_global_id as usize);
                    let message_label = LDBCVertexParser::<usize>::get_label_id(message_global_id as usize);
                    if message_label == 2 {
                        let ts = comment_createdate_col[message_id].date_to_i32();
                        if ts >= start_date_ts && ts < first_window_ts {
                            Ok(Some((tag_global_id, 1)))
                        } else if ts >= first_window_ts && ts < second_window_ts {
                            Ok(Some((tag_global_id, 2)))
                        } else {
                            Ok(None)
                        }
                    } else {
                        let ts = post_createdate_col[message_id].date_to_i32();
                        if ts >= start_date_ts && ts < first_window_ts {
                            Ok(Some((tag_global_id, 1)))
                        } else if ts >= first_window_ts && ts < second_window_ts {
                            Ok(Some((tag_global_id, 2)))
                        } else {
                            Ok(None)
                        }
                    }
                })?
                .repartition(move |(id, _)| Ok(crate::queries::graph::get_partition(id, workers as usize, servers)))
                .fold_partition(HashMap::<u64, (i32, i32)>::new(), move || {
                    move |mut collect, (tag_global_id, windows)| {
                        if let Some(count) = collect.get_mut(&tag_global_id) {
                            if windows == 1 {
                                count.0 += 1;
                            } else if windows == 2 {
                                count.1 += 1;
                            }
                        } else {
                            let data = if windows == 0 {
                                (0, 0)
                            } else if windows == 1 {
                                (1, 0)
                            } else {
                                (0, 1)
                            };
                            collect.insert(tag_global_id, data);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(move |map| {
                    let mut result = vec![];
                    for (tag_global_id, (count1, count2)) in map.iter() {
                        let tag_id = crate::queries::graph::CSR.get_internal_id(*tag_global_id as usize);
                        result.push((
                            tag_name_col[tag_id].clone(),
                            *count1,
                            *count2,
                            i32::abs(*count1 - *count2),
                        ));
                    }
                    Ok(result.into_iter())
                })?
                .sort_limit_by(100_u32, |x, y| y.3.cmp(&x.3).then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
        .expect("submit bi2_sub job failed")
}
