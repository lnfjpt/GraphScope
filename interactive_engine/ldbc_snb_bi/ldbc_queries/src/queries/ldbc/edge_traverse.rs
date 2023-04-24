use std::collections::HashMap;

use log::debug;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::*;
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::schema::Schema;
use mcsr::types::DefaultId;
use mcsr::{
    columns::{DateTimeColumn, StringColumn},
    date,
};
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;

pub fn edge_traverse(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;
    let schema = &CSR.graph_schema;
    let replyof_label = schema.get_edge_label_id("REPLYOF").unwrap();
    let hastag_label = schema.get_edge_label_id("HASTAG").unwrap();

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            stream
                .map(move |_source| {
                    let mut comment_list = vec![];
                    let post_num = 100000;
                    for i in 0..post_num {
                        let post_global_id = CSR.get_global_id(i, 3).unwrap() as u64;
                        for comment in
                            CSR.get_in_vertices(post_global_id as usize, Some(&vec![replyof_label]))
                        {
                            comment_list.push(comment.get_id());
                        }
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let mut comment_list = vec![];
                    let mut post_list = vec![];
                    let post_num = 100000;
                    for i in 0..post_num {
                        let post_global_id = CSR.get_global_id(i, 3).unwrap() as u64;
                        post_list.push(post_global_id);
                        if let Some(edges) = crate::queries::graph::COMMENT_REPLYOF_POST_IN.get_adj_list(i)
                        {
                            for edge in edges {
                                let comment_global_id = CSR.get_global_id(edge.neighbor, 2).unwrap();
                                comment_list.push(comment_global_id);
                            }
                        }
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let mut tag_list = vec![];
                    let mut result = vec![];
                    for i in 0..100 {
                        let tag_global_id = CSR.get_global_id(i as usize, 7).unwrap() as u64;
                        tag_list.push(tag_global_id);
                    }
                    for tag_global_id in tag_list {
                        for vertex in CSR.get_in_vertices(tag_global_id as usize, Some(&vec![hastag_label]))
                        {
                            result.push(vertex.get_id());
                        }
                    }
                    Ok(0)
                })?
                .map(|_source| {
                    let mut tag_list = vec![];
                    let mut result = vec![];
                    for i in 0..100 {
                        let tag_global_id = CSR.get_global_id(i as usize, 7).unwrap() as u64;
                        tag_list.push(tag_global_id);
                    }
                    for tag_global_id in tag_list {
                        let tag_id = CSR.get_internal_id(tag_global_id as usize);
                        if let Some(edges) =
                            crate::queries::graph::COMMENT_HASTAG_TAG_IN.get_adj_list(tag_id)
                        {
                            for edge in edges {
                                let comment_global_id = CSR.get_global_id(edge.neighbor, 2).unwrap() as u64;
                                result.push(comment_global_id);
                            }
                        }
                        if let Some(edges) = crate::queries::graph::POST_HASTAG_TAG_IN.get_adj_list(tag_id)
                        {
                            for edge in edges {
                                let post_global_id = CSR.get_global_id(edge.neighbor, 3).unwrap() as u64;
                                result.push(post_global_id);
                            }
                        }
                        if let Some(edges) = crate::queries::graph::FORUM_HASTAG_TAG_IN.get_adj_list(tag_id)
                        {
                            for edge in edges {
                                let forum_global_id = CSR.get_global_id(edge.neighbor, 4).unwrap() as u64;
                                result.push(forum_global_id);
                            }
                        }
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let mut tag_list = vec![];
                    let mut result = vec![];
                    for i in 0..100 {
                        let tag_global_id = CSR.get_global_id(i as usize, 7).unwrap() as u64;
                        tag_list.push(tag_global_id);
                    }
                    for tag_global_id in tag_list {
                        for vertex in CSR
                            .get_in_vertices(tag_global_id as usize, Some(&vec![hastag_label]))
                            .filter(move |v| v.get_label() != 4)
                        {
                            result.push(vertex.get_id());
                        }
                    }
                    Ok(0)
                })?
                .map(|_source| {
                    let mut tag_list = vec![];
                    let mut result = vec![];
                    for i in 0..100 {
                        let tag_global_id = CSR.get_global_id(i as usize, 7).unwrap() as u64;
                        tag_list.push(tag_global_id);
                    }
                    for tag_global_id in tag_list {
                        let tag_id = CSR.get_internal_id(tag_global_id as usize);
                        if let Some(edges) =
                            crate::queries::graph::COMMENT_HASTAG_TAG_IN.get_adj_list(tag_id)
                        {
                            for edge in edges {
                                let comment_global_id = CSR.get_global_id(edge.neighbor, 2).unwrap() as u64;
                                result.push(comment_global_id);
                            }
                        }
                        if let Some(edges) = crate::queries::graph::POST_HASTAG_TAG_IN.get_adj_list(tag_id)
                        {
                            for edge in edges {
                                let post_global_id = CSR.get_global_id(edge.neighbor, 3).unwrap() as u64;
                                result.push(post_global_id);
                            }
                        }
                    }
                    Ok(0)
                })?
                .sink_into(output)
        }
    })
    .expect("submit edge_traverse job failure")
}
