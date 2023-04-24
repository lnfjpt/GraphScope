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

pub fn vertex_traverse(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            stream
                .map(move |_source| {
                    let mut result = vec![];
                    for i in CSR.get_all_vertices(Some(&vec![1, 2, 3, 4])) {
                        let global_id = i.get_id();
                        result.push(global_id);
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let mut result = vec![];
                    let person_num = CSR.get_vertices_num(1);
                    let comment_num = CSR.get_vertices_num(2);
                    let post_num = CSR.get_vertices_num(3);
                    let forum_num = CSR.get_vertices_num(4);
                    for i in 0..person_num {
                        let global_id = CSR.get_global_id(i, 1).unwrap() as u64;
                        result.push(global_id);
                    }
                    for i in 0..comment_num {
                        let global_id = CSR.get_global_id(i, 2).unwrap() as u64;
                        result.push(global_id);
                    }
                    for i in 0..post_num {
                        let global_id = CSR.get_global_id(i, 3).unwrap() as u64;
                        result.push(global_id);
                    }
                    for i in 0..forum_num {
                        let global_id = CSR.get_global_id(i, 4).unwrap() as u64;
                        result.push(global_id);
                    }
                    Ok(0)
                })?
                .sink_into(output)
        }
    })
    .expect("submit edge_traverse job failure")
}
