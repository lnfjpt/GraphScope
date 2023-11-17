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

pub fn vertex_count(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;
    let schema = &super::graph::CSR.graph_schema;

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            let worker_id = input.get_worker_index();
            stream
                .map(move |_source| {
                    let mut count = 0;
                    if worker_id % workers == 0 {
                        for vertex_label in 0..super::graph::CSR.vertex_label_num {
                            count += super::graph::CSR.get_vertices_num(vertex_label as u8);
                        }
                    }
                    Ok(count as u64)
                })?
                .fold(0, || {
                    |mut total_count, count| {
                        total_count += count;
                        Ok(total_count)
                    }
                })?
                .into_stream()?
                .sink_into(output)
        }
    })
    .expect("submit edge_traverse job failure")
}
