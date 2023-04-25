use std::collections::HashMap;

use graph_proxy::adapters::csr_store::read_graph::to_runtime_vertex;
use graph_proxy::apis::GraphElement;
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
use runtime::process::entry::{DynEntry, Entry};
use runtime::process::record::Record;

use crate::queries::graph::*;

pub fn handwriting_traverse(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;

    let forum_name_col = &CSR.vertex_prop_table[4_usize]
        .get_column_by_name("title")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            stream
                .flat_map(move |_source| {
                    let mut forum_list = vec![];
                    let mut person_list = vec![];
                    let person_num = CSR.get_vertices_num(1);
                    for person_index in 0..person_num {
                        let person_global_id = CSR.get_global_id(person_index, 1).unwrap() as u64;
                        person_list.push(person_global_id);
                        if let Some(edges) =
                            crate::queries::graph::FORUM_HASMODERATOR_PERSON_IN.get_adj_list(person_index)
                        {
                            for edge in edges {
                                let forum_global_id = CSR.get_global_id(edge.neighbor, 4).unwrap() as u64;
                                forum_list.push(forum_global_id);
                            }
                        }
                    }
                    Ok(forum_list.into_iter())
                })?
                .repartition(move |id| Ok(get_partition(id, workers as usize, pegasus::get_servers_len())))
                .map(move |forum_global_id| {
                    let mut name_list = vec![];
                    let forum_id = CSR.get_internal_id(forum_global_id as usize);
                    let forum_name = forum_name_col[forum_id].clone();
                    name_list.push(forum_name);
                    Ok(0)
                })?
                .fold(0_u64, || |collect, _| Ok(collect))?
                .into_stream()?
                .broadcast()
                .sink_into(output)
        }
    })
    .expect("submit edge_traverse job failure")
}
