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

pub fn gie_traverse(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;
    let schema = &CSR.graph_schema;
    let hasmoderator_label = schema
        .get_edge_label_id("HASMODERATOR")
        .unwrap();

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            stream
                .flat_map(move |_source| {
                    let mut forum = vec![];
                    let person_vertices = CSR.get_all_vertices(Some(&vec![1]));
                    for person in person_vertices {
                        let gie_person = to_runtime_vertex(person, None);
                        let person_global_id = gie_person.id();
                        for forum_vertex in
                            CSR.get_in_vertices(person_global_id as usize, Some(&vec![hasmoderator_label]))
                        {
                            forum.push(to_runtime_vertex(forum_vertex, None));
                        }
                    }
                    Ok(forum.into_iter())
                })?
                .repartition(move |vertex| {
                    Ok(get_partition(&(vertex.id() as u64), workers as usize, pegasus::get_servers_len()))
                })
                .map(|forum| {
                    let mut name_list = vec![];
                    let forum_vertex = CSR.get_vertex(forum.id() as usize).unwrap();
                    let forum_name = forum_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    name_list.push(forum_name);
                    Ok(0)
                })?
                .sink_into(output)
        }
    })
    .expect("submit edge_traverse job failure")
}
