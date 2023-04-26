use std::collections::HashMap;
use std::ffi::CStr;

use graph_proxy::adapters::csr_store::read_graph::to_runtime_vertex;
use graph_proxy::apis::GraphElement;
use ir_common::NameOrId;
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
use rand::Rng;
use runtime::process::entry::Entry;
use runtime::process::record::Record;

use crate::queries::graph::*;

pub fn property_traverse(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;
    let person_firstname_col = &CSR.vertex_prop_table[1_usize]
        .get_column_by_name("firstName")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;

    let person_lastname_col = &CSR.vertex_prop_table[1_usize]
        .get_column_by_name("lastName")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0])?;
            stream
                .map(move |_source| {
                    let person_vertices = CSR.get_all_vertices(Some(&vec![1]));
                    let mut result = vec![];
                    for i in person_vertices {
                        let first_name = i
                            .get_property("firstName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let last_name = i
                            .get_property("lastName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        result.push((first_name, last_name));
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let person_vertices = CSR.get_all_vertices(Some(&vec![1]));
                    let mut result = vec![];
                    for i in person_vertices {
                        let vertex = to_runtime_vertex(
                            i,
                            Some(vec![
                                NameOrId::Str("firstName".to_string()),
                                NameOrId::Str("lastName".to_string()),
                            ]),
                        );
                        let first_name = vertex
                            .get_property(&NameOrId::Str("firstName".to_string()))
                            .unwrap()
                            .try_to_owned()
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let last_name = vertex
                            .get_property(&NameOrId::Str("lastName".to_string()))
                            .unwrap()
                            .try_to_owned()
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        result.push((first_name, last_name));
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let person_vertices = CSR.get_all_vertices(Some(&vec![1]));
                    let mut result = vec![];
                    for i in person_vertices {
                        let vertex = to_runtime_vertex(
                            i,
                            Some(vec![
                                NameOrId::Str("firstName".to_string()),
                                NameOrId::Str("lastName".to_string()),
                            ]),
                        );
                        let record = Record::new(vertex, None);
                        let first_name = record
                            .get(None)
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .get_property(&NameOrId::Str("firstName".to_string()))
                            .unwrap()
                            .try_to_owned()
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let last_name = record
                            .get(None)
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .get_property(&NameOrId::Str("lastName".to_string()))
                            .unwrap()
                            .try_to_owned()
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        result.push((first_name, last_name));
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let person_vertices = CSR.get_all_vertices(Some(&vec![1]));
                    let person_num = person_vertices.count();
                    let mut result = vec![];
                    for i in 0..person_num {
                        let first_name = person_firstname_col[i].clone();
                        let last_name = person_lastname_col[i].clone();
                        result.push((first_name, last_name));
                    }
                    Ok(0)
                })?
                .map(move |_source| {
                    let person_vertices = CSR.get_all_vertices(Some(&vec![1]));
                    let person_num = person_vertices.count();
                    let mut result = vec![];
                    for _ in 0..person_num {
                        let random_index = rand::thread_rng().gen_range(0..person_num);
                        let first_name = person_firstname_col[random_index].clone();
                        let last_name = person_lastname_col[random_index].clone();
                        result.push((first_name, last_name));
                    }
                    Ok(0)
                })?
                .sink_into(output)
        }
    })
    .expect("submit edge_traverse job failure")
}
