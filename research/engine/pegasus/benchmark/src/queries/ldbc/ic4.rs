use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, HasAny, HasKey, IterCondition,
    Iteration, Map, PartitionByKey, Sink, SortBy, SortLimitBy, Unary,
};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic4(
    conf: JobConf, person_id: u64, start_date: u64, duration: i32,
) -> ResultStream<(u64, String, String, i32, i32, i32)> {
    // Todo: parse timestamp here
    pegasus::run(conf, || {
        let end_date = start_date + duration as u64;
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .flat_map(|source| {
                    Ok(super::graph::GRAPH
                        .get_both_vertices(source as DefaultId, Some(&vec![12]))
                        .map(move |vertex| vertex.get_id() as u64))
                })?
                .repartition(|id| Ok(*id))
                .flat_map(|person_id| {
                    Ok(super::graph::GRAPH
                        .get_both_vertices(person_id as DefaultId, Some(&vec![0]))
                        .filter(|vertex| vertex.get_label()[0] == 3)
                        .map(move |vertex| (person_id, vertex.get_id() as u64)))
                })?
                .filter_map(move |(person_id, message_id)| {
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_id as DefaultId)
                        .unwrap();
                    let create_date = message_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    if create_date >= start_date && create_date < end_date {
                        Ok(Some((person_id, message_id)))
                    } else {
                        Ok(None)
                    }
                })?;
        }
    })
    .expect("submit ic4 job failure")
}
