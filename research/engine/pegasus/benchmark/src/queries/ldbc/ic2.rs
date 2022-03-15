use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, Dedup, EmitKind, Filter, HasKey, IterCondition, Iteration, Map, PartitionByKey, Sink,
    SortBy, SortLimitBy, Unary,
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

pub fn ic2(
    conf: JobConf, person_id: u64, max_date: u64,
) -> ResultStream<(u64, String, String, u64, String, u64)> {
    // Todo: parse timestamp here
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .flat_map(|source_person| {
                    Ok(super::graph::GRAPH
                        .get_both_vertices(source_person as DefaultId, Some(&vec![12]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .repartition(|id| Ok(*id))
                .flat_map(|friend_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(friend_id as DefaultId, Some(&vec![0]))
                        .map(move |vertex| (vertex.get_id() as u64, friend_id)))
                })?
                .filter_map(move |(message_internal_id, friend_id)| {
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_internal_id as DefaultId)
                        .unwrap();
                    let create_date = message_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let message_id = message_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    if create_date <= max_date {
                        Ok(Some((create_date, message_id, message_internal_id, friend_id)))
                    } else {
                        Ok(None)
                    }
                })?
                .sort_limit_by(20, |x, y| x.0.cmp(&y.0).reverse().then(x.1.cmp(&y.1)))?
                .map(|(create_date, message_id, message_internal_id, friend_internal_id)| {
                    let friend_vertex = super::graph::GRAPH
                        .get_vertex(friend_internal_id as DefaultId)
                        .unwrap();
                    let friend_id = friend_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let friend_first_name = friend_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let friend_second_name = friend_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_internal_id as DefaultId)
                        .unwrap();
                    let content = if message_vertex.get_label()[0] == 2 {
                        message_vertex
                            .get_property("content")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned()
                    } else {
                        message_vertex
                            .get_property("imageFile")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned()
                    };
                    Ok((friend_id, friend_first_name, friend_second_name, message_id, content, create_date))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic2 job failure")
}
