use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, FoldByKey, HasAny, HasKey,
    IterCondition, Iteration, KeyBy, Map, PartitionByKey, Sink, SortBy, SortLimitBy, Unary,
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

pub fn ic5(conf: JobConf, person_id: u64, start_date: u64, duration: i32) -> ResultStream<(u64, u64)> {
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
                .iterate_emit_until(IterCondition::max_iters(2), EmitKind::After, |start| {
                    start
                        .repartition(|id| Ok(*id))
                        .flat_map(move |id| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(id as DefaultId, Some(&vec![12]))
                                .map(|x| x.get_id() as u64))
                        })
                })?
                .dedup()?
                .flat_map(|person_id| {
                    Ok(super::graph::GRAPH
                        .get_in_edges(person_id as DefaultId, Some(&vec![6]))
                        .map(move |edge| {
                            (
                                person_id,
                                edge.get_property("joinDate")
                                    .unwrap()
                                    .as_u64()
                                    .unwrap(),
                                edge.get_dst_id() as u64,
                            )
                        }))
                })?
                .filter_map(move |(person_id, join_date, forum_id)| {
                    if join_date > start_date {
                        Ok(Some((forum_id, person_id)))
                    } else {
                        Ok(None)
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic5 job failure")
}
