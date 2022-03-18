use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use graph_store::prelude::*;
use pegasus::api::{
    Binary, Branch, CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, HasAny, HasKey, IterCondition,
    Iteration, Limit, Map, PartitionByKey, Sink, SortBy, SortLimitBy, Unary,
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

pub fn ic13(conf: JobConf, start_person_id: u64, end_person_id: u64) -> ResultStream<(i32)> {
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![start_person_id])
            } else {
                input.input_from(vec![])
            }?;
            let mut condition = IterCondition::new();
            condition.until(move |item: &(u64, i32)| Ok(item.0 == end_person_id));
            stream
                .map(|source| Ok(((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64, 0)))?
                .iterate_until(condition, |start| {
                    start
                        .repartition(|(id, _)| Ok(*id))
                        .flat_map(move |(person_id, step)| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_id as DefaultId, Some(&vec![12]))
                                .map(move |vertex| (vertex.get_id() as u64, step+1))
                                .filter(move |(id, step)| {
                                    ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize != *id as usize
                                }))
                        })
                })?
                .limit(1)?
                .map(|(person_id, step)| Ok(step))?
                .sink_into(output)
        }
    })
    .expect("submit ic13 job failure");
    todo!()
}
