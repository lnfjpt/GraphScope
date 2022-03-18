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

pub fn ic12(
    conf: JobConf, person_id: u64, input_tag_name: String,
) -> ResultStream<(u64,Vec<u64>)> {
    pegasus::run(conf, || {
        let input_tag_name = input_tag_name.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .flat_map(|person_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_both_vertices(person_internal_id as DefaultId, Some(&vec![12]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .repartition(|id| Ok(*id))
                .flat_map(|person_id| {
                    Ok(super::graph::GRAPH
                        .get_both_vertices(person_id as DefaultId, Some(&vec![0]))
                        .filter(|vertex| vertex.get_label()[0] == 2)
                        .map(move |vertex| (person_id, vertex.get_id() as u64)))
                })?
                .apply(|sub| {
                    let input_tag_name = input_tag_name;
                    sub.filter_map(|(person_internal_id, comment_id)| {
                        let reply_message = super::graph::GRAPH
                            .get_out_vertices(comment_id as DefaultId, Some(&vec![3]))
                            .next()
                            .unwrap();
                        if reply_message.get_label()[0] == 3 {
                            Ok(Some(reply_message.get_id() as u64))
                        } else {
                            Ok(None)
                        }
                    })?
                    .flat_map(|post_internal_id| {
                        Ok(super::graph::GRAPH
                            .get_out_vertices(post_internal_id as DefaultId, Some(&vec![1]))
                            .map(|vertex| vertex.get_id() as u64))
                    })?
                    .filter_map(move |tag_internal_id| {
                        let tag_class_id = super::graph::GRAPH
                            .get_in_vertices(tag_internal_id as DefaultId, Some(&vec![22]))
                            .next()
                            .unwrap()
                            .get_id();
                        let tag_class = super::graph::GRAPH
                            .get_vertex(tag_class_id)
                            .unwrap();
                        let tag_class_name = tag_class
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if tag_class_name == input_tag_name {
                            Ok(Some(tag_internal_id))
                        } else {
                            Ok(None)
                        }
                    })?
                    .fold(vec![], || {
                        |mut collect, tag_id| {
                            collect.push(tag_id);
                            Ok(collect)
                        }
                    })
                })?
                .filter_map(|((person_internal_id, comment_internal_id), tag_list)| {
                    if tag_list.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some((person_internal_id, tag_list)))
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic12 job failure");
    todo!()
}
