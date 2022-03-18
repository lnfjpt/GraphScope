use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use graph_store::prelude::*;
use pegasus::api::{Binary, Branch, Dedup, EmitKind, HasKey, IterCondition, Iteration, Map, PartitionByKey, Sink, Unary, SortBy};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

// interactive complex query 1 :
// g.V().hasLabel('person').has('person_id', $id)
// .repeat(both('knows').has('firstName', $name).has('person_id', neq($id)).dedup()).emit().times(3)
// .dedup().limit(20)
// .project('dist', 'person').by(loops()).by(identity())
// .fold()
// .map{ 排序 }

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic1_ln(conf: JobConf, person_id: u64, first_name: String) -> ResultStream<Vec<u8>> {
    pegasus::run(conf, || {
        let first_name = first_name.clone();
        move |input, output| {
            let person_internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize;
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![(person_internal_id.clone() as u64, 0 as u64, false)])
            } else {
                input.input_from(vec![])
            }?;

            let iterate_condition = IterCondition::max_iters(3);
            let vertex_result =
                stream.iterate_emit_until(IterCondition::max_iters(3), EmitKind::After, |start| {
                    let emit_count = 0;
                    start
                        .repartition(|(id, _, _)| Ok(*id))
                        .flat_map(move |(src_id, hop, selected)| {
                            Ok(super::graph::GRAPH
                                .get_out_vertices(src_id as DefaultId, Some(&vec![12]))
                                .map(|x| {
                                    let filter_name = super::graph::GRAPH
                                        .get_vertex(x.get_id())
                                        .unwrap()
                                        .get_property("firstName")
                                        .unwrap()
                                        .as_str()
                                        .unwrap();
                                    (x.get_id() as u64, hop + 1, filter_name == first_name)
                                })
                                .filter(|(id, hop, select)| {
                                    let neighbor_id =
                                        (id as usize ^ ((1 as usize) << LABEL_SHIFT_BITS)) as u64;
                                    (neighbor_id == person_id)
                                }))
                        })?
                        .aggregate()
                        .unary("dedup", |_| {
                            move |input, output| {
                                let mut table = TidyTagMap::<HashSet<u64>>::new(info.scope_level);
                                input.for_each_batch(|batch| {
                                    if !batch.is_empty() {
                                        let mut session = output.new_session(&batch.tag)?;
                                        let set = table.get_mut_or_insert(&batch.tag);
                                        for d in batch.drain() {
                                            if !set.contains(&d.0) {
                                                set.insert(d.0);
                                                session.give(d)?;
                                            }
                                        }
                                    }
                                    if batch.is_last() {
                                        table.remove(&batch.tag);
                                    }
                                    Ok(())
                                })
                            }
                        })
                })?;
            vertex_result
                .filter_map(|(id, hop, select) | {
                    if select {
                        let person_vertex = super::graph::GRAPH.get_vertex(id as DefaultId).unwrap();
                        let last_name = person_vertex.get_property("lastName").unwrap().as_str().unwrap().into_owned();
                        Ok(Some((hop, last_name, id)))
                    } else {
                        Ok(None)
                    }
                })?
                .sort_by(|x, y| {
                    x < y
                })?
                .unary("dedup_limit", |_|{
                    move |input, output| {
                        let mut table = TidyTagMap::<HashSet<u64>>::new(info.scope_level);
                        input.for_each_batch(|batch| {
                            if !batch.is_empty() {
                                let mut session = output.new_session(&batch.tag)?;
                                let set = table.get_mut_or_insert(&batch.tag);
                                for d in batch.drain() {
                                    if set.len() >= 20 {
                                        break;
                                    }
                                    if !set.contains(&d.0) {
                                        set.insert(d.0);
                                        session.give(d)?;
                                    }
                                }
                            }
                            if batch.is_last() {
                                table.remove(&batch.tag);
                            }
                            Ok(())
                        })
                    }
                })?
                .map( |hop, last_name, id| {
                    let person_vertex = super::graph::GRAPH.get_vertex(id as DefaultId).unwrap();
                    let birthday = person_vertex.get_property("birthday").unwrap().as_u64().unwrap();
                    let creation_date = person_vertex.get_property("creationDate").unwrap().as_u64().unwrap();
                    let gender = person_vertex.get_property("gender").unwrap().as_str().unwrap().into_owned();
                    let browser = person_vertex.get_property("browserUsed").unwrap().as_str().unwrap().into_owned();
                    let ip = person_vertex.get_property("locationIP").unwrap().as_str().unwrap().into_owned();
                    Ok(())
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic1 job failure")
}
