use std::collections::{HashMap, HashSet};

use graph_store::prelude::*;
use pegasus::api::{CorrelatedSubTask, Dedup, Filter, Fold, KeyBy, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 12 with no subtask:
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('friend')
// .in('HASCREATOR').hasLabel('COMMENT').as('_t')
// .out('REPLYOF').hasLabel('POST').out('HASTAG').out('HASTYPE').has('name',eq('$tagClassName'))
// .select('_t').dedup().select('friend').groupCount()
// .order().by(select(values), desc).by(select(keys).values('id'), asc).limit(20)

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic12_nosub(
    conf: JobConf, person_id: u64, input_tag_name: String,
) -> ResultStream<(u64, String, String, i32)> {
    pegasus::run(conf, || {
        let input_tag_name = input_tag_name.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                // g.V().hasLabel('PERSON').has('id',$personId)
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                // .both('KNOWS')
                .flat_map(|person_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_both_vertices(person_internal_id as DefaultId, Some(&vec![12]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .repartition(|id| Ok(*id))
                // .as('friend').in('HASCREATOR').hasLabel('COMMENT')
                .flat_map(|person_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(person_id as DefaultId, Some(&vec![0]))
                        .filter(|vertex| vertex.get_label()[0] == 2)
                        .map(move |vertex| (person_id, vertex.get_id() as u64)))
                })?
                .repartition(|(_, comment_id)| Ok(*comment_id))
                // .as('_t').out('REPLYOF').hasLabel('POST')
                .filter_map(|(person_internal_id, comment_id)| {
                    let reply_message = super::graph::GRAPH
                        .get_out_vertices(comment_id as DefaultId, Some(&vec![3]))
                        .next()
                        .unwrap();
                    if reply_message.get_label()[0] == 3 {
                        Ok(Some((person_internal_id, comment_id, reply_message.get_id() as u64)))
                    } else {
                        Ok(None)
                    }
                })?
                .repartition(|(_, _, post_internal_id)| Ok(*post_internal_id))
                // .out('HASTAG')
                .flat_map(|(person_internal_id, comment_id, post_internal_id)| {
                    Ok(super::graph::GRAPH
                        .get_out_vertices(post_internal_id as DefaultId, Some(&vec![1]))
                        .map(move |vertex| (person_internal_id, comment_id, vertex.get_id() as u64)))
                })?
                // .out('HASTYPE').has('name',eq('$tagClassName'))
                .filter_map(move |(person_internal_id, comment_id, tag_internal_id)| {
                    if let Some(tag_class_vertex) = super::graph::GRAPH
                        .get_out_vertices(tag_internal_id as DefaultId, Some(&vec![21]))
                        .next()
                    {
                        let tag_class_id = tag_class_vertex.get_id();
                        let tag_class = super::graph::GRAPH.get_vertex(tag_class_id).unwrap();
                        let tag_class_name =
                            tag_class.get_property("name").unwrap().as_str().unwrap().into_owned();
                        if tag_class_name == input_tag_name {
                            Ok(Some((person_internal_id, comment_id)))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                })?
                // .select('_t').dedup().select('friend')
                .key_by(|(person_internal_id, comment_id)| Ok((comment_id, person_internal_id)))?
                .dedup()?
                .map(|pair| Ok(pair.value))?
                // .groupCount()
                .fold(HashMap::<u64, i32>::new(), || {
                    |mut collect, friend_id| {
                        if let Some(data) = collect.get_mut(&friend_id) {
                            *data += 1;
                        } else {
                            collect.insert(friend_id, 1);
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    let mut friend_list = vec![];
                    for (friend_id, count) in map {
                        friend_list.push((friend_id, count));
                    }
                    Ok(friend_list.into_iter())
                })?
                // .order()
                .sort_limit_by(20, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .map(|(person_internal_id, count)| {
                    let person_vertex =
                        super::graph::GRAPH.get_vertex(person_internal_id as DefaultId).unwrap();
                    let person_id = person_vertex.get_property("id").unwrap().as_u64().unwrap();
                    let person_first_name =
                        person_vertex.get_property("firstName").unwrap().as_str().unwrap().into_owned();
                    let person_last_name =
                        person_vertex.get_property("lastName").unwrap().as_str().unwrap().into_owned();
                    Ok((person_id, person_first_name, person_last_name, count))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic12 job failure")
}
