use graph_store::prelude::*;
use pegasus::api::{Dedup, EmitKind, Filter, IterCondition, Iteration, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic9(conf: JobConf, person_id: u64) -> ResultStream<(u64, String, String, u64, String, u64)> {
    pegasus::run(conf, || {
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
                        .flat_map(move |person_id| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_id as DefaultId, Some(&vec![12]))
                                .map(move |vertex| vertex.get_id() as u64)
                                .filter(move |id| {
                                    ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize != *id as usize
                                }))
                        })
                })?
                .dedup()?
                .flat_map(move |person_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(person_id as DefaultId, Some(&vec![0]))
                        .map(move |vertex| (person_id, vertex.get_id() as u64)))
                })?
                .map(|(person_id, message_internal_id)| {
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
                    Ok((person_id, message_internal_id, create_date, message_id))
                })?
                .sort_limit_by(20, |x, y| x.2.cmp(&y.2).reverse().then(x.3.cmp(&y.3)))?
                .map(|(person_internal_id, message_internal_id, create_date, message_id)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let message_vertex = super::graph::GRAPH
                        .get_vertex(message_internal_id as DefaultId)
                        .unwrap();
                    let person_id = person_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let person_first_name = person_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let person_last_name = person_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let content = match message_vertex.get_label()[0] {
                        2 => message_vertex
                            .get_property("content")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned(),
                        3 => message_vertex
                            .get_property("content")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned(),
                        _ => "".to_string(),
                    };
                    Ok((person_id, person_first_name, person_last_name, message_id, content, create_date))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic9 job failure")
}
