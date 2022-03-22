use graph_store::prelude::*;
use pegasus::api::{Dedup, EmitKind, IterCondition, Iteration, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 1 :
// g.V().hasLabel('person').has('person_id', $id)
// .repeat(both('knows').has('firstName', $name).has('person_id', neq($id)).dedup()).emit().times(3)
// .dedup().limit(20)
// .project('dist', 'person').by(loops()).by(identity())
// .fold()
// .map{ 排序 }

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic1_ir(
    conf: JobConf, person_id: u64, first_name: String,
) -> ResultStream<(u64, String, i32, u64, u64, String, String, String)> {
    pegasus::run(conf, || {
        let first_name = first_name.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id.clone()])
            } else {
                input.input_from(vec![])
            }?;

            stream
                .map(|source| {
                    Ok(((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64, 0))
                })?
                .iterate_emit_until(IterCondition::max_iters(3), EmitKind::After, |start| {
                    start
                        .repartition(|(id, _)| Ok(*id))
                        .flat_map(move |(person_internal_id, distance)| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_internal_id as DefaultId, Some(&vec![12]))
                                .map(move |x| (x.get_id() as u64, distance + 1)))
                        })
                })?
                .filter_map(move |(person_internal_id, distance)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let person_first_name = person_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if person_first_name == first_name {
                        Ok(Some((person_internal_id, distance)))
                    } else {
                        Ok(None)
                    }
                })?
                .dedup()?
                .map(|(person_internal_id, distance)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
                        .unwrap();
                    let last_name = person_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((distance, last_name, person_internal_id))
                })?
                .sort_limit_by(20, |x, y| {
                    x.0.cmp(&y.0)
                        .then(x.1.cmp(&y.1).then(x.2.cmp(&y.2)))
                })?
                .map(|(distance, last_name, person_internal_id)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id as DefaultId)
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
                    let birthday = person_vertex
                        .get_property("birthday")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let creation_date = person_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let gender = person_vertex
                        .get_property("gender")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let browser = person_vertex
                        .get_property("browserUsed")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let ip = person_vertex
                        .get_property("locationIP")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((
                        person_id,
                        last_name,
                        distance,
                        birthday,
                        creation_date,
                        gender,
                        browser,
                        ip,
                    ))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic1 job failure")
}
