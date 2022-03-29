use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
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

pub fn ic1_groot(
    conf: JobConf, person_id: i64, first_name: String,
) -> ResultStream<(i64, String, i32, i64, i64, String, String, String)> {
    let person_vertices = super::groot_graph::GRAPH.get_all_vertices(
        MAX_SNAPSHOT_ID,
        &vec![4],
        None,
        None,
        None,
        usize::max_value(),
        &vec![0],
    );
    let mut person_inner_id = 0 as i64;
    for i in person_vertices {
        let inner_id = i.get_property(3).unwrap().get_long().unwrap();
        if inner_id == person_id {
            person_inner_id = i.get_id();
            break;
        }
    }
    pegasus::run(conf, || {
        let first_name = first_name.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_inner_id])
            } else {
                input.input_from(vec![])
            }?;

            stream
                .map(|source| Ok((source, 0)))?
                .iterate_emit_until(IterCondition::max_iters(3), EmitKind::After, |start| {
                    start
                        .repartition(|(id, _)| Ok(*id as u64))
                        .flat_map(move |(person_internal_id, distance)| {
                            Ok(super::groot_graph::GRAPH
                                .get_out_vertex_ids(
                                    MAX_SNAPSHOT_ID,
                                    vec![(0, vec![person_id])],
                                    &vec![22],
                                    None,
                                    None,
                                    usize::max_value(),
                                )
                                .next()
                                .unwrap()
                                .1
                                .map(move |vertex| (vertex.get_id(), distance + 1))
                                .chain(
                                    super::groot_graph::GRAPH
                                        .get_in_vertex_ids(
                                            MAX_SNAPSHOT_ID,
                                            vec![(0, vec![person_id])],
                                            &vec![22],
                                            None,
                                            None,
                                            usize::max_value(),
                                        )
                                        .next()
                                        .unwrap()
                                        .1
                                        .map(move |vertex| (vertex.get_id(), distance + 1)),
                                ))
                        })
                })?
                .filter_map(move |(person_internal_id, distance)| {
                    let person_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(4), vec![person_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let person_first_name = person_vertex
                        .get_property(12)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    if person_first_name == first_name {
                        Ok(Some((person_internal_id, distance)))
                    } else {
                        Ok(None)
                    }
                })?
                .dedup()?
                .map(|(person_internal_id, distance)| {
                    let person_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(4), vec![person_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let last_name = person_vertex
                        .get_property(13)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    Ok((distance, last_name, person_internal_id))
                })?
                .sort_limit_by(20, |x, y| {
                    x.0.cmp(&y.0)
                        .then(x.1.cmp(&y.1).then(x.2.cmp(&y.2)))
                })?
                .map(|(distance, last_name, person_internal_id)| {
                    let person_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(4), vec![person_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let person_id = person_vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let birthday = person_vertex
                        .get_property(15)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let creation_date = person_vertex
                        .get_property(2)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let gender = person_vertex
                        .get_property(14)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let browser = person_vertex
                        .get_property(8)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let ip = person_vertex
                        .get_property(7)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    Ok((person_id, last_name, distance, birthday, creation_date, gender, browser, ip))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic1 job failure")
}
