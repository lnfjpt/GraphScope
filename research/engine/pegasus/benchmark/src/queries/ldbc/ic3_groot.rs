use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
use pegasus::api::{
    CorrelatedSubTask, Dedup, EmitKind, Filter, Fold, IterCondition, Iteration, Map, Sink, SortLimitBy,
};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

pub fn ic3_groot(
    conf: JobConf, person_id: i64, country_x: String, country_y: String, start_date: String, duration: i32,
) -> ResultStream<(i64, String, String, i32, i32, i32)> {
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
    let duration = duration as i64 * 24 * 3600 * 100 * 1000;
    let end_date = start_date.parse::<i64>().unwrap() + duration;
    let start_date = super::graph::parse_datetime(&start_date).unwrap() as i64;
    let end_date = super::graph::parse_datetime(&end_date.to_string()).unwrap() as i64;
    pegasus::run(conf, || {
        let country_x = country_x.clone();
        let country_y = country_y.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![(person_inner_id, country_x.clone(), country_y.clone())])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .iterate_emit_until(IterCondition::max_iters(2), EmitKind::After, |start| {
                    start
                        .repartition(|(id, _, _)| Ok(*id as u64))
                        .flat_map(move |(person_id, country_x, country_y)| {
                            let cx = country_x.clone();
                            let cy = country_y.clone();
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
                                .map(move |vertex| (vertex.get_id(), country_x.clone(), country_y.clone()))
                                .filter(move |(id, _, _)| person_inner_id != *id)
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
                                        .map(move |vertex| {
                                            (vertex.get_id(), cx.clone(), cy.clone())
                                        })
                                        .filter(move |(id, _, _)| person_inner_id != *id),
                                ))
                        })
                })?
                .dedup()?
                .repartition(|(id, _, _)| Ok(*id as u64))
                .map(|(person_internal_id, country_x, country_y)| {
                    let city_id = super::groot_graph::GRAPH
                        .get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![person_internal_id])],
                            &vec![17],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .next()
                        .unwrap()
                        .get_id();
                    let country_id = super::groot_graph::GRAPH
                        .get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![city_id])],
                            &vec![14],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .next()
                        .unwrap()
                        .get_id();
                    Ok((person_internal_id, country_id, country_x, country_y))
                })?
                .filter_map(move |(person_internal_id, country_id, country_x, country_y)| {
                    let country_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(8), vec![country_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let country_name = country_vertex
                        .get_property(4)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    if country_name != country_x && country_name != country_y {
                        Ok(Some((person_internal_id, country_x, country_y)))
                    } else {
                        Ok(None)
                    }
                })?
                .apply(|sub| {
                    let start_date = start_date;
                    let end_date = end_date;
                    sub.flat_map(|(person_id, country_x, country_y)| {
                        Ok(super::groot_graph::GRAPH
                            .get_in_vertex_ids(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![person_id])],
                                &vec![9],
                                None,
                                None,
                                usize::max_value(),
                            )
                            .next()
                            .unwrap()
                            .1
                            .map(move |vertex| {
                                (vertex.get_id(), country_x.clone(), country_y.clone())
                            }))
                    })?
                    .filter_map(move |(message_internal_id, country_x, country_y)| {
                        let vertex = super::groot_graph::GRAPH
                            .get_vertex_properties(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![(None, vec![message_internal_id])])],
                                None,
                            )
                            .next()
                            .unwrap();
                        let create_time = vertex
                            .get_property(2)
                            .unwrap()
                            .get_long()
                            .unwrap();
                        if create_time > start_date && create_time < end_date {
                            Ok(Some((message_internal_id, country_x, country_y)))
                        } else {
                            Ok(None)
                        }
                    })?
                    .flat_map(|(message_internal_id, country_x, country_y)| {
                        Ok(super::groot_graph::GRAPH
                               .get_out_vertex_ids(
                                   MAX_SNAPSHOT_ID,
                                   vec![(0, vec![message_internal_id])],
                                   &vec![17],
                                   None,
                                   None,
                                   usize::max_value(),
                               )
                               .next()
                               .unwrap()
                               .1
                            .map(move |vertex| {
                                (
                                    message_internal_id,
                                    vertex.get_id(),
                                    country_x.clone(),
                                    country_y.clone(),
                                )
                            }))
                    })?
                    .filter_map(|(message_internal_id, country_id, country_x, country_y)| {
                        let country_vertex = super::groot_graph::GRAPH
                            .get_vertex_properties(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![(Some(8), vec![country_id])])],
                                None,
                            )
                            .next()
                            .unwrap();
                        let country = country_vertex
                            .get_property(4)
                            .unwrap()
                            .get_string()
                            .unwrap()
                            .clone();
                        if country == country_x {
                            Ok(Some((1, 0)))
                        } else if country == country_y {
                            Ok(Some((0, 1)))
                        } else {
                            Ok(None)
                        }
                    })?
                    .fold((0, 0), || |a, b| Ok((a.0 + b.0, a.1 + b.1)))
                })?
                .filter_map(|(person_internal_id, count)| {
                    if count.0 > 0 && count.1 > 0 {
                        Ok(Some((person_internal_id, count)))
                    } else {
                        Ok(None)
                    }
                })?
                .sort_limit_by(20, |(id_x, count_x), (id_y, count_y)| {
                    (count_x.0 + count_x.1)
                        .cmp(&(count_y.0 + count_y.1))
                        .reverse()
                        .then(id_x.cmp(&id_y))
                })?
                .map(|((person_internal_id, country_x, country_y), count)| {
                    let vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(4), vec![person_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let person_id = vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let first_name = vertex
                        .get_property(12)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let last_name = vertex
                        .get_property(13)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    Ok((person_id, first_name, last_name, count.0, count.1, count.0 + count.1))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic3 job failure")
}
