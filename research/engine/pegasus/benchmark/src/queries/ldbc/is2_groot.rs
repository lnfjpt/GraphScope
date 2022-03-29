use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
use pegasus::api::{IterCondition, Iteration, Limit, Map, Sink, SortBy, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

pub fn is2_groot(
    conf: JobConf, message_id: i64,
) -> ResultStream<(i64, String, i64, i64, i64, String, String)> {
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
        if inner_id == message_id {
            person_inner_id = i.get_id();
            break;
        }
    }
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_inner_id]) } else { None };
        move |input, output| {
            let mut condition = IterCondition::new();
            condition.until(|item: &(i64, i64, i64)| {
                Ok(super::groot_graph::Graph
                    .get_vertex_properties(MAX_SNAPSHOT_ID, vec![(0, vec![(None, vec![item.2])])], None)
                    .next()
                    .unwrap()
                    .get_label()
                    == 3)
            });
            input
                .input_from(start)?
                .flat_map(move |source| {
                    let person_inner_id = source[0];
                    let message_vertices = super::groot_graph::GRAPH
                        .get_in_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![person_inner_id])],
                            &vec![9],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1;
                    let mut lists = vec![];
                    for i in message_vertices {
                        let message_internal_id = i.get_id();
                        let message_vertex = super::groot_graph::GRAPH
                            .get_vertex_properties(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![(None, vec![message_internal_id])])],
                                None,
                            )
                            .next()
                            .unwrap();
                        let create_date = message_vertex.get_property(2).unwrap().get_long().unwrap();
                        lists.push((create_date, message_internal_id, message_internal_id));
                    }
                    Ok(lists.into_iter())
                })?
                .sort_limit_by(10, |x, y| {
                    x.0.cmp(&y.0)
                        .reverse()
                        .then(x.1.cmp(&y.1).reverse())
                })?
                .iterate_until(condition, |start| {
                    start.map(|(create_date, message_internal_id, reply_internal_id)| {
                        let mut reply_id = 0;
                        let reply_vertex = super::groot_graph::GRAPH
                            .get_out_vertex_ids(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![reply_internal_id])],
                                &vec![20],
                                None,
                                None,
                                usize::max_value(),
                            )
                            .next()
                            .unwrap()
                            .1
                            .next()
                            .unwrap();
                        Ok((create_date, message_internal_id, reply_vertex.get_id()))
                    })
                })?
                .map(move |(create_date, message_internal_id, post_internal_id)| {
                    let message_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(None, vec![message_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let message_id = message_vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let post_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(3), vec![post_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let post_id = post_vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let author_vertex = super::groot_graph::GRAPH
                        .get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![post_internal_id])],
                            &vec![9],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .next()
                        .unwrap();
                    let author_id = author_vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let author_first_name = author_vertex
                        .get_property(12)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let author_last_name = author_vertex
                        .get_property(13)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    match message_vertex.get_label() {
                        2 => Ok((
                            message_id,
                            message_vertex
                                .get_property(10)
                                .unwrap()
                                .get_string()
                                .unwrap()
                                .clone(),
                            create_date,
                            post_id,
                            author_id,
                            author_first_name,
                            author_last_name,
                        )),
                        3 => Ok((
                            message_id,
                            message_vertex
                                .get_property(6)
                                .unwrap()
                                .get_string()
                                .unwrap()
                                .clone(),
                            create_date,
                            post_id,
                            author_id,
                            author_first_name,
                            author_last_name,
                        )),
                        _ => Ok((
                            message_id,
                            "".to_string(),
                            create_date,
                            post_id,
                            author_id,
                            author_first_name,
                            author_last_name,
                        )),
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
