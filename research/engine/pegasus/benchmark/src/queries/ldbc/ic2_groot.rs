use graph_store::common::DefaultId;
use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
use pegasus::api::{Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

pub fn ic2_groot(
    conf: JobConf, person_id: i64, max_date: String,
) -> ResultStream<(i64, String, String, i64, String, i64)> {
    let max_date = super::graph::parse_datetime(&max_date).unwrap() as i64;

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
            person_inner_id = inner_id;
            break;
        }
    }
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_inner_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(|source_person| {
                    Ok(super::groot_graph::GRAPH
                        .get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![source_person])],
                            &vec![22],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .map(|vertex| vertex.get_id())
                        .chain(
                            super::groot_graph::GRAPH
                                .get_in_vertex_ids(
                                    MAX_SNAPSHOT_ID,
                                    vec![(0, vec![source_person])],
                                    &vec![22],
                                    None,
                                    None,
                                    usize::max_value(),
                                )
                                .next()
                                .unwrap()
                                .1
                                .map(|vertex| vertex.get_id()),
                        ))
                })?
                .repartition(|id| Ok(*id))
                .flat_map(|friend_id| {
                    Ok(super::groot_graph::GRAPH
                        ..get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![friend_id])],
                            &vec![20],
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1
                        .map(move |vertex| (vertex.get_id(), friend_id)))
                })?
                .filter_map(move |(message_internal_id, friend_id)| {
                    let message_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(None, vec![message_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let create_date = message_vertex
                        .get_property(2)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let message_id = message_vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    if create_date <= max_date {
                        Ok(Some((create_date, message_id, message_internal_id, friend_id)))
                    } else {
                        Ok(None)
                    }
                })?
                .sort_limit_by(20, |x, y| x.0.cmp(&y.0).reverse().then(x.1.cmp(&y.1)))?
                .map(|(create_date, message_id, message_internal_id, friend_internal_id)| {
                    let friend_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(4), vec![message_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let friend_id = friend_vertex
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let friend_first_name = friend_vertex
                        .get_property(12)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let friend_second_name = friend_vertex
                        .get_property(13)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let message_vertex = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(None, vec![message_internal_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let content = if message_vertex.get_label_id() == 6 {
                        message_vertex
                            .get_property(10)
                            .unwrap()
                            .get_string()
                            .unwrap()
                            .clone()
                    } else {
                        message_vertex
                            .get_property(6)
                            .unwrap()
                            .get_string()
                            .unwrap()
                            .clone()
                    };
                    Ok((friend_id, friend_first_name, friend_second_name, message_id, content, create_date))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic2 job failure")
}
