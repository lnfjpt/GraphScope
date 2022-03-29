use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
use pegasus::api::{Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

pub fn is1_groot(
    conf: JobConf, person_id: i64,
) -> ResultStream<(String, String, i64, String, String, i64, String, i64)> {
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
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_inner_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .map(move |source| {
                    let person_inner_id = source[0];
                    let mut vl = super::groot_graph::GRAPH.get_vertex_properties(
                        MAX_SNAPSHOT_ID,
                        vec![(0, vec![(Some(4), vec![person_inner_id])])],
                        None,
                    );
                    let person_vertex = vl.next().unwrap();
                    let first_name = person_vertex
                        .get_property(12)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let last_name = person_vertex
                        .get_property(13)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let birthday = person_vertex
                        .get_property(15)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let location_ip = person_vertex
                        .get_property(7)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let browser_used = person_vertex
                        .get_property(8)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let locate_id = super::groot_graph::GRAPH
                        .get_out_vertex_ids(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![person_inner_id])],
                            &vec![17],
                            None,
                            None,
                            1,
                        )
                        .next()
                        .unwrap().1
                        .next()
                        .unwrap().get_id();
                    let located = super::groot_graph::GRAPH.get_vertex_properties(
                        MAX_SNAPSHOT_ID,
                        vec![(0, vec![(Some(8), vec![locate_id])])],
                        None,
                    ).next().unwrap();
                    let city_id = located
                        .get_property(3)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    let gender = person_vertex
                        .get_property(14)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let creation_date = person_vertex
                        .get_property(2)
                        .unwrap()
                        .get_long()
                        .unwrap();
                    Ok((
                        first_name,
                        last_name,
                        birthday,
                        location_ip,
                        browser_used,
                        city_id,
                        gender,
                        creation_date,
                    ))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
