use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex, MAX_SNAPSHOT_ID};
use maxgraph_store::groot::global_graph::GlobalGraph;
use pegasus::api::{IterCondition, Iteration, Limit, Map, Sink, SortBy, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use std::cmp::Ordering;

pub fn is3_groot(conf: JobConf, person_id: i64) -> ResultStream<(i64, String, String, i64)> {
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
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_inner_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .flat_map(move |source| {
                    let person_inner_id = source[0];
                    let knows_edge = super::groot_graph::GRAPH
                        .get_out_edges(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![person_inner_id])],
                            &vec![22],
                            None,
                            None,
                            None,
                            usize::max_value(),
                        )
                        .next()
                        .unwrap()
                        .1;
                    let mut lists = vec![];
                    for i in knows_edge {
                        let knows_date = i.get_property(2).unwrap().get_long().unwrap();
                        let person_inner_id = i.get_dst_id();
                        let pv = super::groot_graph::GRAPH
                            .get_vertex_properties(
                                MAX_SNAPSHOT_ID,
                                vec![(0, vec![(Some(4), vec![person_inner_id])])],
                                None,
                            )
                            .next()
                            .unwrap();
                        let person_id = pv.get_property(3).unwrap().get_long().unwrap();
                        lists.push((knows_date, person_id, person_inner_id));
                    }
                    Ok(lists.into_iter())
                })?
                .sort_by(|x, y| x.0.cmp(&y.0).reverse().then(x.1.cmp(&y.1)))?
                .map(move |(knows_date, person_id, person_inner_id)| {
                    let lv = super::groot_graph::GRAPH
                        .get_vertex_properties(
                            MAX_SNAPSHOT_ID,
                            vec![(0, vec![(Some(4), vec![person_inner_id])])],
                            None,
                        )
                        .next()
                        .unwrap();
                    let first_name = lv
                        .get_property(12)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    let second_name = lv
                        .get_property(13)
                        .unwrap()
                        .get_string()
                        .unwrap()
                        .clone();
                    Ok((person_id, first_name, second_name, knows_date))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
