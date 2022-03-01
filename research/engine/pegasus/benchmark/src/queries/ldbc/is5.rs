use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::api::{Binary, Branch, IterCondition, Iteration, Map, Sink, Sort, Unary};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is5(
    conf: JobConf, graph: LargeGraphDB<DefaultId, InternalId>, person_id: u64,
) -> ResultStream<(u64, String, String)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .map(move |source| {
                    let vertex_id = source[0] as DefaultId;
                    let post_id = ((3 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                    let comment_id = ((2 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                    let mut person_id = 0;
                    let mut first_name = "".to_string();
                    let mut second_name = "".to_string();
                    if let Some(vertex) = graph.get_vertex(post_id) {
                        let person = graph.get_out_vertices(vertex.get_id(), Some(&vec![0]));
                        for i in person {
                            person_id = i
                                .get_property("personId")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            first_name = i
                                .get_property("firstName")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned();
                            second_name = i
                                .get_property("secondName")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned();
                            break;
                        }
                    } else if let Some(vertex) = graph.get_vertex(comment_id) {
                        let person = graph.get_out_vertices(vertex.get_id(), Some(&vec![0]));
                        for i in person {
                            person_id = i
                                .get_property("personId")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            first_name = i
                                .get_property("firstName")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned();
                            second_name = i
                                .get_property("secondName")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned();
                            break;
                        }
                    }
                    Ok((person_id, first_name, second_name))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
