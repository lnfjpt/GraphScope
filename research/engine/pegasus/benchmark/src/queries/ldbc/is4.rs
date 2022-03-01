use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::api::{Binary, Branch, IterCondition, Iteration, Map, Sink, Sort, Unary};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is4(
    conf: JobConf, graph: LargeGraphDB<DefaultId, InternalId>, person_id: u64,
) -> ResultStream<(u64, String)> {
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
                    let mut date = 0;
                    let mut content = "".to_string();
                    if let Some(vertex) = graph.get_vertex(post_id) {
                        date = vertex
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        content = vertex
                            .get_property("imageFile")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                    } else if let Some(vertex) = graph.get_vertex(comment_id) {
                        date = vertex
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        content = vertex
                            .get_property("content")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                    }
                    Ok((date, content))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
