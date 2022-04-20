use graph_store::prelude::*;
use pegasus::api::{Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic2(
    conf: JobConf, person_id: u64, max_date: String,
) -> ResultStream<(u64, String, String, u64, String, u64)> {
    let max_date = super::graph::parse_datetime(&max_date).unwrap();
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(move |source| {
                    let source_internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | source as usize;
                    let mut message_vec = vec![];
                    for friend_vertex in
                        super::graph::GRAPH.get_both_vertices(source_internal_id, Some(&vec![12]))
                    {
                        let friend_internal_id = friend_vertex.get_id();
                        for message_vertex in
                            super::graph::GRAPH.get_in_vertices(friend_internal_id, Some(&vec![0]))
                        {
                            let message_vertex = super::graph::GRAPH
                                .get_vertex(message_vertex.get_id())
                                .unwrap();
                            let create_date = message_vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            if create_date <= max_date {
                                continue;
                            }
                            let message_id = message_vertex
                                .get_property("id")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            message_vec.push((
                                create_date,
                                message_id,
                                message_vertex.get_id() as u64,
                                friend_internal_id as u64,
                            ));
                        }
                    }
                    message_vec.sort_by(|x, y| x.0.cmp(&y.0).reverse().then(x.1.cmp(&y.1)));
                    if message_vec.len() > 20 {
                        message_vec.resize(20, (0, 0, 0, 0));
                    }
                    let mut result_vec = vec![];
                    for (create_date, message_id, message_internal_id, friend_internal_id) in message_vec {
                        let friend_vertex = super::graph::GRAPH
                            .get_vertex(friend_internal_id as DefaultId)
                            .unwrap();
                        let friend_id = friend_vertex
                            .get_property("id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let friend_first_name = friend_vertex
                            .get_property("firstName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let friend_second_name = friend_vertex
                            .get_property("lastName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let message_vertex = super::graph::GRAPH
                            .get_vertex(message_internal_id as DefaultId)
                            .unwrap();
                        let content = if message_vertex.get_label()[0] == 2 {
                            message_vertex
                                .get_property("content")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned()
                        } else {
                            message_vertex
                                .get_property("imageFile")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned()
                        };
                        result_vec.push((
                            friend_id,
                            friend_first_name,
                            friend_second_name,
                            message_id,
                            content,
                            create_date,
                        ));
                    }
                    Ok(result_vec.into_iter())
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic2 job failure")
}
