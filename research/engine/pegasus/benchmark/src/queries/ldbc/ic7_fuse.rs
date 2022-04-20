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

pub fn ic7(
    conf: JobConf, person_id: u64,
) -> ResultStream<(u64, String, String, u64, u64, String, i32, bool)> {
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(move |source| {
                    let person_internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | source as usize;
                    let mut data_list = vec![];
                    for message_vertex in
                    super::graph::GRAPH.get_in_vertices(person_internal_id, Some(&vec![0]))
                    {
                        let message_internal_id = message_vertex.get_id();
                        for edge in super::graph::GRAPH
                            .get_in_edges(message_internal_id as DefaultId, Some(&vec![13]))
                        {
                            data_list.push((
                                edge.get_property("creationDate")
                                    .unwrap()
                                    .as_u64()
                                    .unwrap(),
                                message_internal_id as u64,
                                edge.get_src_id() as u64,
                                person_internal_id as u64,
                            ));
                        }
                    }
                    data_list.sort_by(|x, y| x.0.cmp(&y.0).reverse().then(x.2.cmp(&y.2)));
                    if data_list.len() > 20 {
                        data_list.resize(20, (0, 0, 0, 0));
                    }
                    let mut result_list = vec![];
                    for (like_date, message_internal_id, friend_internal_id, person_internal_id) in data_list {
                        let message_vertex = super::graph::GRAPH
                            .get_vertex(message_internal_id as DefaultId)
                            .unwrap();
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
                        let friend_last_name = friend_vertex
                            .get_property("lastName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let mut is_new = true;
                        for i in super::graph::GRAPH
                            .get_both_vertices(friend_internal_id as DefaultId, Some(&vec![12]))
                        {
                            if i.get_id() == person_internal_id as DefaultId {
                                is_new = false;
                                break;
                            }
                        }
                        let message_id = message_vertex
                            .get_property("id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let content = match message_vertex.get_label()[0] {
                            2 => message_vertex
                                .get_property("content")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned(),
                            3 => message_vertex
                                .get_property("content")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned(),
                            _ => "".to_string(),
                        };
                        let message_date = message_vertex
                            .get_property("creationDate")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        result_list.push((
                            friend_id,
                            friend_first_name,
                            friend_last_name,
                            like_date,
                            message_id,
                            content,
                            (like_date - message_date) as i32,
                            is_new,
                        ));
                    }
                    Ok(result_list.into_iter())
                })?
                .sink_into(output)
        }
    })
        .expect("submit ic7 job failure")
}
