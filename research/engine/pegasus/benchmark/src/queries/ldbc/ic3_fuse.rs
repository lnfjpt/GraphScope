use graph_store::prelude::*;
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

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic3(
    conf: JobConf, person_id: u64, country_x: String, country_y: String, start_date: String, duration: i32,
) -> ResultStream<(u64, String, String, i32, i32, i32)> {
    let duration = duration as i64 * 24 * 3600 * 100 * 1000;
    let end_date = start_date.parse::<i64>().unwrap() + duration;
    let start_date = super::graph::parse_datetime(&start_date).unwrap();
    let end_date = super::graph::parse_datetime(&end_date.to_string()).unwrap();
    pegasus::run(conf, || {
        let country_x = country_x.clone();
        let country_y = country_y.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![(person_id, country_x.clone(), country_y.clone())])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .flat_map(move |(source, country_x, country_y)| {
                    let source_internal_id = (((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64;
                    let mut friend_list = Vec::<u64>::new();
                    let mut current_list = vec![];
                    let mut temp_vec = vec![];
                    current_list.push(source_internal_id);
                    for _ in 0..2 {
                        for person_internal_id in current_list {
                            for vertex in super::graph::GRAPH
                                .get_both_vertices(person_internal_id as DefaultId, Some(&vec![12]))
                            {
                                if vertex.get_id() as u64 != source_internal_id {
                                    temp_vec.push(vertex.get_id() as u64);
                                }
                            }
                        }
                        current_list = temp_vec.clone();
                        friend_list.append(&mut temp_vec);
                    }
                    friend_list.sort();
                    friend_list.dedup();
                    let mut person_list = vec![];
                    for friend_internal_id in friend_list {
                        let city_id = super::graph::GRAPH
                            .get_out_vertices(friend_internal_id as DefaultId, Some(&vec![11]))
                            .next()
                            .unwrap()
                            .get_id();
                        let country_id = super::graph::GRAPH
                            .get_out_vertices(city_id as DefaultId, Some(&vec![17]))
                            .next()
                            .unwrap()
                            .get_id();
                        let country_vertex = super::graph::GRAPH
                            .get_vertex(country_id)
                            .unwrap();
                        let country_name = country_vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if country_name != country_x && country_name != country_y {
                            person_list.push(friend_internal_id);
                        }
                    }
                    let mut count_list = vec![];
                    for person_internal_id in person_list {
                        let mut count = (0, 0);
                        for message_vertex in super::graph::GRAPH
                            .get_in_vertices(person_internal_id as DefaultId, Some(&vec![0]))
                        {
                            let message_internal_id = message_vertex.get_id();
                            let message_property_vertex = super::graph::GRAPH
                                .get_vertex(message_internal_id)
                                .unwrap();
                            let create_time = message_property_vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap();
                            if create_time > start_date && create_time < end_date {
                                for country_vertex in super::graph::GRAPH
                                    .get_out_vertices(message_internal_id, Some(&vec![11]))
                                {
                                    let country_internal_id = country_vertex.get_id();
                                    let country_property_vertex = super::graph::GRAPH
                                        .get_vertex(country_internal_id)
                                        .unwrap();
                                    let country = country_property_vertex
                                        .get_property("name")
                                        .unwrap()
                                        .as_str()
                                        .unwrap()
                                        .into_owned();
                                    if country == country_x {
                                        count.0 += 1;
                                    } else if country == country_y {
                                        count.1 += 1;
                                    }
                                }
                            }
                        }
                        if count.0 > 0 && count.1 > 0 {
                            count_list.push((person_internal_id, count.0, count.1));
                        }
                    }
                    count_list.sort_by(|(id_x, count_x0, count_x1), (id_y, count_y0, count_y1)| {
                        (count_x0 + count_x1)
                            .cmp(&(count_y0 + count_y1))
                            .reverse()
                            .then(id_x.cmp(&id_y))
                    });
                    if count_list.len() > 20 {
                        count_list.resize(20, (0,0,0));
                    }
                    let mut result_list = vec![];
                    for (person_internal_id, count0, count1) in count_list {
                        let vertex = super::graph::GRAPH
                            .get_vertex(person_internal_id as DefaultId)
                            .unwrap();
                        let person_id = vertex
                            .get_property("id")
                            .unwrap()
                            .as_u64()
                            .unwrap();
                        let first_name = vertex
                            .get_property("firstName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        let last_name = vertex
                            .get_property("lastName")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        result_list.push((person_id, first_name, last_name, count0, count1, count0 + count1));
                    }
                    Ok(result_list.into_iter())
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic3 job failure")
}
