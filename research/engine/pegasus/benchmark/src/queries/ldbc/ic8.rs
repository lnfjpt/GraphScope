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

pub fn ic8(conf: JobConf, person_id: u64) -> ResultStream<(u64, String, String, u64, u64, String)> {
    pegasus::run(conf, || {
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .flat_map(|person_interanl_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(person_interanl_id as DefaultId, Some(&vec![0]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .repartition(|id| Ok(*id))
                .flat_map(|message_internal_id| {
                    Ok(super::graph::GRAPH
                        .get_in_vertices(message_internal_id as DefaultId, Some(&vec![3]))
                        .map(|vertex| vertex.get_id() as u64))
                })?
                .map(|comment_internal_id| {
                    let comment_vertex = super::graph::GRAPH
                        .get_vertex(comment_internal_id as DefaultId)
                        .unwrap();
                    let creation_date = comment_vertex
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    Ok((comment_internal_id, creation_date))
                })?
                .sort_limit_by(20, |x, y| x.1.cmp(&y.1).reverse().then(x.0.cmp(&y.0)))?
                .map(|(comment_internal_id, creation_date)| {
                    let comment_vertex = super::graph::GRAPH
                        .get_vertex(comment_internal_id as DefaultId)
                        .unwrap();
                    let mut person_internal_id = 0;
                    for i in super::graph::GRAPH
                        .get_out_vertices(comment_internal_id as DefaultId, Some(&vec![0]))
                    {
                        person_internal_id = i.get_id();
                        break;
                    }
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_internal_id)
                        .unwrap();
                    let person_id = person_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let person_first_name = person_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let person_last_name = person_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let comment_vertex = super::graph::GRAPH
                        .get_vertex(comment_internal_id as DefaultId)
                        .unwrap();
                    let comment_id = comment_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let comment_content = comment_vertex
                        .get_property("content")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((
                        person_id,
                        person_first_name,
                        person_last_name,
                        creation_date,
                        comment_id,
                        comment_content,
                    ))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic8 job failure")
}
