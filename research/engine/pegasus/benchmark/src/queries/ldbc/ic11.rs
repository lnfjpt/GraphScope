use graph_store::prelude::*;
use pegasus::api::{Dedup, EmitKind, Filter, IterCondition, Iteration, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

// interactive complex query 2 :
// g.V().hasLabel('PERSON').has('id',$personId).both('KNOWS').as('p')
// .in('HASCREATOR').has('creationDate',lte($maxDate)).order().by('creationDate',desc)
// .by('id',asc).limit(20).as('m').select('p', 'm').by(valueMap('id', 'firstName', 'lastName'))
// .by(valueMap('id', 'imageFile', 'creationDate', 'content'))

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn ic11(
    conf: JobConf, person_id: u64, input_country_name: String, year: i32,
) -> ResultStream<(u64, String, String, String, i32)> {
    pegasus::run(conf, || {
        let input_country_name = input_country_name.clone();
        move |input, output| {
            let stream = if input.get_worker_index() == 0 {
                input.input_from(vec![person_id])
            } else {
                input.input_from(vec![])
            }?;
            stream
                .map(|source| Ok((((1 as usize) << LABEL_SHIFT_BITS) | source as usize) as u64))?
                .iterate_emit_until(IterCondition::max_iters(2), EmitKind::After, |start| {
                    start
                        .repartition(|id| Ok(*id))
                        .flat_map(move |person_id| {
                            Ok(super::graph::GRAPH
                                .get_both_vertices(person_id as DefaultId, Some(&vec![12]))
                                .map(move |vertex| vertex.get_id() as u64)
                                .filter(move |id| {
                                    ((1 as usize) << LABEL_SHIFT_BITS) | person_id as usize != *id as usize
                                }))
                        })
                })?
                .dedup()?
                .flat_map(move |person_id| {
                    Ok(super::graph::GRAPH
                        .get_out_edges(person_id as DefaultId, Some(&vec![16]))
                        .map(move |edge| {
                            (
                                person_id,
                                edge.get_dst_id() as u64,
                                edge.get_property("workFrom")
                                    .unwrap()
                                    .as_i32()
                                    .unwrap(),
                            )
                        }))
                })?
                .filter_map(move |(person_id, company_internal_id, work_from)| {
                    if work_from < year {
                        Ok(Some((person_id, company_internal_id, work_from)))
                    } else {
                        Ok(None)
                    }
                })?
                .map(|(person_id, company_internal_id, work_from)| {
                    let mut country_internal_id = 0;
                    for i in super::graph::GRAPH
                        .get_out_vertices(company_internal_id as DefaultId, Some(&vec![11]))
                    {
                        country_internal_id = i.get_id() as u64;
                        break;
                    }
                    Ok((person_id, company_internal_id, work_from, country_internal_id))
                })?
                .filter_map(move |(person_id, company_internal_id, work_from, country_internal_id)| {
                    let country_vertex = super::graph::GRAPH
                        .get_vertex(country_internal_id as DefaultId)
                        .unwrap();
                    let country_name = country_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    if country_name == input_country_name {
                        Ok(Some((person_id, company_internal_id, work_from)))
                    } else {
                        Ok(None)
                    }
                })?
                .map(|(person_id, company_internal_id, work_from)| {
                    let company_vertex = super::graph::GRAPH
                        .get_vertex(company_internal_id as DefaultId)
                        .unwrap();
                    let company_name = company_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((person_id, work_from, company_name))
                })?
                .sort_limit_by(10, |x, y| {
                    x.1.cmp(&y.1)
                        .then(x.0.cmp(&y.0).then(x.2.cmp(&y.2).reverse()))
                })?
                .map(|(person_id, work_from, company_name)| {
                    let person_vertex = super::graph::GRAPH
                        .get_vertex(person_id as DefaultId)
                        .unwrap();
                    let id = person_vertex
                        .get_property("id")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let first_name = person_vertex
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let last_name = person_vertex
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((id, first_name, last_name, company_name, work_from))
                })?
                .sink_into(output)
        }
    })
    .expect("submit ic11 job failure")
}