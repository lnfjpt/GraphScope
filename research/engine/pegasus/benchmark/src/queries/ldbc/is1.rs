use graph_store::prelude::*;
use pegasus::api::{Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

static LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

pub fn is1(
    conf: JobConf, person_id: u64,
) -> ResultStream<(String, String, u64, String, String, i32, String, u64)> {
    pegasus::run(conf, || {
        let worker_id = pegasus::get_current_worker().index;
        let start = if worker_id == 0 { Some(vec![person_id]) } else { None };
        move |input, output| {
            input
                .input_from(start)?
                .map(move |source| {
                    let vertex_id = source[0] as DefaultId;
                    let internal_id = ((1 as usize) << LABEL_SHIFT_BITS) | vertex_id;
                    let lv = super::graph::GRAPH
                        .get_vertex(internal_id)
                        .unwrap();
                    let first_name = lv
                        .get_property("firstName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let second_name = lv
                        .get_property("lastName")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let birthday = lv
                        .get_property("birthday")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    let location_ip = lv
                        .get_property("locationIP")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let browser_used = lv
                        .get_property("browserUsed")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let located = super::graph::GRAPH.get_out_vertices(internal_id, Some(&vec![0]));
                    let mut city_id = 0;
                    for v in located {
                        city_id = v.get_property("id").unwrap().as_i32().unwrap();
                        break;
                    }
                    let gender = lv
                        .get_property("gender")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    let creation_date = lv
                        .get_property("creationDate")
                        .unwrap()
                        .as_u64()
                        .unwrap();
                    Ok((
                        first_name,
                        second_name,
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
