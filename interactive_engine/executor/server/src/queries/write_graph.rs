use std::collections::HashSet;
use std::str::FromStr;

use bmcsr::bmscsr::BatchMutableSingleCsr;
use bmcsr::col_table::ColTable;
use bmcsr::columns::{DataType, Item};
use bmcsr::csr::CsrTrait;
use bmcsr::graph::{Direction, IndexType};
use bmcsr::graph_db::GraphDB;
use bmcsr::schema::{CsrGraphSchema, InputSchema, Schema};
use bmcsr::types::{DefaultId, LabelId};
use graph_index::types::ArrayData;

fn properties_to_items<G, I>(properties: Vec<ArrayData>) -> Vec<Vec<Item>>
where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let properties_len = if properties.len() > 0 { properties[0].len() } else { 0 };
    let mut properties_items = Vec::with_capacity(properties_len);
    for i in 0..properties_len {
        let mut property = vec![];
        for data in properties.iter() {
            property.push(data.get_item(i));
        }
        properties_items.push(property);
    }
    properties_items
}

pub fn insert_vertices<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, global_ids: Vec<G>,
    properties: Option<Vec<ArrayData>>,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    if let Some(vertex_header) = graph
        .graph_schema
        .get_vertex_header(vertex_label)
    {
        if let Some(properties) = properties {
            if properties.len() != vertex_header.len() {}
            let properties_item = properties_to_items::<usize, usize>(properties);
            for (i, property) in properties_item.into_iter().enumerate() {
                graph.insert_vertex(vertex_label, global_ids[i], Some(property));
            }
        } else {
            if vertex_header.len() != 0 {}
            for i in 0..global_ids.len() {
                graph.insert_vertex(vertex_label, global_ids[i], None);
            }
        }
    }
}

pub fn insert_edges<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    edges: Vec<(G, G)>, properties: Option<Vec<ArrayData>>, parallel: u32
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut lids = vec![];
    for (src_gid, dst_gid) in edges {
        lids.push((graph.get_internal_id(src_gid), graph.get_internal_id(dst_gid)));
    }
    if let Some(edge_header) = graph
        .graph_schema
        .get_edge_header(src_label, edge_label, dst_label)
    {
        let index = graph.edge_label_to_index(src_label, edge_label, dst_label, Direction::Outgoing);
        let mut ie_csr = std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
        let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
        let mut oe_csr = std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
        let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
        if let Some(properties) = properties {
            if properties.len() != edge_header.len() {}
            let mut col_header = vec![];
            for (col_name, data_type) in edge_header {
                col_header.push((data_type.clone(), col_name.clone()));
            }
            let mut col_table = ColTable::new(col_header);
            let mut properties_item = properties_to_items::<usize, usize>(properties);
            for items in properties_item.drain(..) {
                col_table.push(&items);
            }
            let new_src_num = graph.vertex_map.vertex_num(src_label);
            oe_prop = if let Some(old_table) = oe_prop.take() {
                Some(oe_csr.insert_edges_with_prop(
                    new_src_num,
                    &lids,
                    &col_table,
                    false,
                    parallel,
                    old_table,
                ))
            } else {
                oe_csr.insert_edges(new_src_num, &lids, false, parallel);
                None
            };
            let new_dst_num = graph.vertex_map.vertex_num(dst_label);
            ie_prop = if let Some(old_table) = ie_prop.take() {
                Some(ie_csr.insert_edges_with_prop(
                    new_dst_num,
                    &lids,
                    &col_table,
                    true,
                    parallel,
                    old_table,
                ))
            } else {
                ie_csr
                    .insert_edges(new_dst_num, &lids, true, parallel);
                None
            };
        } else {
            if edge_header.len() != 0 {}
            let new_src_num = graph.vertex_map.vertex_num(src_label);
            oe_csr.insert_edges(new_src_num, &lids, false, parallel);
            oe_prop = None;
            let new_dst_num = graph.vertex_map.vertex_num(dst_label);
            ie_csr
                .insert_edges(new_dst_num, &lids, true, parallel);
            ie_prop = None;
        }
        graph.ie[index] = ie_csr;
        if let Some(table) = ie_prop {
            graph.ie_edge_prop_table.insert(index, table);
        }
        graph.oe[index] = oe_csr;
        if let Some(table) = oe_prop {
            graph.oe_edge_prop_table.insert(index, table);
        }
    }
}

pub fn delete_vertices<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, global_ids: Vec<G>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut lids = HashSet::new();
    for v in global_ids.iter() {
        lids.insert(graph.get_internal_id(*v));
    }
    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;
    for e_label_i in 0..edge_label_num {
        for src_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(src_label_i as LabelId, e_label_i as LabelId, vertex_label as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                src_label_i as LabelId,
                vertex_label as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            let mut ie_csr =
                std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
            let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
            let mut oe_csr =
                std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
            let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
            let mut ie_to_delete = Vec::new();
            for v in lids.iter() {
                if let Some(ie_list) = ie_csr.get_edges(*v) {
                    for e in ie_list {
                        ie_to_delete.push((*e, *v));
                    }
                }
            }
            ie_csr.delete_vertices(&lids);
            if let Some(table) = oe_prop.as_mut() {
                oe_csr.parallel_delete_edges_with_props(&ie_to_delete, false, table, parallel);
            } else {
                oe_csr.parallel_delete_edges(&ie_to_delete, false, parallel);
            }
            graph.ie[index] = ie_csr;
            if let Some(table) = ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = oe_csr;
            if let Some(table) = oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
        for dst_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(vertex_label as LabelId, e_label_i as LabelId, dst_label_i as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                vertex_label as LabelId,
                dst_label_i as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            let mut ie_csr =
                std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
            let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
            let mut oe_csr =
                std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
            let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
            let mut oe_to_delete = Vec::new();
            for v in lids.iter() {
                if let Some(oe_list) = oe_csr.get_edges(*v) {
                    for e in oe_list {
                        oe_to_delete.push((*v, *e));
                    }
                }
            }
            oe_csr.delete_vertices(&lids);
            if let Some(table) = ie_prop.as_mut() {
                ie_csr.parallel_delete_edges_with_props(&oe_to_delete, true, table, parallel);
            } else {
                ie_csr.parallel_delete_edges(&oe_to_delete, true, parallel);
            }
            graph.ie[index] = ie_csr;
            if let Some(table) = ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = oe_csr;
            if let Some(table) = oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
    }

    // delete vertices
    for v in lids.iter() {
        graph.vertex_map.remove_vertex(vertex_label, v);
    }
}

pub fn delete_edges<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    global_ids: Vec<(G, G)>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut lids = vec![];
    for (src_gid, dst_gid) in global_ids.iter() {
        lids.push((graph.get_internal_id(*src_gid), graph.get_internal_id(*dst_gid)));
    }
    let index = graph.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
    let mut ie_csr = std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
    let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
    let mut oe_csr = std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
    let mut oe_prop = graph.oe_edge_prop_table.remove(&index);

    // delete edges
    if let Some(table) = oe_prop.as_mut() {
        oe_csr.parallel_delete_edges_with_props(&lids, false, table, parallel);
    }
    if let Some(table) = ie_prop.as_mut() {
        ie_csr.parallel_delete_edges_with_props(&lids, false, table, parallel);
    }

    // apply delete edges
    graph.ie[index] = ie_csr;
    if let Some(table) = ie_prop {
        graph.ie_edge_prop_table.insert(index, table);
    }
    graph.oe[index] = oe_csr;
    if let Some(table) = oe_prop {
        graph.oe_edge_prop_table.insert(index, table);
    }
}

// fn set_csrs<G, I>(graph: &mut GraphDB<G, I>, mut reps: Vec<CsrRep<I>>)
//     where
//         I: Send + Sync + IndexType,
//         G: FromStr + Send + Sync + IndexType + Eq,
// {
//     for result in reps.drain(..) {
//         let index = graph.edge_label_to_index(
//             result.src_label,
//             result.dst_label,
//             result.edge_label,
//             Direction::Outgoing,
//         );
//
//         graph.ie[index] = result.ie_csr;
//         if let Some(table) = result.ie_prop {
//             graph.ie_edge_prop_table.insert(index, table);
//         }
//         graph.oe[index] = result.oe_csr;
//         if let Some(table) = result.oe_prop {
//             graph.oe_edge_prop_table.insert(index, table);
//         }
//     }
// }
