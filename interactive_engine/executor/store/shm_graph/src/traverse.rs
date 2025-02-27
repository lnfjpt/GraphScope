use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;

use crate::columns::DataType;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::ldbc_parser::LDBCVertexParser;
use crate::schema::Schema;
use crate::sub_graph::{SingleSubGraph, SubGraph};
use crate::types::LabelId;
use crate::vertex_map::VertexMap;

fn output_vertices<G, I>(graph: &GraphDB<G, I>, output_dir: &str)
    where
        G: Send + Sync + IndexType,
        I: Send + Sync + IndexType,
{
    let vertex_label_names = graph.graph_schema.vertex_label_names();
    let output_dir_path = PathBuf::from_str(output_dir).unwrap();
    for n in vertex_label_names.iter() {
        if let Some(v_label) = graph
            .graph_schema
            .get_vertex_label_id(n.as_str())
        {
            println!("outputing vertex-{}, size {}", n, graph.get_vertices_num(v_label));
            let header = graph
                .graph_schema
                .get_vertex_header(v_label)
                .unwrap();

            let mut file = File::create(output_dir_path.join(n.as_str().to_string() + ".csv")).unwrap();
            let mut writer = BufWriter::new(file);
            write!(writer, "id").unwrap();
            for (file_name, _, _) in header.iter() {
                write!(writer, "|{}", file_name).unwrap();
            }
            writeln!(writer).unwrap();

            for v in graph.get_all_vertices(v_label) {
                if graph
                    .vertex_map
                    .is_valid_native_vertex(v_label, v.get_index())
                {
                    if let Some(global_id) = graph
                        .vertex_map
                        .get_global_id(v_label, v.get_index())
                    {
                        if global_id != <G as IndexType>::max() {
                            let id = LDBCVertexParser::<G>::get_original_id(global_id);
                            write!(writer, "{}", id.index()).unwrap();
                            for c in header {
                                if c.1 != DataType::ID {
                                    write!(
                                        writer,
                                        "|{}",
                                        graph.vertex_prop_table[v_label as usize]
                                            .get_item(c.0.as_str(), v.get_index().index())
                                            .expect(
                                                format!(
                                                    "label - {} property - {} vertex - {} not found",
                                                    v_label as usize,
                                                    c.0.as_str(),
                                                    v.get_index().index(),
                                                )
                                                    .as_str()
                                            )
                                            .to_string()
                                    )
                                        .unwrap();
                                }
                            }
                            writeln!(writer).unwrap();
                        }
                    }
                }
            }
        }
    }
}

fn output_sub_graph<G, I>(csr: &SubGraph<'_, G, I>, vm: &VertexMap<G, I>, file: &mut BufWriter<File>, has_corner: bool)
    where
        G: Send + Sync + IndexType,
        I: Send + Sync + IndexType,
{
    let vertex_num = csr.get_vertex_num().index();
    for src in 0..vertex_num {
        let src = I::new(src);
        if let Some(edges) = csr.get_adj_list(src) {
            for e in edges {
                if has_corner {
                    writeln!(
                        file,
                        "{}|{}",
                        LDBCVertexParser::<G>::get_original_id(vm.get_global_id(csr.src_label, src).unwrap())
                            .index(),
                        LDBCVertexParser::<G>::get_original_id(vm.get_global_id(csr.dst_label, e).unwrap())
                            .index(),
                    )
                        .unwrap();
                } else {
                    writeln!(
                        file,
                        "{}|{}",
                        LDBCVertexParser::<G>::get_original_id(vm.get_global_id(csr.src_label, src).unwrap())
                            .index(),
                        LDBCVertexParser::<I>::get_original_id(e).index()
                    )
                        .unwrap();
                }
            }
        }
    }
}

fn output_single_sub_graph<G, I>(
    csr: &SingleSubGraph<'_, G, I>, vm: &VertexMap<G, I>, file: &mut BufWriter<File>, has_corner: bool,
) where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    let vertex_num = csr.get_vertex_num().index();
    for src in 0..vertex_num {
        let src = I::new(src);
        if let Some(edges) = csr.get_adj_list(src) {
            for e in edges {
                if has_corner {
                    writeln!(
                        file,
                        "{}|{}",
                        LDBCVertexParser::<G>::get_original_id(vm.get_global_id(csr.src_label, src).unwrap())
                            .index(),
                        LDBCVertexParser::<G>::get_original_id(vm.get_global_id(csr.dst_label, e).unwrap())
                            .index(),
                    )
                        .unwrap();
                } else {
                    writeln!(
                        file,
                        "{}|{}",
                        LDBCVertexParser::<G>::get_original_id(vm.get_global_id(csr.src_label, src).unwrap())
                            .index(),
                        LDBCVertexParser::<I>::get_original_id(e).index()
                    )
                        .unwrap();
                }
            }
        }
    }
}

fn output_edges<G, I>(graph: &GraphDB<G, I>, output_dir: &str, has_corner: bool)
    where
        G: Send + Sync + IndexType,
        I: Send + Sync + IndexType,
{
    let vertex_label_names = graph.graph_schema.vertex_label_names();
    let edge_label_names = graph.graph_schema.edge_label_names();
    let output_dir_path = PathBuf::from_str(output_dir).unwrap();

    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;

    for src_label in 0..vertex_label_num {
        for dst_label in 0..vertex_label_num {
            for edge_label in 0..edge_label_num {
                if let Some(_) = graph.graph_schema.get_edge_header(
                    src_label as LabelId,
                    edge_label as LabelId,
                    dst_label as LabelId,
                ) {
                    let oe_filename = format!(
                        "oe_{}_{}_{}",
                        vertex_label_names[src_label].as_str(),
                        edge_label_names[edge_label].as_str(),
                        vertex_label_names[dst_label].as_str()
                    );
                    let mut oe_file =
                        File::create(output_dir_path.join(oe_filename.clone() + ".csv")).unwrap();
                    let mut oe_writer = BufWriter::new(oe_file);

                    writeln!(oe_writer, "src|dst").unwrap();

                    if graph.graph_schema.is_single_oe(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let oe = graph.get_single_sub_graph(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                            Direction::Outgoing,
                        );
                        output_single_sub_graph(&oe, &graph.vertex_map, &mut oe_writer, has_corner);
                    } else {
                        let oe = graph.get_sub_graph(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                            Direction::Outgoing,
                        );
                        output_sub_graph(&oe, &graph.vertex_map, &mut oe_writer, has_corner);
                    }

                    let ie_filename = format!(
                        "ie_{}_{}_{}",
                        vertex_label_names[src_label].as_str(),
                        edge_label_names[edge_label].as_str(),
                        vertex_label_names[dst_label].as_str()
                    );
                    let mut ie_file =
                        File::create(output_dir_path.join(ie_filename.clone() + ".csv")).unwrap();
                    let mut ie_writer = BufWriter::new(ie_file);
                    writeln!(ie_writer, "dst|src").unwrap();
                    if graph.graph_schema.is_single_ie(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let ie = graph.get_single_sub_graph(
                            dst_label as LabelId,
                            edge_label as LabelId,
                            src_label as LabelId,
                            Direction::Incoming,
                        );
                        output_single_sub_graph(&ie, &graph.vertex_map, &mut ie_writer, has_corner);
                    } else {
                        let ie = graph.get_sub_graph(
                            dst_label as LabelId,
                            edge_label as LabelId,
                            src_label as LabelId,
                            Direction::Incoming,
                        );
                        output_sub_graph(&ie, &graph.vertex_map, &mut ie_writer, has_corner);
                    }
                }
            }
        }
    }
}

pub fn traverse<G, I>(db: &GraphDB<G, I>, output_dir: &str, has_corner: bool)
    where
        G: Send + Sync + IndexType,
        I: Send + Sync + IndexType,
{
    output_vertices(db, output_dir);
    output_edges(db, output_dir, has_corner);
}
