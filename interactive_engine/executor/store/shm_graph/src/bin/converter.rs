use std::fmt::format;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use bmcsr::col_table::ColTable;
use bmcsr::columns::*;
use bmcsr::csr::CsrTrait;
use bmcsr::graph::IndexType;
use bmcsr::schema::Schema;
use clap::{App, Arg};

use bmcsr::bmcsr::BatchMutableCsr;
use bmcsr::bmscsr::BatchMutableSingleCsr;
use bmcsr::date::Date;
use bmcsr::date_time::DateTime;
use bmcsr::graph_db::GraphDB;
use bmcsr::types::{DefaultId, LabelId, DIR_BINARY_DATA, NAME, VERSION};
use shm_graph::indexer::Indexer;
use shm_graph::vector::{SharedStringVec, SharedVec};

fn get_partition_num(graph_data_dir: &String) -> usize {
    let root_dir = PathBuf::from_str(graph_data_dir.as_str()).unwrap();
    let partitions_dir = root_dir.join(DIR_BINARY_DATA);
    let mut index = 0_usize;
    loop {
        let partition_dir = partitions_dir.join(format!("partition_{}", index));
        let b = Path::new(partition_dir.to_str().unwrap()).is_dir();
        if b {
            index += 1;
        } else {
            return index;
        }
    }
}

fn dump_csr<I: IndexType>(prefix: &str, csr: &BatchMutableCsr<I>) {
    SharedVec::<I>::dump_vec(format!("{}_nbrs", prefix).as_str(), &csr.neighbors);
    SharedVec::<usize>::dump_vec(format!("{}_offsets", prefix).as_str(), &csr.offsets);
    SharedVec::<i32>::dump_vec(format!("{}_degree", prefix).as_str(), &csr.degree);

    let tmp_vec = vec![csr.edge_num()];
    SharedVec::<usize>::dump_vec(format!("{}_meta", prefix).as_str(), &tmp_vec);
}

fn dump_scsr<I: IndexType>(prefix: &str, csr: &BatchMutableSingleCsr<I>) {
    SharedVec::<I>::dump_vec(format!("{}_nbrs", prefix).as_str(), &csr.nbr_list);

    let tmp_vec = vec![csr.max_edge_offset(), csr.edge_num(), csr.vertex_capacity];
    SharedVec::<usize>::dump_vec(format!("{}_meta", prefix).as_str(), &tmp_vec);
}

fn dump_table(prefix: &str, tbl: &ColTable) {
    for i in 0..tbl.col_num() {
        let col = tbl.get_column_by_index(i);
        let col_type = col.get_type();
        let col_path = format!("{}_col_{}", prefix, i);
        match col_type {
            DataType::Int32 => {
                let data = &col
                    .as_any()
                    .downcast_ref::<Int32Column>()
                    .unwrap()
                    .data;
                SharedVec::<i32>::dump_vec(col_path.as_str(), data);
            }
            DataType::UInt32 => {
                let data = &col
                    .as_any()
                    .downcast_ref::<UInt32Column>()
                    .unwrap()
                    .data;
                SharedVec::<u32>::dump_vec(col_path.as_str(), data);
            }
            DataType::Int64 => {
                let data = &col
                    .as_any()
                    .downcast_ref::<Int64Column>()
                    .unwrap()
                    .data;
                SharedVec::<i64>::dump_vec(col_path.as_str(), data);
            }
            DataType::UInt64 => {
                let data = &col
                    .as_any()
                    .downcast_ref::<UInt64Column>()
                    .unwrap()
                    .data;
                SharedVec::<u64>::dump_vec(col_path.as_str(), data);
            }
            DataType::String => {
                let data = &col
                    .as_any()
                    .downcast_ref::<StringColumn>()
                    .unwrap()
                    .data;
                SharedStringVec::dump_vec(col_path.as_str(), data);
            }
            DataType::LCString => {
                let casted_col = col
                    .as_any()
                    .downcast_ref::<LCStringColumn>()
                    .unwrap();
                SharedVec::<u16>::dump_vec(
                    format!("{}_index", col_path.as_str()).as_str(),
                    &casted_col.data,
                );
                SharedStringVec::dump_vec(format!("{}_data", col_path.as_str()).as_str(), &casted_col.list);
            }
            DataType::Double => {
                let data = &col
                    .as_any()
                    .downcast_ref::<DoubleColumn>()
                    .unwrap()
                    .data;
                SharedVec::<f64>::dump_vec(col_path.as_str(), data);
            }
            DataType::Date => {
                let data = &col
                    .as_any()
                    .downcast_ref::<DateColumn>()
                    .unwrap()
                    .data;
                SharedVec::<Date>::dump_vec(col_path.as_str(), data);
            }
            DataType::DateTime => {
                let data = &col
                    .as_any()
                    .downcast_ref::<DateTimeColumn>()
                    .unwrap()
                    .data;
                SharedVec::<DateTime>::dump_vec(col_path.as_str(), data);
            }
            DataType::ID => {
                let data = &col
                    .as_any()
                    .downcast_ref::<IDColumn>()
                    .unwrap()
                    .data;
                SharedVec::<DefaultId>::dump_vec(col_path.as_str(), data);
            }
            DataType::NULL => {
                println!("Unexpected column type");
            }
        }
    }
}

fn convert_graph(input_dir: &String, output_dir: &String, partition: usize) {
    let graph = GraphDB::<usize, usize>::deserialize(input_dir.as_str(), partition, None).unwrap();

    let vertex_label_num = graph.vertex_label_num;
    let output_partition_dir = format!("{}/graph_data_bin/partition_{}", output_dir, partition);
    fs::create_dir_all(output_partition_dir.as_str()).unwrap();
    for vl in 0..vertex_label_num {
        let vm_bin_path = format!("{}/vm_{}", output_partition_dir, vl as usize);
        Indexer::dump(vm_bin_path.as_str(), &graph.vertex_map.index_to_global_id[vl]);

        let vmc_bin_path = format!("{}/vmc_{}", output_partition_dir, vl as usize);
        Indexer::dump(vmc_bin_path.as_str(), &graph.vertex_map.index_to_corner_global_id[vl]);

        let vp_prefix = format!("{}/vp_{}", output_partition_dir, vl as usize);
        dump_table(&vp_prefix, &graph.vertex_prop_table[vl]);
    }

    let edge_label_num = graph.edge_label_num;
    for src_label in 0..vertex_label_num {
        for edge_label in 0..edge_label_num {
            for dst_label in 0..vertex_label_num {
                if let Some(header) = graph.graph_schema.get_edge_header(
                    src_label as LabelId,
                    edge_label as LabelId,
                    dst_label as LabelId,
                ) {
                    let oe_idx = graph.edge_label_to_index(
                        src_label as LabelId,
                        dst_label as LabelId,
                        edge_label as LabelId,
                        bmcsr::graph::Direction::Outgoing,
                    );
                    if !graph.graph_schema.is_single_oe(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        dump_csr(
                            format!(
                                "{}/oe_{}_{}_{}",
                                output_partition_dir, src_label, edge_label, dst_label
                            )
                            .as_str(),
                            &graph.oe[oe_idx]
                                .as_any()
                                .downcast_ref::<BatchMutableCsr<usize>>()
                                .unwrap(),
                        );
                    } else {
                        dump_scsr(
                            format!(
                                "{}/oe_{}_{}_{}",
                                output_partition_dir, src_label, edge_label, dst_label
                            )
                            .as_str(),
                            &graph.oe[oe_idx]
                                .as_any()
                                .downcast_ref::<BatchMutableSingleCsr<usize>>()
                                .unwrap(),
                        );
                    }

                    if let Some(tbl) = graph.oe_edge_prop_table.get(&oe_idx) {
                        dump_table(
                            format!(
                                "{}/oep_{}_{}_{}",
                                output_partition_dir, src_label, edge_label, dst_label
                            )
                            .as_str(),
                            tbl,
                        );
                    }

                    let ie_idx = graph.edge_label_to_index(
                        dst_label as LabelId,
                        src_label as LabelId,
                        edge_label as LabelId,
                        bmcsr::graph::Direction::Incoming,
                    );
                    if !graph.graph_schema.is_single_ie(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        dump_csr(
                            format!(
                                "{}/ie_{}_{}_{}",
                                output_partition_dir, src_label, edge_label, dst_label
                            )
                            .as_str(),
                            &graph.ie[ie_idx]
                                .as_any()
                                .downcast_ref::<BatchMutableCsr<usize>>()
                                .unwrap(),
                        );
                    } else {
                        dump_scsr(
                            format!(
                                "{}/ie_{}_{}_{}",
                                output_partition_dir, src_label, edge_label, dst_label
                            )
                            .as_str(),
                            &graph.ie[ie_idx]
                                .as_any()
                                .downcast_ref::<BatchMutableSingleCsr<usize>>()
                                .unwrap(),
                        );
                    }

                    if let Some(tbl) = graph.ie_edge_prop_table.get(&ie_idx) {
                        dump_table(
                            format!(
                                "{}/iep_{}_{}_{}",
                                output_partition_dir, src_label, edge_label, dst_label
                            )
                            .as_str(),
                            tbl,
                        );
                    }
                }
            }
        }
    }
}

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Convert graph storage")
        .args(&[
            Arg::with_name("input_graph_data_dir")
                .short("i")
                .long_help("The directory of input graph data")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("output_graph_data_dir")
                .short("o")
                .long_help("The directory of output graph data")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("partition_id")
                .short("p")
                .long_help("The partition of output graph data")
                .required(true)
                .takes_value(true)
                .index(3),
        ])
        .get_matches();

    let input_dir = matches
        .value_of("input_graph_data_dir")
        .unwrap()
        .to_string();
    let output_dir = matches
        .value_of("output_graph_data_dir")
        .unwrap()
        .to_string();
    let partition_id: usize = matches
        .value_of("partition_id")
        .unwrap()
        .to_string()
        .parse()
        .unwrap();

    convert_graph(&input_dir, &output_dir, partition_id);
}
