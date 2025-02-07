use std::collections::HashSet;
use std::fs::{create_dir_all, read_dir, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use bmcsr::bmcsr::{BatchMutableCsr, BatchMutableCsrBuilder};
use bmcsr::bmscsr::{BatchMutableSingleCsr, BatchMutableSingleCsrBuilder};
use bmcsr::col_table::{parse_properties, ColTable};
use bmcsr::columns::*;
use bmcsr::columns::{DataType, Item};
use bmcsr::csr::CsrTrait;
use bmcsr::date::Date;
use bmcsr::date_time::DateTime;
use bmcsr::error::{GDBError, GDBResult};
use bmcsr::graph::IndexType;
use bmcsr::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use bmcsr::schema::{CsrGraphSchema, InputSchema, Schema};
use bmcsr::types::{DefaultId, InternalId, LabelId, DIR_BINARY_DATA};
use bmcsr::vertex_map::VertexMap;
use csv::{Reader, ReaderBuilder, StringRecord};
use rayon::prelude::*;
use regex::Regex;
use rust_htslib::bgzf::Reader as GzReader;
use shm_container::{SharedStringVec, SharedVec};

use crate::indexer::Indexer;

pub fn get_files_list(prefix: &PathBuf, file_strings: &Vec<String>) -> GDBResult<Vec<PathBuf>> {
    let mut path_lists = vec![];
    for file_string in file_strings {
        let temp_path = PathBuf::from(prefix.to_string_lossy().to_string() + "/" + file_string);
        let filename = temp_path
            .file_name()
            .ok_or(GDBError::UnknownError)?
            .to_str()
            .ok_or(GDBError::UnknownError)?;
        if filename.contains("*") {
            let re_string = "^".to_owned() + &filename.replace(".", "\\.").replace("*", ".*") + "$";
            let re = Regex::new(&re_string).unwrap();
            let parent_dir = temp_path.parent().unwrap();
            for _entry in read_dir(parent_dir)? {
                let entry = _entry?;
                let path = entry.path();
                let fname = path
                    .file_name()
                    .ok_or(GDBError::UnknownError)?
                    .to_str()
                    .ok_or(GDBError::UnknownError)?;
                if re.is_match(fname) {
                    path_lists.push(path);
                }
            }
        } else {
            path_lists.push(temp_path);
        }
    }
    Ok(path_lists)
}

pub(crate) fn keep_vertex(vid: usize, peers: usize, work_id: usize) -> bool {
    vid.index() % peers == work_id
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
                println!("dumping to {}, list size {}", col_path.as_str(), casted_col.list.len());
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

fn dump_csr_global_id(
    prefix: &str, csr: &BatchMutableCsr<usize>, vertex_map: &VertexMap<usize, usize>, nbr_label: LabelId,
) {
    // SharedVec::<I>::dump_vec(format!("{}_nbrs", prefix).as_str(), &csr.neighbors);
    SharedVec::<usize>::dump_vec(format!("{}_offsets", prefix).as_str(), &csr.offsets);
    SharedVec::<i32>::dump_vec(format!("{}_degree", prefix).as_str(), &csr.degree);

    let nbrs_global_id: Vec<usize> = csr
        .neighbors
        .par_iter()
        .map(|v| if let Some(vg) = vertex_map.get_global_id(nbr_label, *v) { vg } else { usize::MAX })
        .collect();
    SharedVec::<usize>::dump_vec(format!("{}_nbrs", prefix).as_str(), &nbrs_global_id);

    let tmp_vec = vec![csr.edge_num()];
    SharedVec::<usize>::dump_vec(format!("{}_meta", prefix).as_str(), &tmp_vec);
}

fn dump_scsr_global_id(
    prefix: &str, csr: &BatchMutableSingleCsr<usize>, vertex_map: &VertexMap<usize, usize>,
    nbr_label: LabelId,
) {
    // SharedVec::<I>::dump_vec(format!("{}_nbrs", prefix).as_str(), &csr.nbr_list);

    let nbrs_global_id: Vec<usize> = csr
        .nbr_list
        .par_iter()
        .map(|v| if let Some(vg) = vertex_map.get_global_id(nbr_label, *v) { vg } else { usize::MAX })
        .collect();
    SharedVec::<usize>::dump_vec(format!("{}_nbrs", prefix).as_str(), &nbrs_global_id);

    let tmp_vec = vec![csr.max_edge_offset(), csr.edge_num(), csr.vertex_capacity];
    SharedVec::<usize>::dump_vec(format!("{}_meta", prefix).as_str(), &tmp_vec);
}

pub struct GraphLoader {
    input_dir: PathBuf,
    partition_dir: PathBuf,

    work_id: usize,
    peers: usize,
    delim: u8,
    input_schema: Arc<InputSchema>,
    graph_schema: Arc<CsrGraphSchema>,
    skip_header: bool,
    vertex_map: VertexMap<usize, usize>,
}

impl GraphLoader {
    pub fn new<D: AsRef<Path>>(
        input_dir: D, output_path: D, input_schema_file: D, graph_schema_file: D, work_id: usize,
        peers: usize,
    ) -> GraphLoader {
        let graph_schema =
            CsrGraphSchema::from_json_file(graph_schema_file).expect("Read trim schema error!");
        let input_schema = InputSchema::from_json_file(input_schema_file, &graph_schema)
            .expect("Read graph schema error!");
        graph_schema.desc();

        let vertex_label_num = graph_schema.vertex_type_to_id.len();
        let vertex_map = VertexMap::<usize, usize>::new(vertex_label_num);

        let output_dir = output_path.as_ref();
        let partition_dir = output_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", work_id));

        Self {
            input_dir: input_dir.as_ref().to_path_buf(),
            partition_dir,

            work_id,
            peers,
            delim: b'|',
            input_schema: Arc::new(input_schema),
            graph_schema: Arc::new(graph_schema),
            skip_header: false,

            vertex_map,
        }
    }

    /// For specifying a different delimiter
    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn load_vertices<R: Read>(
        &mut self, vertex_type: LabelId, mut rdr: Reader<R>, table: &mut ColTable, is_static: bool,
    ) {
        let input_header = self
            .input_schema
            .get_vertex_header(vertex_type)
            .unwrap();
        let graph_header = self
            .graph_schema
            .get_vertex_header(vertex_type)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; input_header.len()];
        let mut id_col_id = 0;
        for (index, (n, _)) in input_header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "id" {
                id_col_id = index;
            }
        }
        let parser = LDBCVertexParser::new(vertex_type, id_col_id);
        info!("loading vertex-{}", vertex_type);
        if is_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        let vertex_index = self
                            .vertex_map
                            .add_vertex(vertex_meta.global_id, vertex_meta.label);
                        if properties.len() > 0 {
                            table.insert(vertex_index.index(), &properties);
                        }
                    }
                }
            }
        } else {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if keep_vertex(vertex_meta.global_id, self.peers, self.work_id) {
                        if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice())
                        {
                            let vertex_index = self
                                .vertex_map
                                .add_vertex(vertex_meta.global_id, vertex_meta.label);
                            if properties.len() > 0 {
                                table.insert(vertex_index.index(), &properties);
                            }
                        }
                    }
                }
            }
        }
    }

    fn load_edges<R: Read>(
        &mut self, src_vertex_type: LabelId, dst_vertex_type: LabelId, edge_type: LabelId,
        is_src_static: bool, is_dst_static: bool, mut rdr: Reader<R>, idegree: &mut Vec<i32>,
        odegree: &mut Vec<i32>, parsed_edges: &mut Vec<(usize, usize, Vec<Item>)>,
    ) {
        info!("loading edge-{}-{}-{}", src_vertex_type, edge_type, dst_vertex_type);
        let input_header = self
            .input_schema
            .get_edge_header(src_vertex_type, edge_type, dst_vertex_type)
            .unwrap();
        let graph_header = self
            .graph_schema
            .get_edge_header(src_vertex_type, edge_type, dst_vertex_type)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; input_header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (name, _)) in input_header.iter().enumerate() {
            if keep_set.contains(name) {
                selected[index] = true;
            }
            if name == "start_id" {
                src_col_id = index;
            } else if name == "end_id" {
                dst_col_id = index;
            }
        }

        let src_num = self.vertex_map.vertex_num(src_vertex_type);
        let dst_num = self.vertex_map.vertex_num(dst_vertex_type);
        let mut parser = LDBCEdgeParser::<usize>::new(src_vertex_type, dst_vertex_type, edge_type);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        if is_src_static && is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        let src_lid = self
                            .vertex_map
                            .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                        if src_lid.index() < src_num {
                            odegree[src_lid.index()] += 1;
                        }
                        let dst_lid = self
                            .vertex_map
                            .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                        if dst_lid.index() < dst_num {
                            idegree[dst_lid.index()] += 1;
                        }
                        parsed_edges.push((src_lid, dst_lid, properties));
                    }
                }
            }
        } else if is_src_static && !is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                            || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                        {
                            let src_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                            if src_lid.index() < src_num {
                                odegree[src_lid.index()] += 1;
                            }
                            let dst_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                            if dst_lid.index() < dst_num {
                                idegree[dst_lid.index()] += 1;
                            }
                            parsed_edges.push((src_lid, dst_lid, properties));
                        }
                    }
                }
            }
        } else if !is_src_static && is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id) {
                            let src_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                            if src_lid.index() < src_num {
                                odegree[src_lid.index()] += 1;
                            }
                            let dst_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                            if dst_lid.index() < dst_num {
                                idegree[dst_lid.index()] += 1;
                            }
                            parsed_edges.push((src_lid, dst_lid, properties));
                        }
                    }
                }
            }
        } else {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                            || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                        {
                            let src_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                            if src_lid.index() < src_num {
                                odegree[src_lid.index()] += 1;
                            }
                            let dst_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                            if dst_lid.index() < dst_num {
                                idegree[dst_lid.index()] += 1;
                            }
                            parsed_edges.push((src_lid, dst_lid, properties));
                        }
                    }
                }
            }
        }
    }

    pub fn load(&mut self) -> GDBResult<()> {
        create_dir_all(&self.partition_dir)?;

        let v_label_num = self.graph_schema.vertex_type_to_id.len() as LabelId;
        for v_label_i in 0..v_label_num {
            let cols = self
                .graph_schema
                .get_vertex_header(v_label_i)
                .unwrap();
            let mut header = vec![];
            for pair in cols.iter() {
                header.push((pair.1.clone(), pair.0.clone()));
            }
            let mut table = ColTable::new(header);
            let vertex_file_strings = self
                .input_schema
                .get_vertex_file(v_label_i)
                .unwrap();
            let vertex_files = get_files_list(&self.input_dir, vertex_file_strings).unwrap();

            for vertex_file in vertex_files.iter() {
                if vertex_file
                    .clone()
                    .to_str()
                    .unwrap()
                    .ends_with(".csv")
                {
                    let rdr = ReaderBuilder::new()
                        .delimiter(self.delim)
                        .buffer_capacity(4096)
                        .comment(Some(b'#'))
                        .flexible(true)
                        .has_headers(self.skip_header)
                        .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                    self.load_vertices(
                        v_label_i,
                        rdr,
                        &mut table,
                        self.graph_schema.is_static_vertex(v_label_i),
                    );
                } else if vertex_file
                    .clone()
                    .to_str()
                    .unwrap()
                    .ends_with(".csv.gz")
                {
                    let rdr = ReaderBuilder::new()
                        .delimiter(self.delim)
                        .buffer_capacity(4096)
                        .comment(Some(b'#'))
                        .flexible(true)
                        .has_headers(self.skip_header)
                        .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                    self.load_vertices(
                        v_label_i,
                        rdr,
                        &mut table,
                        self.graph_schema.is_static_vertex(v_label_i),
                    );
                }
            }
            let vp_prefix = format!("{}/vp_{}", self.partition_dir.to_str().unwrap(), v_label_i as usize);
            dump_table(&vp_prefix, &table);
        }

        let e_label_num = self.graph_schema.edge_type_to_id.len() as LabelId;
        for e_label_i in 0..e_label_num {
            let edge_label_name = self.graph_schema.edge_label_names()[e_label_i as usize].clone();

            for src_label_i in 0..v_label_num {
                for dst_label_i in 0..v_label_num {
                    let src_num = self.vertex_map.vertex_num(src_label_i);
                    let dst_num = self.vertex_map.vertex_num(dst_label_i);
                    let mut idegree = vec![0_i32; dst_num as usize];
                    let mut odegree = vec![0_i32; src_num as usize];
                    let mut parsed_edges = vec![];

                    if let Some(edge_file_strings) =
                        self.input_schema
                            .get_edge_file(src_label_i, e_label_i, dst_label_i)
                    {
                        for i in edge_file_strings {
                            info!("{}", i);
                        }
                        let edge_files = get_files_list(&self.input_dir, edge_file_strings).unwrap();
                        for edge_file in edge_files.iter() {
                            info!("reading from file: {}", edge_file.clone().to_str().unwrap());
                            if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv")
                            {
                                let rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                self.load_edges(
                                    src_label_i,
                                    dst_label_i,
                                    e_label_i,
                                    self.graph_schema.is_static_vertex(src_label_i),
                                    self.graph_schema.is_static_vertex(dst_label_i),
                                    rdr,
                                    &mut idegree,
                                    &mut odegree,
                                    &mut parsed_edges,
                                );
                            } else if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv.gz")
                            {
                                let rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(GzReader::from_path(&edge_file).unwrap()));
                                self.load_edges(
                                    src_label_i,
                                    dst_label_i,
                                    e_label_i,
                                    self.graph_schema.is_static_vertex(src_label_i),
                                    self.graph_schema.is_static_vertex(dst_label_i),
                                    rdr,
                                    &mut idegree,
                                    &mut odegree,
                                    &mut parsed_edges,
                                );
                            }
                        }
                    }
                    if parsed_edges.is_empty() {
                        continue;
                    }
                    let src_label_name =
                        self.graph_schema.vertex_label_names()[src_label_i as usize].clone();
                    let dst_label_name =
                        self.graph_schema.vertex_label_names()[dst_label_i as usize].clone();
                    let cols = self
                        .graph_schema
                        .get_edge_header(src_label_i, e_label_i, dst_label_i)
                        .unwrap();
                    let mut header = vec![];
                    for pair in cols.iter() {
                        header.push((pair.1.clone(), pair.0.clone()));
                    }
                    let mut ie_edge_properties = ColTable::new(header.clone());
                    let mut oe_edge_properties = ColTable::new(header.clone());
                    if self
                        .graph_schema
                        .is_single_ie(src_label_i, e_label_i, dst_label_i)
                    {
                        let mut ie_csr_builder = BatchMutableSingleCsrBuilder::<usize>::new();
                        let mut oe_csr_builder = BatchMutableCsrBuilder::<usize>::new();
                        ie_csr_builder.init(&idegree, 1.2);
                        oe_csr_builder.init(&odegree, 1.2);
                        for e in parsed_edges.iter() {
                            let ie_offset = ie_csr_builder.put_edge(e.1, e.0).unwrap();
                            let oe_offset = oe_csr_builder.put_edge(e.0, e.1).unwrap();
                            if e.2.len() > 0 {
                                if ie_offset != usize::MAX {
                                    ie_edge_properties.insert(ie_offset, &e.2);
                                }
                                if oe_offset != usize::MAX {
                                    oe_edge_properties.insert(oe_offset, &e.2);
                                }
                            }
                        }

                        let ie_csr = ie_csr_builder.finish().unwrap();
                        let oe_csr = oe_csr_builder.finish().unwrap();

                        info!("start export ie");
                        dump_scsr_global_id(
                            format!(
                                "{}/ie_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &ie_csr
                                .as_any()
                                .downcast_ref::<BatchMutableSingleCsr<usize>>()
                                .unwrap(),
                            &self.vertex_map,
                            src_label_i as LabelId,
                        );
                        info!("start export oe");
                        dump_csr_global_id(
                            format!(
                                "{}/oe_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &oe_csr
                                .as_any()
                                .downcast_ref::<BatchMutableCsr<usize>>()
                                .unwrap(),
                            &self.vertex_map,
                            dst_label_i as LabelId,
                        );
                        info!("finished export");
                    } else if self
                        .graph_schema
                        .is_single_oe(src_label_i, e_label_i, dst_label_i)
                    {
                        let mut ie_csr_builder = BatchMutableCsrBuilder::<usize>::new();
                        let mut oe_csr_builder = BatchMutableSingleCsrBuilder::<usize>::new();
                        ie_csr_builder.init(&idegree, 1.2);
                        oe_csr_builder.init(&odegree, 1.2);
                        for e in parsed_edges.iter() {
                            let ie_offset = ie_csr_builder.put_edge(e.1, e.0).unwrap();
                            let oe_offset = oe_csr_builder.put_edge(e.0, e.1).unwrap();
                            if e.2.len() > 0 {
                                if ie_offset != usize::MAX {
                                    ie_edge_properties.insert(ie_offset, &e.2);
                                }
                                if oe_offset != usize::MAX {
                                    oe_edge_properties.insert(oe_offset, &e.2);
                                }
                            }
                        }

                        let ie_csr = ie_csr_builder.finish().unwrap();
                        let oe_csr = oe_csr_builder.finish().unwrap();

                        info!("start export ie");
                        dump_csr_global_id(
                            format!(
                                "{}/ie_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &ie_csr
                                .as_any()
                                .downcast_ref::<BatchMutableCsr<usize>>()
                                .unwrap(),
                            &self.vertex_map,
                            src_label_i as LabelId,
                        );
                        info!("start export oe");
                        dump_scsr_global_id(
                            format!(
                                "{}/oe_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &oe_csr
                                .as_any()
                                .downcast_ref::<BatchMutableSingleCsr<usize>>()
                                .unwrap(),
                            &self.vertex_map,
                            dst_label_i as LabelId,
                        );
                        info!("finished export");
                    } else {
                        let mut ie_csr_builder = BatchMutableCsrBuilder::<usize>::new();
                        let mut oe_csr_builder = BatchMutableCsrBuilder::<usize>::new();
                        ie_csr_builder.init(&idegree, 1.2);
                        oe_csr_builder.init(&odegree, 1.2);
                        for e in parsed_edges.iter() {
                            let ie_offset = ie_csr_builder.put_edge(e.1, e.0).unwrap();
                            let oe_offset = oe_csr_builder.put_edge(e.0, e.1).unwrap();
                            if e.2.len() > 0 {
                                if ie_offset != usize::MAX {
                                    ie_edge_properties.insert(ie_offset, &e.2);
                                }
                                if oe_offset != usize::MAX {
                                    oe_edge_properties.insert(oe_offset, &e.2);
                                }
                            }
                        }

                        let ie_csr = ie_csr_builder.finish().unwrap();
                        let oe_csr = oe_csr_builder.finish().unwrap();

                        info!("start export ie, edge size {}", ie_csr.edge_num());
                        dump_csr_global_id(
                            format!(
                                "{}/ie_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &ie_csr
                                .as_any()
                                .downcast_ref::<BatchMutableCsr<usize>>()
                                .unwrap(),
                            &self.vertex_map,
                            src_label_i as LabelId,
                        );
                        dump_csr_global_id(
                            format!(
                                "{}/oe_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &oe_csr
                                .as_any()
                                .downcast_ref::<BatchMutableCsr<usize>>()
                                .unwrap(),
                            &self.vertex_map,
                            dst_label_i as LabelId,
                        );
                        info!("finished export");
                    }
                    if oe_edge_properties.row_num() > 0 {
                        dump_table(
                            format!(
                                "{}/oep_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &oe_edge_properties,
                        );
                    }
                    if ie_edge_properties.row_num() > 0 {
                        dump_table(
                            format!(
                                "{}/iep_{}_{}_{}",
                                self.partition_dir.to_str().unwrap(),
                                src_label_i,
                                e_label_i,
                                dst_label_i
                            )
                            .as_str(),
                            &ie_edge_properties,
                        );
                    }
                }
            }
        }

        let vertex_label_num = self.vertex_map.label_num();
        for vl in 0..vertex_label_num {
            println!("start dump vm: {}", vl);
            let vm_bin_path = format!("{}/vm_{}", self.partition_dir.to_str().unwrap(), vl as usize);
            Indexer::dump(vm_bin_path.as_str(), &self.vertex_map.index_to_global_id[vl as usize]);

            let mut native_tomb = vec![0_u8; self.vertex_map.index_to_global_id[vl as usize].len()];
            SharedVec::<u8>::dump_vec(
                format!("{}/vm_tomb_{}", self.partition_dir.to_str().unwrap(), vl).as_str(),
                &native_tomb,
            );
        }

        Ok(())
    }
}
