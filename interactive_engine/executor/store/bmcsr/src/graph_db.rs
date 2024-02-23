use std::any::Any;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use chrono::format::parse;
use itertools::Itertools;

use crate::graph::{IndexType, Direction};
use crate::sub_graph::{SubGraph, SingleSubGraph};
use crate::schema::{CsrGraphSchema, Schema};
use crate::vertex_map::VertexMap;
use crate::edge_trim::EdgeTrimJson;
use crate::types::*;
use crate::bmcsr::{BatchMutableCsr, BatchMutableCsrBuilder};
use crate::bmscsr::BatchMutableSingleCsr;
use crate::csr::CsrTrait;
use crate::col_table::ColTable;
use crate::columns::{RefItem, Item, DataType};
use crate::error::GDBResult;
use crate::graph::Direction::Outgoing;
use crate::utils::{Iter, LabeledIterator, LabeledRangeIterator, Range};

/// A data structure to maintain a local view of the vertex.
#[derive(Debug, Clone)]
pub struct LocalVertex<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// The vertex's global id
    index: I,
    /// The vertex's label
    label: LabelId,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    ///
    table: Option<&'a ColTable>,
    id_list: &'a Vec<G>,
    corner_id_list: &'a Vec<G>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalVertex<'a, G, I> {
    pub fn new(index: I, label: LabelId, id_list: &'a Vec<G>, corner_id_list: &'a Vec<G>) -> Self {
        LocalVertex { index, label, id_list, table: None, corner_id_list }
    }

    pub fn with_property(
        index: I, label: LabelId, id_list: &'a Vec<G>, corner_id_list: &'a Vec<G>,
        table: Option<&'a ColTable>,
    ) -> Self {
        LocalVertex { index, label, id_list, table, corner_id_list }
    }

    pub fn get_id(&self) -> G {
        let index = self.index.index();
        if index < self.id_list.len() {
            self.id_list[index]
        } else {
            self.corner_id_list[<I as IndexType>::max().index() - index - 1]
        }
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_property(&self, key: &str) -> Option<RefItem> {
        if let Some(prop) = self.table {
            prop.get_item(key, self.index.index())
        } else {
            None
        }
    }

    pub fn get_all_properties(&self) -> Option<HashMap<String, RefItem>> {
        if let Some(prop) = self.table {
            let mut property_table = HashMap::new();
            for head in prop.header.keys() {
                property_table.insert(head.clone(), prop.get_item(head, self.index.index()).unwrap());
            }
            Some(property_table)
        } else {
            None
        }
    }
}

/// A data structure to maintain a local view of the edge.
#[derive(Clone)]
pub struct LocalEdge<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// The start vertex's global id
    start: I,
    /// The end vertex's global id
    end: I,
    /// The edge label id
    label: LabelId,
    src_label: LabelId,
    dst_label: LabelId,

    offset: usize,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    table: Option<&'a ColTable>,

    vertex_map: &'a VertexMap<G, I>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalEdge<'a, G, I> {
    pub fn new(
        start: I, end: I, label: LabelId, src_label: LabelId, dst_label: LabelId,
        vertex_map: &'a VertexMap<G, I>, offset: usize, properties: Option<&'a ColTable>,
    ) -> Self {
        LocalEdge { start, end, label, src_label, dst_label, offset, table: properties, vertex_map }
    }

    pub fn get_src_id(&self) -> G {
        self.vertex_map
            .get_global_id(self.src_label, self.start)
            .unwrap()
    }

    pub fn get_dst_id(&self) -> G {
        self.vertex_map
            .get_global_id(self.dst_label, self.end)
            .unwrap()
    }

    pub fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_dst_label(&self) -> LabelId {
        self.dst_label
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_src_lid(&self) -> I {
        self.start
    }

    pub fn get_dst_lid(&self) -> I {
        self.end
    }

    pub fn get_property(&self, key: &str) -> Option<RefItem> {
        if let Some(prop) = self.table {
            prop.get_item(key, self.offset)
        } else {
            None
        }
    }

    pub fn get_all_properties(&self) -> Option<HashMap<String, RefItem>> {
        if let Some(prop) = self.table {
            let mut property_table = HashMap::new();
            for head in prop.header.keys() {
                property_table.insert(head.clone(), prop.get_item(head, self.offset).unwrap());
            }
            Some(property_table)
        } else {
            None
        }
    }
}


pub struct GraphDBModification<G: Send + Sync + IndexType = DefaultId> {
    pub delete_edges: Vec<Vec<(G, G)>>,
    pub delete_vertices: Vec<Vec<G>>,

    pub add_edges: Vec<Vec<(G, G)>>,
    pub add_edge_prop_tables: HashMap<usize, ColTable>,

    pub vertex_label_num: usize,
    pub edge_label_num: usize,
}


impl<G> GraphDBModification<G> where G: Eq + IndexType + Send + Sync, {
    pub fn new() -> Self {
        Self {
            delete_edges: vec![],
            delete_vertices: vec![],

            add_edges: vec![],
            add_edge_prop_tables: HashMap::new(),

            vertex_label_num: 0,
            edge_label_num: 0,
        }
    }

    pub fn edge_label_to_index(
        &self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId,
    ) -> usize {
        src_label as usize * self.vertex_label_num * self.edge_label_num
            + dst_label as usize * self.edge_label_num
            + edge_label as usize
    }

    pub fn edge_label_tuple_num(&self) -> usize {
        self.vertex_label_num * self.vertex_label_num * self.edge_label_num
    }

    pub fn init(&mut self, graph_schema: &CsrGraphSchema) {
        self.vertex_label_num = graph_schema.vertex_label_names().len();
        self.edge_label_num = graph_schema.edge_label_names().len();

        self.delete_edges.resize(self.edge_label_tuple_num(), vec![]);
        self.delete_vertices.resize(self.vertex_label_num, vec![]);

        for e_label_i in 0..self.edge_label_num {
            for src_label_i in 0..self.vertex_label_num {
                for dst_label_i in 0..self.vertex_label_num {
                    let index = self.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, e_label_i as LabelId);
                    let mut header = vec![];
                    for pair in graph_schema.get_edge_header(src_label_i as LabelId, dst_label_i as LabelId, e_label_i as LabelId).unwrap() {
                        header.push((pair.1.clone(), pair.0.clone()));
                    }
                    if !header.is_empty() {
                        self.add_edge_prop_tables.insert(index, ColTable::new(header));
                    }
                }
            }
        }
    }

    pub fn delete_vertex(&mut self, label: LabelId, id: G) {
        let index = label as usize;
        self.delete_vertices[index].push(id);
    }

    pub fn insert_edge(&mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, src: G, dst: G, properties: Option<Vec<Item>>) {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label);
        self.insert_edge_opt(index, src, dst, properties);
    }

    pub fn insert_edge_opt(&mut self, index: usize, src: G, dst: G, properties: Option<Vec<Item>>) {
        self.add_edges[index].push((src, dst));
        if let Some(properties) = properties {
            self.add_edge_prop_tables.get_mut(&index).unwrap().push(&properties);
        }
    }

    pub fn delete_edge(&mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, src: G, dst: G) {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label);
        self.delete_edge_opt(index, src, dst);
    }

    pub fn delete_edge_opt(&mut self, index: usize, src: G, dst: G) {
        self.delete_edges[index].push((src, dst));
    }
}

pub struct GraphDB<G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub partition: usize,
    pub ie: Vec<Box<dyn CsrTrait<I>>>,
    pub oe: Vec<Box<dyn CsrTrait<I>>>,

    pub graph_schema: Arc<CsrGraphSchema>,

    pub vertex_map: VertexMap<G, I>,

    pub vertex_prop_table: Vec<ColTable>,
    pub ie_edge_prop_table: HashMap<usize, ColTable>,
    pub oe_edge_prop_table: HashMap<usize, ColTable>,

    pub vertex_label_num: usize,
    pub edge_label_num: usize,

    pub modification: GraphDBModification<G>,
}

impl<G, I> GraphDB<G, I>
    where
        G: Eq + IndexType + Send + Sync,
        I: IndexType + Send + Sync,
{
    pub fn edge_label_to_index(
        &self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, dir: Direction,
    ) -> usize {
        match dir {
            Direction::Incoming => {
                dst_label as usize * self.vertex_label_num * self.edge_label_num
                    + src_label as usize * self.edge_label_num
                    + edge_label as usize
            }
            Direction::Outgoing => {
                src_label as usize * self.vertex_label_num * self.edge_label_num
                    + dst_label as usize * self.edge_label_num
                    + edge_label as usize
            }
        }
    }

    pub fn get_vertices_num(&self, label: LabelId) -> usize {
        self.vertex_map.vertex_num(label)
    }

    pub fn get_edges_num(&self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId) -> usize {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        self.oe[index].edge_num()
    }

    pub fn get_global_id(&self, id: I, label: LabelId) -> Option<G> {
        self.vertex_map.get_global_id(label, id)
    }

    pub fn get_internal_id(&self, id: G) -> I {
        self.vertex_map.get_internal_id(id).unwrap().1
    }

    fn index_to_local_vertex(&self, label_id: LabelId, index: I, with_property: bool) -> LocalVertex<G, I> {
        if with_property {
            LocalVertex::with_property(
                index,
                label_id,
                &self.vertex_map.index_to_global_id[label_id as usize],
                &self.vertex_map.index_to_corner_global_id[label_id as usize],
                Some(&self.vertex_prop_table[label_id as usize]),
            )
        } else {
            LocalVertex::new(
                index,
                label_id,
                &self.vertex_map.index_to_global_id[label_id as usize],
                &self.vertex_map.index_to_corner_global_id[label_id as usize],
            )
        }
    }

    pub fn get_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>> {
        if labels.is_none() {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in 0..self.vertex_label_num {
                iters.push(Range::new(I::new(0), I::new(self.get_vertices_num(v as LabelId))).into_iter());
                got_labels.push(v as LabelId)
            }
            Iter::from_iter(LabeledIterator::new(got_labels, iters).map(move |(label, index)| {
                self.index_to_local_vertex(label, index, true)
            }))
        } else if labels.unwrap().len() == 1 {
            let label = labels.unwrap()[0];
            let range = Range::new(I::new(0), I::new(self.get_vertices_num(label)));
            Iter::from_iter(range.into_iter().map(move |index| {
                self.index_to_local_vertex(label, index, true)
            }))
        } else {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in labels.unwrap() {
                iters.push(Range::new(I::new(0), I::new(self.get_vertices_num(*v))).into_iter());
                got_labels.push(*v)
            }
            Iter::from_iter(LabeledIterator::new(got_labels, iters).map(move |(label, index)| {
                self.index_to_local_vertex(label, index, true)
            }))
        }
    }

    pub fn deserialize(dir: &str, partition: usize, trim_json_path: Option<String>) -> GDBResult<Self> {
        let root_dir = PathBuf::from_str(dir).unwrap();
        let schema_path = root_dir
            .join(DIR_GRAPH_SCHEMA)
            .join(FILE_SCHEMA);
        let graph_schema = CsrGraphSchema::from_json_file(schema_path)?;
        let partition_dir = root_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", partition));

        let (ie_enable, oe_enable) = if let Some(trim_json_path) = &trim_json_path {
            let edge_trim_path = PathBuf::from_str(trim_json_path).unwrap();
            let file = File::open(edge_trim_path)?;
            let trim_json =
                serde_json::from_reader::<File, EdgeTrimJson>(file).map_err(std::io::Error::from)?;
            trim_json.get_enable_indexs(&graph_schema)
        } else {
            (HashSet::<usize>::new(), HashSet::<usize>::new())
        };

        let vertex_label_num = graph_schema.vertex_type_to_id.len();
        let edge_label_num = graph_schema.edge_type_to_id.len();

        let csr_num = vertex_label_num * vertex_label_num * edge_label_num;
        let mut ie: Vec<Box<dyn CsrTrait<I>>> = vec![];
        let mut oe: Vec<Box<dyn CsrTrait<I>>> = vec![];
        for _ in 0..csr_num {
            ie.push(Box::new(BatchMutableSingleCsr::<I>::new()));
            oe.push(Box::new(BatchMutableSingleCsr::<I>::new()));
        }

        for e_label_i in 0..edge_label_num {
            let edge_label_name = graph_schema.edge_label_names()[e_label_i].clone();
            for src_label_i in 0..vertex_label_num {
                let src_label_name = graph_schema.vertex_label_names()[src_label_i].clone();
                for dst_label_i in 0..vertex_label_num {
                    let dst_label_name = graph_schema.vertex_label_names()[dst_label_i].clone();
                    let index: usize = src_label_i * vertex_label_num * edge_label_num
                        + dst_label_i * edge_label_num
                        + e_label_i;

                    let ie_path = &partition_dir
                        .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(ie_path) && (trim_json_path.is_none() || ie_enable.contains(&index)) {
                        info!("importing {}", ie_path.as_os_str().to_str().unwrap());
                        let path_str = ie_path.to_str().unwrap().to_string();
                        if graph_schema.is_single_ie(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            let mut ie_csr = BatchMutableSingleCsr::<I>::new();
                            ie_csr.deserialize(&path_str);
                            ie[index] = Box::new(ie_csr);
                        } else {
                            let mut ie_csr = BatchMutableCsr::<I>::new();
                            ie_csr.deserialize(&path_str);
                            ie[index] = Box::new(ie_csr);
                        }
                    }

                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) && (trim_json_path.is_none() || oe_enable.contains(&index)) {
                        info!("importing {}", oe_path.as_os_str().to_str().unwrap());
                        let path_str = oe_path.to_str().unwrap().to_string();
                        if graph_schema.is_single_oe(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            let mut oe_csr = BatchMutableSingleCsr::<I>::new();
                            oe_csr.deserialize(&path_str);
                            oe[index] = Box::new(oe_csr);
                        } else {
                            let mut oe_csr = BatchMutableCsr::<I>::new();
                            oe_csr.deserialize(&path_str);
                            oe[index] = Box::new(oe_csr);
                        }
                    }
                }
            }
        }
        info!("finished import csrs");
        info!("start import vertex properties");
        let mut vertex_prop_table = vec![];
        for i in 0..vertex_label_num {
            let v_label_name = graph_schema.vertex_label_names()[i].clone();
            let mut table = ColTable::new(vec![]);
            let table_path = &partition_dir.join(format!("vp_{}", v_label_name));
            let table_path_str = table_path.to_str().unwrap().to_string();
            info!("importing vertex property: {}, {}", v_label_name, table_path_str);
            table.deserialize_table(&table_path_str);
            vertex_prop_table.push(table);
        }
        info!("finished import vertex properties");

        info!("start import edge properties");
        let mut oe_edge_prop_table = HashMap::new();
        let mut ie_edge_prop_table = HashMap::new();
        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    let edge_index = src_label_i * vertex_label_num * edge_label_num
                        + dst_label_i * edge_label_num
                        + e_label_i;
                    let src_label_name = graph_schema.vertex_label_names()[src_label_i].clone();
                    let dst_label_name = graph_schema.vertex_label_names()[dst_label_i].clone();
                    let edge_label_name = graph_schema.edge_label_names()[e_label_i].clone();

                    let oe_edge_property_path = &partition_dir
                        .join(format!("oep_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let oe_edge_property_path_str = oe_edge_property_path.to_str().unwrap().to_string();
                    if Path::new(&oe_edge_property_path_str).exists() {
                        let mut table = ColTable::new(vec![]);
                        info!(
                            "importing oe edge property: {}_{}_{}, {}",
                            src_label_name, edge_label_name, dst_label_name, oe_edge_property_path_str
                        );
                        table.deserialize_table(&oe_edge_property_path_str);
                        oe_edge_prop_table.insert(edge_index, table);
                    }

                    let ie_edge_property_path = &partition_dir
                        .join(format!("iep_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let ie_edge_property_path_str = ie_edge_property_path.to_str().unwrap().to_string();
                    if Path::new(&ie_edge_property_path_str).exists() {
                        let mut table = ColTable::new(vec![]);
                        info!(
                            "importing ie edge property: {}_{}_{}, {}",
                            src_label_name, edge_label_name, dst_label_name, oe_edge_property_path_str
                        );
                        table.deserialize_table(&ie_edge_property_path_str);
                        ie_edge_prop_table.insert(edge_index, table);
                    }
                }
            }
        }
        info!("finished import edge properties");

        let mut vertex_map = VertexMap::new(vertex_label_num);
        info!("start import native vertex map");
        let vm_path = &partition_dir.join("vm");
        let vm_path_str = vm_path.to_str().unwrap().to_string();
        vertex_map.deserialize(&vm_path_str);
        info!("finish import corner vertex map");

        let mut modification = GraphDBModification::new();
        modification.init(&graph_schema);

        Ok(Self {
            partition,
            ie,
            oe,
            graph_schema: Arc::new(graph_schema),
            vertex_prop_table,
            vertex_map,
            ie_edge_prop_table,
            oe_edge_prop_table,
            vertex_label_num,
            edge_label_num,
            modification,
        })
    }

    pub fn get_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Outgoing => SubGraph::new(
                &self.oe[index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.oe_edge_prop_table.get(&index),
            ),
            Direction::Incoming => SubGraph::new(
                &self.ie[index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.ie_edge_prop_table.get(&index),
            ),
        }
    }

    pub fn get_single_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SingleSubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Outgoing => SingleSubGraph::new(
                &self.oe[index]
                    .as_any()
                    .downcast_ref::<BatchMutableSingleCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.oe_edge_prop_table.get(&index),
            ),
            Direction::Incoming => SingleSubGraph::new(
                &self.ie[index]
                    .as_any()
                    .downcast_ref::<BatchMutableSingleCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.ie_edge_prop_table.get(&index),
            ),
        }
    }

    pub fn insert_vertex(&mut self, label: LabelId, id: G, properties: Option<Vec<Item>>) {
        let lid = self.vertex_map.add_vertex(id, label);
        if let Some(properties) = properties {
            self.vertex_prop_table[label as usize].insert(lid.index(), &properties);
        }
    }

    pub fn delete_vertex(&mut self, label: LabelId, id: G) {
        self.modification.delete_vertex(label, id);
    }

    pub fn insert_edge(&mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, src: G, dst: G, properties: Option<Vec<Item>>) {
        self.modification.insert_edge(src_label, dst_label, edge_label, src, dst, properties);
    }

    pub fn insert_edge_opt(&mut self, index: usize, src: G, dst: G, properties: Option<Vec<Item>>) {
        self.modification.insert_edge_opt(index, src, dst, properties);
    }

    pub fn delete_edge(&mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, src: G, dst: G) {
        self.modification.delete_edge(src_label, dst_label, edge_label, src, dst);
    }

    pub fn delete_edge_opt(&mut self, index: usize, src: G, dst: G) {
        self.modification.delete_edge_opt(index, src, dst);
    }

    pub fn apply_modifications_on_csr(&mut self,
                                      index: usize,
                                      modification_index: usize,
                                      cols: Vec<(String, DataType)>,
                                      new_vertex_num: usize,
                                      vertices_to_delete: &Vec<I>,
                                      delete_edges: &Vec<(I, I)>,
                                      add_edges: &Vec<(I, I)>, dir: Direction) {
        let csr = if dir == Direction::Outgoing {
            self.oe[index]
                .as_mut_any()
                .downcast_mut::<BatchMutableCsr<I>>()
                .unwrap()
        } else {
            self.ie[index]
                .as_mut_any()
                .downcast_mut::<BatchMutableCsr<I>>()
                .unwrap()
        };
        let table = if dir == Direction::Outgoing {
            self.oe_edge_prop_table.get_mut(&index)
        } else {
            self.ie_edge_prop_table.get_mut(&index)
        };
        let add_edges_table = if table.is_some() {
            self.modification.add_edge_prop_tables.get(&modification_index)
        } else {
            None
        };
        let mut new_degree = vec![];
        new_degree.resize(new_vertex_num, 0);
        let old_vertex_num = csr.vertex_num();
        for v in 0..old_vertex_num.index() {
            new_degree[v] = csr.degree(I::new(v)) as i64;
        }
        for id in vertices_to_delete {
            new_degree[id.index()] = 0;
        }
        if dir == Direction::Outgoing {
            let mut last_vertex = <I as IndexType>::max();
            let mut nbr_set = HashSet::new();
            for (src, dst) in delete_edges {
                if *src != last_vertex {
                    if !nbr_set.is_empty() && new_degree[last_vertex.index()] > 0 {
                        let mut deleted_edges = 0;
                        for offset in csr.offsets[last_vertex.index()]..csr.offsets[last_vertex.index() + 1] {
                            if nbr_set.contains(&csr.neighbors[offset]) {
                                csr.neighbors[offset] = <I as IndexType>::max();
                                deleted_edges += 1;
                            }
                        }
                        new_degree[last_vertex.index()] -= deleted_edges;
                        nbr_set.clear();
                    }
                    last_vertex = *src;
                }
                nbr_set.insert(*dst);
            }
            if !nbr_set.is_empty() && new_degree[last_vertex.index()] > 0 {
                let mut deleted_edges = 0;
                for offset in csr.offsets[last_vertex.index()]..csr.offsets[last_vertex.index() + 1] {
                    if nbr_set.contains(&csr.neighbors[offset]) {
                        csr.neighbors[offset] = <I as IndexType>::max();
                        deleted_edges += 1;
                    }
                }
                new_degree[last_vertex.index()] -= deleted_edges;
            }

            for (src, dst) in add_edges {
                new_degree[src.index()] += 1;
            }
        } else {
            let mut last_vertex = <I as IndexType>::max();
            let mut nbr_set = HashSet::new();
            for (dst, src) in delete_edges {
                if *src != last_vertex {
                    if !nbr_set.is_empty() && new_degree[last_vertex.index()] > 0 {
                        let mut deleted_edges = 0;
                        for offset in csr.offsets[last_vertex.index()]..csr.offsets[last_vertex.index() + 1] {
                            if nbr_set.contains(&csr.neighbors[offset]) {
                                csr.neighbors[offset] = <I as IndexType>::max();
                                deleted_edges += 1;
                            }
                        }
                        new_degree[last_vertex.index()] -= deleted_edges;
                        nbr_set.clear();
                    }
                    last_vertex = *src;
                }
                nbr_set.insert(*dst);
            }
            if !nbr_set.is_empty() && new_degree[last_vertex.index()] > 0 {
                let mut deleted_edges = 0;
                for offset in csr.offsets[last_vertex.index()]..csr.offsets[last_vertex.index() + 1] {
                    if nbr_set.contains(&csr.neighbors[offset]) {
                        csr.neighbors[offset] = <I as IndexType>::max();
                        deleted_edges += 1;
                    }
                }
                new_degree[last_vertex.index()] -= deleted_edges;
            }

            for (dst, src) in add_edges {
                new_degree[dst.index()] += 1;
            }
        }

        let mut builder = BatchMutableCsrBuilder::new();
        builder.init(&new_degree, 1.0);
        if let Some(t) = table {
            let mut header = vec![];
            for pair in cols.iter() {
                header.push((pair.1.clone(), pair.0.clone()));
            }
            let mut new_table = ColTable::new(header);
            for v in 0..old_vertex_num.index() {
                for offset in csr.offsets[v]..csr.offsets[v + 1] {
                    if csr.neighbors[offset] != <I as IndexType>::max() {
                        let index = builder.put_edge(I::new(v), csr.neighbors[offset]).unwrap();
                        new_table.insert(index, &t.get_row(offset).unwrap());
                    }
                }
            }
            if dir == Direction::Outgoing {
                for (offset, (src, dst)) in add_edges.iter().enumerate() {
                    let index = builder.put_edge(*src, *dst).unwrap();
                    new_table.insert(index, &t.get_row(offset).unwrap());
                }
            } else {
                for (offset, (src, dst)) in add_edges.iter().enumerate() {
                    let index = builder.put_edge(*dst, *src).unwrap();
                    new_table.insert(index, &t.get_row(offset).unwrap());
                }
            }
            *t = new_table;
        } else {
            for v in 0..old_vertex_num.index() {
                for offset in csr.offsets[v]..csr.offsets[v + 1] {
                    if csr.neighbors[offset] != <I as IndexType>::max() {
                        builder.put_edge(I::new(v), csr.neighbors[offset]);
                    }
                }
            }
            if dir == Direction::Outgoing {
                for (src, dst) in add_edges {
                    builder.put_edge(*src, *dst);
                }
            } else {
                for (src, dst) in add_edges {
                    builder.put_edge(*dst, *src);
                }
            }
        }

        *csr = builder.finish().unwrap();
    }

    pub fn apply_modifications_on_single_csr(&mut self,
                                             index: usize,
                                             modification_index: usize,
                                             new_vertex_num: usize,
                                             vertices_to_delete: &Vec<I>,
                                             delete_edges: &Vec<(I, I)>,
                                             add_edges: &Vec<(I, I)>, dir: Direction) {
        let csr = if dir == Direction::Outgoing {
            self.oe[index]
                .as_mut_any()
                .downcast_mut::<BatchMutableSingleCsr<I>>()
                .unwrap()
        } else {
            self.ie[index]
                .as_mut_any()
                .downcast_mut::<BatchMutableSingleCsr<I>>()
                .unwrap()
        };
        let table = if dir == Direction::Outgoing {
            self.oe_edge_prop_table.get_mut(&index)
        } else {
            self.ie_edge_prop_table.get_mut(&index)
        };
        let add_edges_table = if table.is_some() {
            self.modification.add_edge_prop_tables.get(&modification_index)
        } else {
            None
        };
        csr.resize_vertex(new_vertex_num);
        for id in vertices_to_delete {
            csr.remove_vertex(*id);
        }
        if dir == Direction::Outgoing {
            for (src, dst) in delete_edges {
                csr.remove_vertex(*src);
            }
        } else {
            for (src, dst) in delete_edges {
                csr.remove_vertex(*dst);
            }
        }
        if let Some(t) = table {
            if dir == Direction::Outgoing {
                for (index, (src, dst)) in add_edges.iter().enumerate() {
                    csr.put_edge(*src, *dst);
                    t.insert(src.index(), &add_edges_table.unwrap().get_row(index).unwrap());
                }
            } else {
                for (index, (src, dst)) in add_edges.iter().enumerate() {
                    csr.put_edge(*dst, *src);
                    t.insert(dst.index(), &add_edges_table.unwrap().get_row(index).unwrap());
                }
            }
        } else {
            if dir == Direction::Outgoing {
                for (src, dst) in add_edges {
                    csr.put_edge(*src, *dst)
                }
            } else {
                for (src, dst) in add_edges {
                    csr.put_edge(*dst, *src)
                }
            }
        }
    }

    pub fn apply_modifications(&mut self) {
        let mut vertices_to_delete = vec![];
        for (label, ids) in self.modification.delete_vertices.iter_mut().enumerate() {
            let mut parsed_ids = vec![];
            for id in ids.into_iter() {
                let (got_label, lid) = self.vertex_map.get_internal_id(*id).unwrap();
                if (got_label as usize) == label {
                    parsed_ids.push(lid);
                } else {
                    warn!("label not match, got: {}, expect: {}", got_label, label);
                }
            }
            ids.clear();

            vertices_to_delete.push(parsed_ids);
        }

        for edge_label_i in 0..self.edge_label_num {
            for src_label_i in 0..self.vertex_label_num {
                for dst_label_i in 0..self.vertex_label_num {
                    let modification_index = self.modification.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, edge_label_i as LabelId);

                    let inserted_edges = !self.modification.add_edges[modification_index].is_empty();
                    let deleted_edges = !self.modification.delete_edges[modification_index].is_empty();

                    let oe_index = self.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, edge_label_i as LabelId, Direction::Outgoing);
                    let ie_index = self.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, edge_label_i as LabelId, Direction::Incoming);

                    let inserted_src = !self.vertex_map.vertex_num(src_label_i as LabelId) != self.oe[oe_index].vertex_num().index();
                    let inserted_dst = !self.vertex_map.vertex_num(dst_label_i as LabelId) != self.ie[ie_index].vertex_num().index();

                    let deleted_src = !vertices_to_delete[src_label_i].is_empty();
                    let deleted_dst = !vertices_to_delete[dst_label_i].is_empty();

                    if !inserted_edges && !deleted_edges && !inserted_src && !inserted_dst && !deleted_src && !deleted_dst {
                        continue;
                    }

                    let mut parsed_add_edges = vec![];
                    let mut parsed_delete_edges = vec![];

                    for (src, dst) in self.modification.delete_edges[modification_index].iter() {
                        let (got_src_label, src_lid) = self.vertex_map.get_internal_id(*src).unwrap();
                        let (got_dst_label, dst_lid) = self.vertex_map.get_internal_id(*dst).unwrap();
                        if (got_src_label as usize) == src_label_i && (got_dst_label as usize) == dst_label_i {
                            parsed_delete_edges.push((src_lid, dst_lid));
                        } else {
                            warn!("label not match, got: {}->{}, expect: {}->{}", got_src_label, got_dst_label, src_label_i, dst_label_i);
                        }
                    }
                    self.modification.delete_edges[modification_index].clear();

                    for (src, dst) in self.modification.add_edges[modification_index].iter() {
                        let (got_src_label, src_lid) = self.vertex_map.get_internal_id(*src).unwrap();
                        let (got_dst_label, dst_lid) = self.vertex_map.get_internal_id(*dst).unwrap();
                        if (got_src_label as usize) == src_label_i && (got_dst_label as usize) == dst_label_i {
                            parsed_add_edges.push((src_lid, dst_lid));
                        } else {
                            warn!("label not match, got: {}->{}, expect: {}->{}", got_src_label, got_dst_label, src_label_i, dst_label_i);
                        }
                    }
                    self.modification.add_edges[modification_index].clear();

                    let ie_index = self.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, edge_label_i as LabelId, Direction::Incoming);
                    if self.graph_schema.is_single_ie(src_label_i as LabelId, edge_label_i as LabelId, dst_label_i as LabelId) {
                        self.apply_modifications_on_single_csr(
                            ie_index,
                            modification_index,
                            self.vertex_map.vertex_num(dst_label_i as LabelId),
                            &vertices_to_delete[dst_label_i],
                            &parsed_delete_edges,
                            &parsed_delete_edges,
                            Direction::Incoming
                        );
                    } else {
                        let mut cols = vec![];
                        {
                            if let Some(cols_ref) = self.graph_schema.get_edge_header(src_label_i as LabelId, edge_label_i as LabelId, dst_label_i as LabelId) {
                                for (name, data_type) in cols_ref.iter() {
                                    cols.push((name.clone(), data_type.clone()));
                                }
                            }
                        }
                        parsed_delete_edges.sort_by(|a, b| a.1.index().cmp(&b.1.index()));
                        self.apply_modifications_on_csr(
                            ie_index,
                            modification_index,
                            cols,
                            self.vertex_map.vertex_num(dst_label_i as LabelId),
                            &vertices_to_delete[dst_label_i],
                            &parsed_delete_edges,
                            &parsed_delete_edges,
                            Direction::Incoming
                        );
                    }

                    let oe_index = self.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, edge_label_i as LabelId, Direction::Outgoing);
                    if self.graph_schema.is_single_oe(src_label_i as LabelId, edge_label_i as LabelId, dst_label_i as LabelId) {
                        self.apply_modifications_on_single_csr(
                            oe_index,
                            modification_index,
                            self.vertex_map.vertex_num(src_label_i as LabelId),
                            &vertices_to_delete[src_label_i],
                            &parsed_delete_edges,
                            &parsed_delete_edges,
                            Direction::Outgoing
                        );
                    } else {
                        let mut cols = vec![];
                        {
                            if let Some(cols_ref) = self.graph_schema.get_edge_header(src_label_i as LabelId, edge_label_i as LabelId, dst_label_i as LabelId) {
                                for (name, data_type) in cols_ref.iter() {
                                    cols.push((name.clone(), data_type.clone()));
                                }
                            }
                        }
                        parsed_delete_edges.sort_by(|a, b| a.0.index().cmp(&b.0.index()));
                        self.apply_modifications_on_csr(
                            oe_index,
                            modification_index,
                            cols,
                            // self.graph_schema.get_edge_header(src_label_i as LabelId, edge_label_i as LabelId, dst_label_i as LabelId),
                            self.vertex_map.vertex_num(src_label_i as LabelId),
                            &vertices_to_delete[src_label_i],
                            &parsed_delete_edges,
                            &parsed_delete_edges,
                            Direction::Outgoing
                        );
                    }
                }
            }
        }

        self.modification.add_edge_prop_tables.clear();
    }
}
