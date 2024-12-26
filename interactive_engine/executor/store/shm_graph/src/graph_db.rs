use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use crate::columns::Column;
use crate::csr::Csr;
use crate::csr_trait::CsrTrait;
use crate::graph::*;
use crate::schema::CsrGraphSchema;
use crate::schema::Schema;
use crate::scsr::SCsr;
use crate::sub_graph::SingleSubGraph;
use crate::sub_graph::SubGraph;
use crate::table::Table;
use crate::types::*;
use crate::vertex_map::VertexMap;

/// A data structure to maintain a local view of the vertex.
#[derive(Clone)]
pub struct LocalVertex<I: IndexType + Sync + Send> {
    /// The vertex's global id
    index: I,
    /// The vertex's label
    label: LabelId,
}

impl<I: IndexType + Sync + Send> LocalVertex<I> {
    pub fn new(index: I, label: LabelId) -> Self {
        Self { index, label }
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_index(&self) -> I {
        self.index
    }
}

pub struct Iter<'a, T> {
    inner: Box<dyn Iterator<Item = T> + 'a + Send>,
}

impl<'a, T> Iter<'a, T> {
    pub fn from_iter<I: Iterator<Item = T> + 'a + Send>(iter: I) -> Self {
        Iter { inner: Box::new(iter) }
    }

    pub fn from_iter_box(iter: Box<dyn Iterator<Item = T> + 'a + Send>) -> Self {
        Iter { inner: iter }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.inner.count()
    }
}

unsafe impl<'a, T> Send for Iter<'a, T> {}

pub struct Range<I: IndexType> {
    begin: I,
    end: I,
}

pub struct RangeIterator<I: IndexType> {
    cur: I,
    end: I,
}

impl<I: IndexType> Iterator for RangeIterator<I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == self.end {
            None
        } else {
            let ret = self.cur.clone();
            self.cur += I::new(1);
            Some(ret)
        }
    }
}

impl<I: IndexType> Range<I> {
    pub fn new(begin: I, end: I) -> Self {
        Range { begin, end }
    }

    pub fn into_iter(self) -> RangeIterator<I> {
        RangeIterator { cur: self.begin.clone(), end: self.end.clone() }
    }
}

pub struct GraphDB<G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub partition: usize,
    pub ie: HashMap<usize, Box<dyn CsrTrait<I>>>,
    pub oe: HashMap<usize, Box<dyn CsrTrait<I>>>,

    pub graph_schema: CsrGraphSchema,

    pub vertex_map: VertexMap<G, I>,

    pub vertex_prop_table: Vec<Table>,
    pub ie_edge_prop_table: HashMap<usize, Table>,
    pub oe_edge_prop_table: HashMap<usize, Table>,

    pub vertex_label_num: usize,
    pub edge_label_num: usize,

    // pub root_path: String,
    pub partition_prefix: String,

    pub pending_to_delete: HashMap<LabelId, HashSet<I>>,
}

impl<G, I> GraphDB<G, I>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    pub fn load(prefix: &str, partition: usize, name: &str) -> CsrGraphSchema {
        let schema_path = PathBuf::from_str(prefix)
            .unwrap()
            .join(DIR_GRAPH_SCHEMA)
            .join(FILE_SCHEMA);

        let graph_schema = CsrGraphSchema::from_json_file(schema_path).unwrap();

        let partition_prefix = format!("{}/{}/partition_{}", prefix, DIR_BINARY_DATA, partition);

        let vertex_label_num = graph_schema.vertex_type_to_id.len();
        let edge_label_num = graph_schema.edge_type_to_id.len();

        for i in 0..vertex_label_num {
            Table::load(
                format!("{}/vp_{}", partition_prefix.as_str(), i).as_str(),
                graph_schema
                    .get_vertex_header(i as LabelId)
                    .unwrap(),
                format!("{}_vp_{}", name, i).as_str(),
            );
        }

        for src_label in 0..vertex_label_num {
            for edge_label in 0..edge_label_num {
                for dst_label in 0..vertex_label_num {
                    if let Some(_) = graph_schema.get_edge_header(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let oe_prefix = format!(
                            "{}/oe_{}_{}_{}",
                            partition_prefix.as_str(),
                            src_label,
                            edge_label,
                            dst_label,
                        );
                        let oe_name_prefix =
                            format!("{}_oe_{}_{}_{}", name, src_label, edge_label, dst_label);
                        if graph_schema.is_single_oe(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            SCsr::<I>::load(oe_prefix.as_str(), oe_name_prefix.as_str());
                        } else {
                            Csr::<I>::load(oe_prefix.as_str(), oe_name_prefix.as_str());
                        }

                        let oep_prefix = format!(
                            "{}/oep_{}_{}_{}",
                            partition_prefix.as_str(),
                            src_label,
                            edge_label,
                            dst_label,
                        );
                        let oep_name_prefix =
                            format!("{}_oep_{}_{}_{}", name, src_label, edge_label, dst_label,);
                        if let Some(header) = graph_schema.get_edge_header(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            if !header.is_empty() {
                                Table::load(oep_prefix.as_str(), header, oep_name_prefix.as_str());
                            }
                        }

                        let ie_prefix = format!(
                            "{}/ie_{}_{}_{}",
                            partition_prefix.as_str(),
                            src_label,
                            edge_label,
                            dst_label,
                        );
                        let ie_name_prefix =
                            format!("{}_ie_{}_{}_{}", name, src_label, edge_label, dst_label);
                        if graph_schema.is_single_ie(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            SCsr::<I>::load(ie_prefix.as_str(), ie_name_prefix.as_str());
                        } else {
                            Csr::<I>::load(ie_prefix.as_str(), ie_name_prefix.as_str());
                        }

                        let iep_prefix = format!(
                            "{}/iep_{}_{}_{}",
                            partition_prefix.as_str(),
                            src_label,
                            edge_label,
                            dst_label,
                        );
                        let iep_name_prefix =
                            format!("{}_iep_{}_{}_{}", name, src_label, edge_label, dst_label,);
                        if let Some(header) = graph_schema.get_edge_header(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            if !header.is_empty() {
                                Table::load(iep_prefix.as_str(), header, iep_name_prefix.as_str());
                            }
                        }
                    }
                }
            }
        }

        VertexMap::<G, I>::load(partition_prefix.as_str(), vertex_label_num, name);

        graph_schema
    }

    pub fn open(name: &str, graph_schema: CsrGraphSchema, partition: usize) -> Self {
        let vertex_label_num = graph_schema.vertex_type_to_id.len();

        let mut vertex_prop_table = Vec::with_capacity(vertex_label_num);
        for i in 0..vertex_label_num {
            vertex_prop_table.push(Table::open(
                format!("{}_vp_{}", name, i).as_str(),
                graph_schema
                    .get_vertex_header(i as LabelId)
                    .unwrap(),
            ));
        }

        let edge_label_num = graph_schema.edge_type_to_id.len();

        let mut ie = HashMap::<usize, Box<dyn CsrTrait<I>>>::new();
        let mut oe = HashMap::<usize, Box<dyn CsrTrait<I>>>::new();

        let mut ie_edge_prop_table = HashMap::<usize, Table>::new();
        let mut oe_edge_prop_table = HashMap::<usize, Table>::new();

        for src_label in 0..vertex_label_num {
            for edge_label in 0..edge_label_num {
                for dst_label in 0..vertex_label_num {
                    if let Some(_) = graph_schema.get_edge_header(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let index = src_label * vertex_label_num * edge_label_num
                            + dst_label * edge_label_num
                            + edge_label;
                        let oe_prefix = format!("{}_oe_{}_{}_{}", name, src_label, edge_label, dst_label,);
                        if graph_schema.is_single_oe(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            oe.insert(index, Box::new(SCsr::open(oe_prefix.as_str())));
                        } else {
                            oe.insert(index, Box::new(Csr::open(oe_prefix.as_str())));
                        }

                        let oep_prefix =
                            format!("{}_oep_{}_{}_{}", name, src_label, edge_label, dst_label,);
                        if let Some(header) = graph_schema.get_edge_header(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            if !header.is_empty() {
                                oe_edge_prop_table.insert(index, Table::open(oep_prefix.as_str(), header));
                            }
                        }

                        let ie_prefix = format!("{}_ie_{}_{}_{}", name, src_label, edge_label, dst_label,);
                        if graph_schema.is_single_ie(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            ie.insert(index, Box::new(SCsr::open(ie_prefix.as_str())));
                        } else {
                            ie.insert(index, Box::new(Csr::open(ie_prefix.as_str())));
                        }

                        let iep_prefix =
                            format!("{}_iep_{}_{}_{}", name, src_label, edge_label, dst_label,);
                        if let Some(header) = graph_schema.get_edge_header(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                        ) {
                            if !header.is_empty() {
                                ie_edge_prop_table.insert(index, Table::open(iep_prefix.as_str(), header));
                            }
                        }
                    }
                }
            }
        }

        Self {
            partition,

            ie,
            oe,

            graph_schema,
            vertex_map: VertexMap::open(name, vertex_label_num),

            vertex_prop_table,
            ie_edge_prop_table,
            oe_edge_prop_table,

            vertex_label_num,
            edge_label_num,

            // root_path: prefix.to_string(),
            partition_prefix: name.to_string(),

            pending_to_delete: HashMap::new(),
        }
    }

    // pub fn get_root_path(&self) -> &str {
    //     self.root_path.as_str()
    // }

    pub fn get_partition_prefix(&self) -> &str {
        self.partition_prefix.as_str()
    }

    pub fn set_vertex_property(&mut self, label: LabelId, prop_name: &str, prop_col: Box<dyn Column>) {
        let idx = self
            .graph_schema
            .get_vertex_property_id(label, prop_name)
            .unwrap();
        self.vertex_prop_table[label as usize].set_column(idx, prop_name, prop_col);
    }

    pub fn set_edge_property(
        &mut self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
        prop_name: &str, prop_col: Box<dyn Column>,
    ) {
        let idx = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        let col_idx = self
            .graph_schema
            .get_edge_property_id(src_label, edge_label, dst_label, prop_name)
            .unwrap();
        match dir {
            Direction::Incoming => {
                if let Some(table) = self.ie_edge_prop_table.get_mut(&idx) {
                    table.set_column(col_idx, prop_name, prop_col);
                } else {
                    self.ie_edge_prop_table
                        .insert(idx, Table::from_column(prop_name, prop_col));
                }
            }
            Direction::Outgoing => {
                if let Some(table) = self.oe_edge_prop_table.get_mut(&idx) {
                    table.set_column(col_idx, prop_name, prop_col);
                } else {
                    self.oe_edge_prop_table
                        .insert(idx, Table::from_column(prop_name, prop_col));
                }
            }
        }
    }

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
        self.oe.get(&index).unwrap().edge_num()
    }

    pub fn get_max_edge_offset(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> usize {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        match dir {
            Direction::Incoming => self.ie.get(&index).unwrap().max_edge_offset(),
            Direction::Outgoing => self.oe.get(&index).unwrap().max_edge_offset(),
        }
    }

    pub fn get_global_id(&self, id: I, label: LabelId) -> Option<G> {
        self.vertex_map.get_global_id(label, id)
    }

    pub fn get_internal_id(&self, id: G) -> I {
        self.vertex_map.get_internal_id(id).unwrap().1
    }

    pub fn get_internal_id_beta(&self, id: G) -> Option<I> {
        if let Some((_, id)) = self.vertex_map.get_internal_id(id) {
            Some(id)
        } else {
            None
        }
    }

    pub fn get_all_vertices(&self, label: LabelId) -> Iter<LocalVertex<I>> {
        let range = Range::new(I::new(0), I::new(self.get_vertices_num(label)));
        Iter::from_iter(
            range
                .into_iter()
                .map(move |index| LocalVertex::new(index, label)),
        )
    }

    pub fn get_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Incoming => SubGraph::new(
                &self
                    .ie
                    .get(&index)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Csr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.ie_edge_prop_table.get(&index),
            ),
            Direction::Outgoing => SubGraph::new(
                &self
                    .oe
                    .get(&index)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Csr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.oe_edge_prop_table.get(&index),
            ),
        }
    }

    pub fn get_single_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SingleSubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Incoming => SingleSubGraph::new(
                &self
                    .ie
                    .get(&index)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<SCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.ie_edge_prop_table.get(&index),
            ),
            Direction::Outgoing => SingleSubGraph::new(
                &self
                    .oe
                    .get(&index)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<SCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.oe_edge_prop_table.get(&index),
            ),
        }
    }

    pub fn apply_delete_neighbors(&mut self) {
        for (&vertex_label, vertex_set) in self.pending_to_delete.iter() {
            let start = Instant::now();
            for e_label_i in 0..self.edge_label_num {
                for src_label_i in 0..self.vertex_label_num {
                    if self.graph_schema.get_edge_header(src_label_i as LabelId, e_label_i as LabelId, vertex_label as LabelId).is_none() {
                        continue;
                    }

                    let index = self.edge_label_to_index(src_label_i as LabelId,
                        vertex_label as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );

                    if let Some(oe_csr) = self.oe.get_mut(&index) {
                        if let Some(table) = self.oe_edge_prop_table.get_mut(&index) {
                            let shuffle_indices = oe_csr.delete_neighbors_with_ret(vertex_set);
                            if !shuffle_indices.is_empty() {
                                table.parallel_move(&shuffle_indices);
                            }
                        } else {
                            oe_csr.delete_neighbors(vertex_set);
                        }
                    }
                }
                for dst_label_i in 0..self.vertex_label_num {
                    if self.graph_schema.get_edge_header(vertex_label as LabelId, e_label_i as LabelId, dst_label_i as LabelId).is_none() {
                        continue;
                    }
                    let index = self.edge_label_to_index(
                        vertex_label as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );
                    if let Some(ie_csr) = self.ie.get_mut(&index) {
                        if let Some(table) = self.ie_edge_prop_table.get_mut(&index) {
                            let shuffle_indices = ie_csr.delete_neighbors_with_ret(vertex_set);
                            if !shuffle_indices.is_empty() {
                                table.parallel_move(&shuffle_indices);
                            }
                        } else {
                            ie_csr.delete_neighbors(vertex_set);
                        }
                    }
                }
            }
            println!("delete pended vertices - {}: {} elapsed {} s", vertex_label as LabelId, vertex_set.len(), start.elapsed().as_secs_f64());
        }
        self.pending_to_delete.clear();
    }
}
