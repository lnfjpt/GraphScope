use crate::csr::Csr;
use crate::scsr::SCsr;
use crate::table::Table;
use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use crate::types::{DefaultId, InternalId, LabelId};
use crate::vertex_map::VertexMap;

#[derive(Copy, Clone)]
pub struct SubGraph<'a, G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub csr: &'a Csr<I>,
    pub vm: &'a VertexMap<G, I>,
    pub src_label: LabelId,
    pub dst_label: LabelId,
    pub e_label: LabelId,

    pub vertex_data: &'a Table,
    pub edge_data: Option<&'a Table>,
}

impl<'a, G: Send + Sync + IndexType, I: Send + Sync + IndexType> SubGraph<'a, G, I> {
    pub fn new(
        csr: &'a Csr<I>, vm: &'a VertexMap<G, I>, src_label: LabelId, dst_label: LabelId,
        e_label: LabelId, vertex_data: &'a Table, edge_data: Option<&'a Table>,
    ) -> Self {
        SubGraph { csr, vm, src_label, dst_label, e_label, vertex_data, edge_data }
    }

    pub fn get_vertex_num(&self) -> I {
        self.csr.vertex_num()
    }

    pub fn get_adj_list(&self, src: I) -> Option<NbrIter<I>> {
        self.csr.get_edges(src)
    }

    pub fn get_adj_list_with_offset(&self, src: I) -> Option<NbrOffsetIter<I>> {
        self.csr.get_edges_with_offset(src)
    }

    pub fn get_properties(&self) -> Option<&'a Table> {
        self.edge_data
    }

    pub fn degree(&self, src_id: I) -> i64 {
        self.csr.degree(src_id) as i64
    }
}

#[derive(Copy, Clone)]
pub struct SingleSubGraph<
    'a,
    G: Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
> {
    pub csr: &'a SCsr<I>,
    pub vm: &'a VertexMap<G, I>,
    pub src_label: LabelId,
    pub dst_label: LabelId,
    pub e_label: LabelId,

    pub vertex_data: &'a Table,
    pub edge_data: Option<&'a Table>,
}

impl<'a, G, I> SingleSubGraph<'a, G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn new(
        csr: &'a SCsr<I>, vm: &'a VertexMap<G, I>, src_label: LabelId, dst_label: LabelId,
        e_label: LabelId, vertex_data: &'a Table, edge_data: Option<&'a Table>,
    ) -> Self {
        Self { csr, vm, src_label, dst_label, e_label, vertex_data, edge_data }
    }

    pub fn get_vertex_num(&self) -> I {
        self.csr.vertex_num()
    }

    pub fn get_properties(&self) -> Option<&'a Table> {
        self.edge_data
    }

    pub fn get_adj_list(&self, src: I) -> Option<NbrIter<I>> {
        self.csr.get_edges(src)
    }

    pub fn get_adj_list_with_offset(&self, src: I) -> Option<NbrOffsetIter<I>> {
        self.csr.get_edges_with_offset(src)
    }

    pub fn get_edge(&self, src: I) -> Option<I> {
        self.csr.get_edge(src)
    }

    pub fn get_edge_with_offset(&self, src: I) -> Option<(I, usize)> {
        self.csr.get_edge_with_offset(src)
    }

    pub fn degree(&self, src_id: I) -> i64 {
        self.csr.degree(src_id) as i64
    }
}