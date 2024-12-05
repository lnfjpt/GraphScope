use std::marker::PhantomData;

use crate::graph::*;
use crate::indexer::Indexer;
use crate::ldbc_parser::LDBCVertexParser;
use crate::types::LabelId;

pub struct VertexMap<G: Send + Sync + IndexType, I: Send + Sync + IndexType> {
    label_num: LabelId,
    vertices_num: Vec<usize>,
    pub indexers: Vec<Indexer<G>>,
    ph: PhantomData<I>,
}

impl<G, I> VertexMap<G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn open(prefix: &str, num_labels: usize) -> Self {
        let mut indexers = vec![];
        let mut vertices_num = Vec::with_capacity(num_labels);
        for i in 0..num_labels {
            let cur_indexer = Indexer::open(&format!("{}/vm_{}", prefix, i as usize));
            vertices_num.push(cur_indexer.len());
            indexers.push(cur_indexer);
        }
        Self {
            label_num: num_labels as LabelId,
            vertices_num,
            indexers,
            ph: PhantomData,
        }
    }

    pub fn get_internal_id(&self, global_id: G) -> Option<(LabelId, I)> {
        let label_id = LDBCVertexParser::get_label_id(global_id);
        if let Some(internal_id) = self.indexers[label_id as usize].get_index(global_id) {
            Some((label_id, I::new(internal_id)))
        } else {
            None
        }
    }

    pub fn get_global_id(&self, label: LabelId, internal_id: I) -> Option<G> {
        let internal_id = internal_id.index();
        self.indexers[label as usize].get_key(internal_id)
    }

    pub fn label_num(&self) -> LabelId {
        self.label_num
    }

    pub fn vertex_num(&self, label: LabelId) -> usize {
        self.indexers[label as usize].len()
    }

    pub fn actual_vertices_num(&self, label: LabelId) -> usize {
        self.vertices_num[label as usize]
    }
}