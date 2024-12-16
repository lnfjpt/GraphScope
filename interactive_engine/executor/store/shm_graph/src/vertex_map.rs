use std::collections::HashSet;
use std::marker::PhantomData;

use crate::graph::*;
use crate::indexer::Indexer;
use crate::ldbc_parser::LDBCVertexParser;
use crate::types::LabelId;

pub struct VertexMap<G: Send + Sync + IndexType, I: Send + Sync + IndexType> {
    label_num: LabelId,
    vertices_num: Vec<usize>,

    labeled_num: Vec<usize>,
    pub indexers: Vec<Indexer<G>>,

    labeled_corner_num: Vec<usize>,
    pub corner_indexers: Vec<Indexer<G>>,

    ph: PhantomData<I>,
}

impl<G, I> VertexMap<G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn load(prefix: &str, num_labels: usize, name: &str) {
        for i in 0..num_labels {
            Indexer::<G>::load(format!("{}/vm_{}", prefix, i).as_str(), format!("{}_vm_{}", name, i).as_str());
            Indexer::<G>::load(format!("{}/vmc_{}", prefix, i).as_str(), format!("{}_vmc_{}", name, i).as_str());
        }
    }

    pub fn open(prefix: &str, num_labels: usize) -> Self {
        let mut indexers = vec![];
        let mut vertices_num = Vec::with_capacity(num_labels);
        let mut labeled_num = Vec::with_capacity(num_labels);
        for i in 0..num_labels {
            let cur_indexer = Indexer::open(format!("{}_vm_{}", prefix, i as usize).as_str());
            vertices_num.push(cur_indexer.len());
            labeled_num.push(cur_indexer.len());
            indexers.push(cur_indexer);
        }
        let mut corner_indexers = vec![];
        let mut labeled_corner_num = Vec::with_capacity(num_labels);
        for i in 0..num_labels {
            let cur_indexer = Indexer::open(format!("{}_vmc_{}", prefix, i as usize).as_str());
            labeled_corner_num.push(cur_indexer.len());
            corner_indexers.push(cur_indexer);
        }
        Self {
            label_num: num_labels as LabelId,
            vertices_num,
            labeled_num,
            indexers,
            labeled_corner_num,
            corner_indexers,
            ph: PhantomData,
        }
    }

    pub fn get_internal_id(&self, global_id: G) -> Option<(LabelId, I)> {
        let label_id = LDBCVertexParser::get_label_id(global_id);
        if let Some(internal_id) = self.indexers[label_id as usize].get_index(global_id) {
            Some((label_id, I::new(internal_id)))
        } else if let Some(internal_id) = self.corner_indexers[label_id as usize].get_index(global_id) {
            Some((label_id, I::new(<I as IndexType>::max().index() - internal_id - 1)))
        } else {
            None
        }
    }

    pub fn get_global_id(&self, label: LabelId, internal_id: I) -> Option<G> {
        let internal_id = internal_id.index();
        if internal_id < self.labeled_num[label as usize] {
            self.indexers[label as usize].get_key(internal_id)
        } else {
            self.corner_indexers[label as usize].get_key(<I as IndexType>::max().index() - internal_id - 1)
        }
    }

    pub fn label_num(&self) -> LabelId {
        self.label_num
    }

    pub fn vertex_num(&self, label: LabelId) -> usize {
        self.labeled_num[label as usize]
    }

    pub fn actual_vertices_num(&self, label: LabelId) -> usize {
        self.vertices_num[label as usize]
    }

    pub fn remove_vertices(&mut self, label: LabelId, id_list: &HashSet<I>) {
        let native_num = self.labeled_num[label as usize];
        let mut native_to_remove = vec![];
        let mut corner_to_remove = vec![];
        for v in id_list.iter() {
            if v.index() < native_num {
                native_to_remove.push(v.index());
            } else {
                corner_to_remove.push(<I as IndexType>::max().index() - v.index() - 1);
            }
        }
        let n = self.indexers[label as usize].erase_indices(&native_to_remove);
        self.corner_indexers[label as usize].erase_indices(&corner_to_remove);
        self.labeled_num[label as usize] -= n;
    }

    pub fn insert_native_vertices(&mut self, label: LabelId, id_list: &Vec<G>) -> Vec<usize> {
        self.indexers[label as usize].insert_batch(id_list)
    }

    pub fn insert_corner_vertices(&mut self, label: LabelId, id_list: &Vec<G>) {
        self.corner_indexers[label as usize].insert_batch(id_list);
    }
}