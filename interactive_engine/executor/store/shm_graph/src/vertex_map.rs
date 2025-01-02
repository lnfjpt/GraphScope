use std::collections::HashSet;
use std::marker::PhantomData;

use rayon::prelude::*;
use shm_container::SharedVec;

use crate::graph::*;
use crate::indexer::Indexer;
use crate::ldbc_parser::LDBCVertexParser;
use crate::types::LabelId;

pub struct VertexMap<G: Send + Sync + IndexType, I: Send + Sync + IndexType> {
    label_num: LabelId,
    vertices_num: SharedVec<usize>,
    pub indexers: Vec<Indexer<G>>,
    pub corner_indexers: Vec<Indexer<G>>,

    pub tombs: Vec<SharedVec<u8>>,
    // pub corner_tombs: Vec<SharedVec<u8>>,
    ph: PhantomData<I>,
}

impl<G, I> VertexMap<G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn load(prefix: &str, num_labels: usize, name: &str) {
        let mut vertices_num = SharedVec::<usize>::create(format!("{}_vm_vnum", name).as_str(), num_labels);
        for i in 0..num_labels {
            let indexer = Indexer::<G>::load(
                format!("{}/vm_{}", prefix, i).as_str(),
                format!("{}_vm_{}", name, i).as_str(),
            );
            let mut vm_tomb = SharedVec::<u8>::create(format!("{}_vm_tomb_{}", name, i).as_str(), indexer.len());
            vm_tomb.as_mut_slice().par_iter_mut().for_each(|x| {*x = 0_u8;});
            vertices_num[i] = indexer.len();
            Indexer::<G>::load(
                format!("{}/vmc_{}", prefix, i).as_str(),
                format!("{}_vmc_{}", name, i).as_str(),
            );
        }
    }

    pub fn open(prefix: &str, num_labels: usize) -> Self {
        let mut indexers = vec![];
        for i in 0..num_labels {
            let cur_indexer = Indexer::open(format!("{}_vm_{}", prefix, i as usize).as_str());
            indexers.push(cur_indexer);
        }
        let mut tombs = vec![];
        for i in 0..num_labels {
            tombs.push(SharedVec::<u8>::open(format!("{}_vm_tomb_{}", prefix, i as usize).as_str()));
        }
        let mut corner_indexers = vec![];
        for i in 0..num_labels {
            let cur_indexer = Indexer::open(format!("{}_vmc_{}", prefix, i as usize).as_str());
            corner_indexers.push(cur_indexer);
        }
        // let mut corner_tombs = vec![];
        // for i in 0..num_labels {
        //     corner_tombs
        //         .push(SharedVec::<u8>::open(format!("{}_vmc_tomb_{}", prefix, i as usize).as_str()));
        // }
        Self {
            label_num: num_labels as LabelId,
            vertices_num: SharedVec::<usize>::open(format!("{}_vm_vnum", prefix).as_str()),
            indexers,
            corner_indexers,
            tombs,
            // corner_tombs,
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
        if internal_id < self.indexers[label as usize].len() {
            self.indexers[label as usize].get_key(internal_id)
        } else {
            self.corner_indexers[label as usize].get_key(<I as IndexType>::max().index() - internal_id - 1)
        }
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

    pub fn remove_vertices(&mut self, label: LabelId, id_list: &HashSet<I>) {
        let native_num = self.indexers[label as usize].len();
        let mut num_to_remove = 0_usize;
        for v in id_list.iter() {
            if v.index() < native_num {
                if self.tombs[label as usize][v.index()] == 0 {
                    self.tombs[label as usize][v.index()] = 1;
                    num_to_remove += 1;
                }
            }
        }
        self.vertices_num[label as usize] -= num_to_remove;
    }

    pub fn insert_native_vertices(&mut self, label: LabelId, id_list: Vec<G>) -> Vec<usize> {
        let ret = self.indexers[label as usize].insert_batch_beta(id_list);
        let old_vertices_num = self.tombs[label as usize].len();
        self.tombs[label as usize].resize(self.indexers[label as usize].len());
        self.tombs[label as usize].as_mut_slice()[old_vertices_num..]
            .par_iter_mut()
            .for_each(|x| *x = 0);
        ret
    }

    pub fn is_valid_native_vertex(&self, label: LabelId, id: I) -> bool {
        let native_num = self.indexers[label as usize].len();
        if id.index() < native_num {
            self.tombs[label as usize][id.index()] == 0
        } else {
            false
        }
    }

    pub fn insert_corner_vertices(&mut self, label: LabelId, id_list: Vec<G>) {
        self.corner_indexers[label as usize].insert_batch_beta(id_list);
    }
}
