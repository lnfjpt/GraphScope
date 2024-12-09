use std::any::Any;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use crate::vector::SharedVec;

pub struct SCsr<I: Copy + Sized> {
    nbr_list: SharedVec<I>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
}

impl<I: IndexType> SCsr<I> {
    pub fn load(prefix: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_meta", prefix).as_str(), format!("{}_meta", name).as_str());
        SharedVec::<I>::load(format!("{}_nbrs", prefix).as_str(), format!("{}_nbrs", name).as_str());
    }

    pub fn open(prefix: &str) -> Self {
        let tmp_vec = SharedVec::<usize>::open(format!("{}_meta", prefix).as_str());
        Self {
            nbr_list: SharedVec::<I>::open(format!("{}_nbrs", prefix).as_str()),
            vertex_num: tmp_vec.get_unchecked(0),
            edge_num: tmp_vec.get_unchecked(1),
            vertex_capacity: tmp_vec.get_unchecked(2),
        }
    }

    pub fn get_edge(&self, src: I) -> Option<I> {
        let nbr = self.nbr_list.get_unchecked(src.index());
        if nbr == <I as IndexType>::max() {
            None
        } else {
            Some(nbr)
        }
    }

    pub fn get_edge_with_offset(&self, src: I) -> Option<(I, usize)> {
        let nbr = self.nbr_list.get_unchecked(src.index());
        if nbr == <I as IndexType>::max() {
            None
        } else {
            Some((nbr, src.index()))
        }
    }
}

unsafe impl<I: IndexType> Send for SCsr<I> {}

unsafe impl<I: IndexType> Sync for SCsr<I> {}

impl<I: IndexType> CsrTrait<I> for SCsr<I> {
    fn vertex_num(&self) -> I {
        I::new(self.vertex_num)
    }

    fn edge_num(&self) -> usize {
        self.edge_num
    }

    fn max_edge_offset(&self) -> usize {
        self.vertex_num
    }

    fn degree(&self, u: I) -> usize {
        (self.nbr_list.get_unchecked(u.index()) == <I as IndexType>::max()) as usize
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        if self.nbr_list.get_unchecked(u.index()) == <I as IndexType>::max() {
            None
        } else {
            Some(NbrIter::new(unsafe { self.nbr_list.as_ptr().add(u.index()) }, unsafe {
                self.nbr_list.as_ptr().add(u.index() + 1)
            }))
        }
    }
    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>> {
        if self.nbr_list.get_unchecked(u.index()) == <I as IndexType>::max() {
            None
        } else {
            Some(NbrOffsetIter::new(
                unsafe { self.nbr_list.as_ptr().add(u.index()) },
                unsafe { self.nbr_list.as_ptr().add(u.index() + 1) },
                u.index(),
            ))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}
