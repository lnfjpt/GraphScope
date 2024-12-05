use std::any::Any;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter};
use crate::vector::SharedVec;
use crate::graph::IndexType;

pub struct Csr<I: Copy + Sized> {
    neighbors: SharedVec<I>,
    offsets: SharedVec<usize>,
    degree: SharedVec<i32>,

    edge_num: usize,
}

impl<I: IndexType> Csr<I> {
    pub fn open(prefix: &str) -> Self {
        let tmp_vec = SharedVec::<usize>::open(format!("{}_meta", prefix).as_str());

        Self {
            neighbors: SharedVec::<I>::open(format!("{}_nbrs", prefix).as_str()),
            offsets: SharedVec::<usize>::open(format!("{}_offsets", prefix).as_str()),
            degree: SharedVec::<i32>::open(format!("{}_degree", prefix).as_str()),
            edge_num: tmp_vec.get_unchecked(0),
        }
    }
}

unsafe impl<I: IndexType> Send for Csr<I> {}

unsafe impl<I: IndexType> Sync for Csr<I> {}

impl<I: IndexType> CsrTrait<I> for Csr<I> {
    fn vertex_num(&self) -> I {
        I::new(self.offsets.len())
    }

    fn edge_num(&self) -> usize {
        self.edge_num
    }

    fn max_edge_offset(&self) -> usize {
        self.neighbors.len()
    }

    fn degree(&self, u: I) -> usize {
        let u = u.index();
        if u >= self.degree.len() {
            0
        } else {
            self.degree.get_unchecked(u) as usize
        }
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start = self.offsets.get_unchecked(u);
            let deg = self.degree.get_unchecked(u) as usize;
            let start = unsafe { self.neighbors.as_ptr().add(start) };
            let end = unsafe { start.add(deg) };
            Some(NbrIter::new(start, end))
        }
    }

    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start_offset = self.offsets.get_unchecked(u);
            let deg = self.degree.get_unchecked(u) as usize;
            let start = unsafe { self.neighbors.as_ptr().add(start_offset) };
            let end = unsafe { start.add(deg) };
            Some(NbrOffsetIter::new(start, end, start_offset))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}