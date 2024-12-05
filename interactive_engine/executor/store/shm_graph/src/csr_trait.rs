use std::any::Any;

use crate::graph::IndexType;
pub struct NbrIter<I> {
    start: *const I,
    end: *const I,
}

impl<I> NbrIter<I> {
    pub fn new(start: *const I, end: *const I) -> Self {
        NbrIter { start, end }
    }
}

impl<I: IndexType> Iterator for NbrIter<I> {
    type Item = I;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            None
        } else {
            let ret = unsafe { *self.start };
            self.start = unsafe { self.start.add(1) };
            Some(ret)
        }
    }
}

unsafe impl<I: IndexType> Sync for NbrIter<I> {}

unsafe impl<I: IndexType> Send for NbrIter<I> {}

pub struct NbrOffsetIter<I> {
    start: *const I,
    end: *const I,
    offset: usize,
}

impl<I> NbrOffsetIter<I> {
    pub fn new(start: *const I, end: *const I, offset: usize) -> Self {
        NbrOffsetIter { start, end, offset }
    }
}

impl<I: IndexType> Iterator for NbrOffsetIter<I> {
    type Item = (I, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            None
        } else {
            let ret = (unsafe { *self.start }, self.offset);
            self.start = unsafe { self.start.add(1) };
            self.offset += 1;
            Some(ret)
        }
    }
}

pub trait CsrTrait<I: IndexType>: Send + Sync {
    fn vertex_num(&self) -> I;
    fn max_edge_offset(&self) -> usize;
    fn edge_num(&self) -> usize;
    fn degree(&self, u: I) -> usize;

    fn get_edges(&self, u: I) -> Option<NbrIter<I>>;
    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>>;

    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}