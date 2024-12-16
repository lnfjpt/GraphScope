use std::marker::PhantomData;
use std::{any::Any, collections::HashSet};

use crate::{graph::IndexType, table::Table};
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

    fn delete_vertices(&mut self, vertices: &HashSet<I>);
    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, table: Option<&mut Table>, p: u32,
nbr_vertices: Option<&HashSet<I>>);

    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}
pub struct SafePtr<I>(*const I, PhantomData<I>);

unsafe impl<I> Send for SafePtr<I> {}

unsafe impl<I> Sync for SafePtr<I> {}

impl<I> Clone for SafePtr<I> {
    fn clone(&self) -> Self {
        SafePtr(self.0.clone(), PhantomData)
    }
}

impl<I> Copy for SafePtr<I> {}

impl<I> SafePtr<I> {
    pub fn new(ptr: &I) -> Self {
        Self { 0: ptr as *const I, 1: PhantomData }
    }

    pub fn get_ref(&self) -> &I {
        unsafe { &*self.0 }
    }
}

pub struct SafeMutPtr<I>(*mut I, PhantomData<I>);

unsafe impl<I> Send for SafeMutPtr<I> {}

unsafe impl<I> Sync for SafeMutPtr<I> {}

impl<I> SafeMutPtr<I> {
    pub fn new(ptr: &mut I) -> Self {
        Self { 0: ptr as *mut I, 1: PhantomData }
    }

    pub fn get_mut(&self) -> &mut I {
        unsafe { &mut *self.0 }
    }
}

impl<I> Clone for SafeMutPtr<I> {
    fn clone(&self) -> Self {
        SafeMutPtr(self.0.clone(), PhantomData)
    }
}

impl<I> Copy for SafeMutPtr<I> {}
