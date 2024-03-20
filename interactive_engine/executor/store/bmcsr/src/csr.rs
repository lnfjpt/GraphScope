use std::any::Any;
use std::collections::HashSet;
use std::marker::PhantomData;

use crate::col_table::ColTable;
use crate::graph::IndexType;

pub struct NbrIter<'a, I> {
    inner: std::slice::Iter<'a, I>,
}

impl<'a, I> NbrIter<'a, I> {
    pub fn new(vec: &'a Vec<I>, start: usize, end: usize) -> Self {
        NbrIter { inner: vec[start..end].iter() }
    }
}

impl<'a, I: IndexType> Iterator for NbrIter<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct NbrOffsetIter<'a, I> {
    inner: std::slice::Iter<'a, I>,
    offset: usize,
}

impl<'a, I> NbrOffsetIter<'a, I> {
    pub fn new(vec: &'a Vec<I>, start: usize, end: usize) -> Self {
        NbrOffsetIter { inner: vec[start..end].iter(), offset: start }
    }
}

impl<'a, I: IndexType> Iterator for NbrOffsetIter<'a, I> {
    type Item = (I, usize);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(x) => {
                let ret = (x.clone(), self.offset);
                self.offset += 1;
                Some(ret)
            }
            None => None,
        }
    }
}

pub trait CsrTrait<I: IndexType>: Send + Sync {
    fn vertex_num(&self) -> I;
    fn max_edge_offset(&self) -> usize;
    fn edge_num(&self) -> usize;
    fn degree(&self, u: I) -> usize;
    fn serialize(&self, path: &String);
    fn deserialize(&mut self, path: &String);

    fn get_edges(&self, u: I) -> Option<NbrIter<I>>;
    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>>;

    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;

    fn delete_vertices(&mut self, vertices: &HashSet<I>);
    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, p: u32);
    fn parallel_delete_edges_with_props(
        &mut self, edges: &Vec<(I, I)>, reverse: bool, table: &mut ColTable, p: u32,
    );

    fn insert_edges(&mut self, vertex_num: usize, edges: &Vec<(I, I)>, reverse: bool, p: u32);

    fn insert_edges_with_prop(
        &mut self, vertex_num: usize, edges: &Vec<(I, I)>, edges_prop: &ColTable, reverse: bool, p: u32,
        old_table: ColTable,
    ) -> ColTable;
}

#[derive(Debug)]
pub enum CsrBuildError {
    OffsetOutOfCapacity,
    UnfinishedVertex,
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