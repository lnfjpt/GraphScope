use std::any::Any;
use std::collections::HashSet;

use bmcsr::csr::{SafeMutPtr, SafePtr};
use rayon::iter::IntoParallelRefIterator;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use crate::vector::{SharedMutVec, SharedVec, PtrWrapper, MutPtrWrapper};
use crate::table::Table;

pub struct SCsr<I: Copy + Sized> {
    nbr_list: SharedVec<I>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
}

impl<I: IndexType> SCsr<I> {
    pub fn new() -> Self {
        Self {
            nbr_list: SharedVec::<I>::new(),
            vertex_num: 0,
            edge_num: 0,
            vertex_capacity: 0,
        }
    }

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

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        let mut mut_vec = SharedMutVec::<I>::open(self.nbr_list.name());
        for vertex in vertices {
            if *vertex < self.vertex_num() {
                mut_vec[vertex.index()] = <I as IndexType>::max();
            }
        }
    }

    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, table: Option<&mut Table>, p: u32,
    nbr_vertices: Option<&HashSet<I>>) {
        let edges_num = edges.len();

        let mut mut_vec = SharedMutVec::<I>::open(self.nbr_list.name());
        let safe_nbr_list_ptr = SafeMutPtr::new(&mut mut_vec);
        let safe_edges_ptr = SafePtr::new(edges);

        let num_threads = p as usize;
        let chunk_size = (edges_num + num_threads - 1) / num_threads;
        let vertex_num = self.vertex_num;

        let nbr_chunk_size = (self.nbr_list.len() + num_threads - 1) / num_threads;
        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = edges_num.min(start_idx + chunk_size);
                let nbr_start_idx = i * nbr_chunk_size;
                let nbr_end_idx = self.nbr_list.len().min(nbr_start_idx + nbr_chunk_size);
                s.spawn(move |_| {
                    let edges_ref = safe_edges_ptr.get_ref();
                    let nbr_list_ref = safe_nbr_list_ptr.get_mut();
                    if reverse {
                        for k in start_idx..end_idx {
                            let v = edges_ref[k].1;
                            if v.index() < vertex_num {
                                nbr_list_ref[v.index()] = <I as IndexType>::max();
                            }
                        }
                    } else {
                        for k in start_idx..end_idx {
                            let v = edges_ref[k].0;
                            if v.index() < vertex_num {
                                nbr_list_ref[v.index()] = <I as IndexType>::max();
                            }
                        }
                    }

                    if let Some(nbr_set) = nbr_vertices {
                        for index in nbr_start_idx..nbr_end_idx {
                            if nbr_set.contains(&nbr_list_ref[index]) {
                                nbr_list_ref[index] = <I as IndexType>::max();
                            }
                        }
                    }
                });
            }
        });
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}
