use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use rayon::prelude::*;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter, SafeMutPtr};
use crate::graph::IndexType;
use crate::table::Table;
use shm_container::SharedVec;

pub struct SCsr<I: Copy + Sized> {
    nbr_list: SharedVec<I>,

    // meta[0]: vertex_num
    // meta[1]: edge_num
    meta: SharedVec<usize>,
}

impl<I: IndexType> SCsr<I> {
    pub fn new() -> Self {
        Self { nbr_list: SharedVec::<I>::new(), meta: SharedVec::<usize>::new() }
    }

    pub fn load(prefix: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_meta", prefix).as_str(), format!("{}_meta", name).as_str());
        SharedVec::<I>::load(format!("{}_nbrs", prefix).as_str(), format!("{}_nbrs", name).as_str());
    }

    pub fn open(prefix: &str) -> Self {
        let tmp_vec = SharedVec::<usize>::open(format!("{}_meta", prefix).as_str());
        Self {
            nbr_list: SharedVec::<I>::open(format!("{}_nbrs", prefix).as_str()),
            meta: SharedVec::<usize>::open(format!("{}_meta", prefix).as_str()),
        }
    }

    pub fn get_edge(&self, src: I) -> Option<I> {
        let nbr = self.nbr_list[src.index()];
        if nbr == <I as IndexType>::max() {
            None
        } else {
            Some(nbr)
        }
    }

    pub fn get_edge_with_offset(&self, src: I) -> Option<(I, usize)> {
        let nbr = self.nbr_list[src.index()];
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
        I::new(self.meta[0])
    }

    fn edge_num(&self) -> usize {
        self.meta[1]
    }

    fn max_edge_offset(&self) -> usize {
        self.nbr_list.len()
    }

    fn degree(&self, u: I) -> usize {
        (self.nbr_list[u.index()] == <I as IndexType>::max()) as usize
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        if self.nbr_list[u.index()] == <I as IndexType>::max() {
            None
        } else {
            Some(NbrIter::new(unsafe { self.nbr_list.as_ptr().add(u.index()) }, unsafe {
                self.nbr_list.as_ptr().add(u.index() + 1)
            }))
        }
    }
    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>> {
        if self.nbr_list[u.index()] == <I as IndexType>::max() {
            None
        } else {
            Some(NbrOffsetIter::new(
                unsafe { self.nbr_list.as_ptr().add(u.index()) },
                unsafe { self.nbr_list.as_ptr().add(u.index() + 1) },
                u.index(),
            ))
        }
    }

    fn delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool) -> Vec<(usize, usize)> {
        let mut delete_map = HashMap::<I, HashSet<I>>::new();
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    if dst.index() < self.nbr_list.len() {
                        let mut set = HashSet::<I>::new();
                        set.insert(*src);
                        delete_map.insert(*dst, set);
                    }
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*dst);
                } else {
                    if src.index() < self.nbr_list.len() {
                        let mut set = HashSet::<I>::new();
                        set.insert(*dst);
                        delete_map.insert(*src, set);
                    }
                }
            }
        }

        let safe_nbr_list = SafeMutPtr::new(&mut self.nbr_list);

        let deleted_counter = AtomicUsize::new(0);
        delete_map
            .par_iter()
            .for_each(|(v, delete_set)| {
                let nbr = safe_nbr_list.get_mut()[v.index()];
                if nbr != <I as IndexType>::max() && delete_set.contains(&nbr) {
                    safe_nbr_list.get_mut()[v.index()] = <I as IndexType>::max();
                    deleted_counter.fetch_add(1, Ordering::Relaxed);
                }
            });

        self.meta[1] -= deleted_counter.load(Ordering::Relaxed);
        vec![]
    }

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        let vnum = self.vertex_num();
        for vertex in vertices {
            if *vertex < vnum {
                self.nbr_list[vertex.index()] = <I as IndexType>::max();
            }
        }
    }

    fn delete_neighbors(&mut self, neighbors: &HashSet<I>) -> Vec<(usize, usize)> {
        let nbrs_slice = self.nbr_list.as_mut_slice();
        let deleted_counter = AtomicUsize::new(0);

        nbrs_slice.par_iter_mut().for_each(|nbr| {
            if *nbr != <I as IndexType>::max() {
                if neighbors.contains(nbr) {
                    *nbr = <I as IndexType>::max();
                    deleted_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        self.meta[1] -= deleted_counter.load(Ordering::Relaxed);
        vec![]
    }

    fn insert_edges(
        &mut self, vertex_num: usize, edges: &Vec<(I, I)>,
        insert_edges_prop: Option<&crate::dataframe::DataFrame>, reverse: bool,
        edges_prop: Option<&mut Table>,
    ) {
        self.nbr_list.resize(vertex_num);

        let mut insert_counter = 0;
        if let Some(it) = insert_edges_prop {
            let mut insert_offsets = Vec::with_capacity(edges.len());
            if reverse {
                for (dst, src) in edges.iter() {
                    if self.nbr_list[src.index()] == <I as IndexType>::max() {
                        insert_counter += 1;
                    }
                    self.nbr_list[src.index()] = *dst;
                    insert_offsets.push(src.index());
                }
            } else {
                for (src, dst) in edges.iter() {
                    if self.nbr_list[src.index()] == <I as IndexType>::max() {
                        insert_counter += 1;
                    }
                    self.nbr_list[src.index()] = *dst;
                    insert_offsets.push(src.index());
                }
            }

            if let Some(ep) = edges_prop {
                ep.resize(vertex_num);
                ep.insert_batch(&insert_offsets, it);
            }
        } else {
            if reverse {
                for (dst, src) in edges.iter() {
                    if self.nbr_list[src.index()] == <I as IndexType>::max() {
                        insert_counter += 1;
                    }
                    self.nbr_list[src.index()] = *dst;
                }
            } else {
                for (src, dst) in edges.iter() {
                    if self.nbr_list[src.index()] == <I as IndexType>::max() {
                        insert_counter += 1;
                    }
                    self.nbr_list[src.index()] = *dst;
                }
            }
        }

        self.meta[0] = vertex_num;
        self.meta[1] += insert_counter;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}
