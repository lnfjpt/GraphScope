use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use rayon::prelude::*;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter, SafeMutPtr};
use crate::graph::IndexType;
use crate::table::Table;
use crate::vector::SharedVec;
use crate::dataframe::DataFrame;

pub struct Csr<I: Copy + Sized> {
    neighbors: SharedVec<I>,
    offsets: SharedVec<usize>,
    degree: SharedVec<i32>,

    // meta[0]: edge_num
    meta: SharedVec<usize>,
}

impl<I: IndexType> Csr<I> {
    pub fn load(prefix: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_meta", prefix).as_str(), format!("{}_meta", name).as_str());
        SharedVec::<I>::load(format!("{}_nbrs", prefix).as_str(), format!("{}_nbrs", name).as_str());
        SharedVec::<usize>::load(
            format!("{}_offsets", prefix).as_str(),
            format!("{}_offsets", name).as_str(),
        );
        SharedVec::<i32>::load(format!("{}_degree", prefix).as_str(), format!("{}_degree", name).as_str());
    }

    pub fn open(prefix: &str) -> Self {
        Self {
            neighbors: SharedVec::<I>::open(format!("{}_nbrs", prefix).as_str()),
            offsets: SharedVec::<usize>::open(format!("{}_offsets", prefix).as_str()),
            degree: SharedVec::<i32>::open(format!("{}_degree", prefix).as_str()),
            meta: SharedVec::<usize>::open(format!("{}_meta", prefix).as_str()),
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
        self.meta[0]
    }

    fn max_edge_offset(&self) -> usize {
        self.neighbors.len()
    }

    fn degree(&self, u: I) -> usize {
        let u = u.index();
        if u >= self.degree.len() {
            0
        } else {
            self.degree[u] as usize
        }
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start = self.offsets[u];
            let deg = self.degree[u] as usize;
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
            let start_offset = self.offsets[u];
            let deg = self.degree[u] as usize;
            let start = unsafe { self.neighbors.as_ptr().add(start_offset) };
            let end = unsafe { start.add(deg) };
            Some(NbrOffsetIter::new(start, end, start_offset))
        }
    }

    fn delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool) -> Vec<(usize, usize)> {
        let offsets_slice = self.offsets.as_slice();

        let mut delete_map = HashMap::<I, HashSet<I>>::new();
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    if dst.index() < self.offsets.len() {
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
                    if src.index() < self.offsets.len() {
                        let mut set = HashSet::<I>::new();
                        set.insert(*dst);
                        delete_map.insert(*src, set);
                    }
                }
            }
        }

        let safe_degree_list = SafeMutPtr::new(&mut self.degree);
        let delete_counter = AtomicUsize::new(0);

        let shuffle_indices: Vec<(usize, usize)> = delete_map.par_iter().flat_map(|(v, delete_set)| {
            let mut ret = vec![];
            let deg = safe_degree_list.get_mut()[v.index()];
            let mut found = 0;
            if deg != 0 {
                let mut from = offsets_slice[v.index()];
                let mut last = from + deg as usize - 1;

                loop {
                    while (from < last) && !delete_set.contains(&self.neighbors[from]) {
                        from += 1;
                    }
                    if delete_set.contains(&self.neighbors[from]) {
                        found += 1;
                    }
                    if from >= last {
                        break;
                    }
                    while (from < last) && delete_set.contains(&self.neighbors[last]) {
                        last -= 1;
                        found += 1;
                    }
                    if from >= last {
                        break;
                    }
                    ret.push((last, from));
                    from += 1;
                    last -= 1;
                }

                if found > 0 {
                    safe_degree_list.get_mut()[v.index()] -= found;
                    delete_counter.fetch_add(found as usize, Ordering::Relaxed);
                }
            }
            ret
        }).collect();
        
        self.meta[0] -= delete_counter.load(Ordering::Relaxed);
        self.neighbors.parallel_move(&shuffle_indices);

        shuffle_indices
    }

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        for vertex in vertices {
            let vertex = vertex.index();
            if vertex >= self.degree.len() {
                continue;
            }
            self.meta[0] -= self.degree[vertex] as usize;
            self.degree[vertex] = 0;
        }
    }

    fn delete_neighbors(&mut self, neighbors: &HashSet<I>) -> Vec<(usize, usize)> {
        let offsets_slice = self.offsets.as_slice();
        let degree_slice = self.degree.as_mut_slice();

        let deleted_counter = AtomicUsize::new(0);

        let shuffle_indices: Vec<(usize, usize)> = degree_slice.par_iter_mut().zip(offsets_slice.par_iter()).flat_map(|(deg, offset)| {
            let mut ret = vec![];
            if *deg > 0 {
                let mut from = *offset;
                let mut last = from + *deg as usize - 1;
                let mut found = 0;

                loop {
                    while (from < last) && !neighbors.contains(&self.neighbors[from]) {
                        from += 1;
                    }
                    if neighbors.contains(&self.neighbors[from]) {
                        found += 1;
                    }
                    if from >= last {
                        break;
                    }
                    while (from < last) && (neighbors.contains(&self.neighbors[last])) {
                        last -= 1;
                        found += 1;
                    }
                    if from >= last {
                        break;
                    }
                    ret.push((last, from));
                    from += 1;
                    last -= 1;
                }
                if found > 0 {
                    *deg -= found;
                    deleted_counter.fetch_add(found as usize, Ordering::Relaxed);
                }
            }

            ret
        }).collect();

        self.meta[0] -= deleted_counter.load(Ordering::Relaxed);
        self.neighbors.parallel_move(&shuffle_indices);
        shuffle_indices
    }

    fn insert_edges_beta(&mut self, vertex_num: usize, edges: &Vec<(I, I)>, insert_edges_prop: Option<&DataFrame>, reverse: bool, edges_prop: Option<&mut Table>) {
        let mut new_degree: Vec<i32> = (0..vertex_num).into_par_iter().map(|_| 0).collect();
        if reverse {
            for e in edges.iter() {
                if e.1.index() < vertex_num {
                    new_degree[e.1.index()] += 1;
                }
            }
        } else {
            for e in edges.iter() {
                if e.0.index() < vertex_num {
                    new_degree[e.0.index()] += 1;
                }
            }
        }

        let old_vertex_num = self.degree.len();
        assert!(old_vertex_num <= vertex_num);

        let mut new_offsets = Vec::<usize>::with_capacity(vertex_num);
        new_offsets.push(0);
        let mut cur_offset = 0_usize;
        for v in 0..old_vertex_num {
            cur_offset += (new_degree[v] + self.degree[v]) as usize;
            new_offsets.push(cur_offset);
        }
        for v in old_vertex_num..vertex_num {
            cur_offset += new_degree[v] as usize;
            new_offsets.push(cur_offset);
        }

        let new_edges_num = self.meta[0] + edges.len();

        self.neighbors.inplace_parallel_chunk_move(
            new_edges_num,
            self.offsets.as_slice(),
            self.degree.as_slice(),
            new_offsets.as_slice(),
        );

        self.degree.resize(vertex_num);
        self.degree.as_mut_slice()[old_vertex_num..vertex_num].par_iter_mut().for_each(|deg| {
            *deg = 0;
        });

        if let Some(it) = insert_edges_prop {
            if let Some(ep) = edges_prop {
                ep.inplace_parallel_chunk_move(
                    new_edges_num,
                    self.offsets.as_slice(),
                    self.degree.as_slice(),
                    new_offsets.as_slice(),
                );
                let mut insert_offsets = Vec::with_capacity(edges.len());
                if reverse {
                    for (dst, src) in edges.iter() {
                        let x = self.degree[src.index()] as usize;
                        self.degree[src.index()] += 1;
                        let offset = new_offsets[src.index()] + x;
                        insert_offsets.push(offset);
                        self.neighbors[offset] = *dst;
                    }
                } else {
                    for (src, dst) in edges.iter() {
                        let x = self.degree[src.index()] as usize;
                        self.degree[src.index()] += 1;
                        let offset = new_offsets[src.index()] + x;
                        insert_offsets.push(offset);
                        self.neighbors[offset] = *dst;
                    }
                }
                ep.insert_batch(&insert_offsets, it);
            } else {
                panic!("not supposed to reach here...");
            }
        } else {
            if reverse {
                for (dst, src) in edges.iter() {
                    let x = self.degree[src.index()] as usize;
                    self.degree[src.index()] += 1;
                    let offset = new_offsets[src.index()] + x;
                    self.neighbors[offset] = *dst;
                }
            } else {
                for (src, dst) in edges.iter() {
                    let x = self.degree[src.index()] as usize;
                    self.degree[src.index()] += 1;
                    let offset = new_offsets[src.index()] + x;
                    self.neighbors[offset] = *dst;
                }
            }
        }

        self.offsets.resize(vertex_num);
        self.offsets.as_mut_slice().par_iter_mut().zip(new_offsets.par_iter()).for_each(|(a, b)| {
            *a = *b;
        });
        self.meta[0] = new_edges_num;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}
