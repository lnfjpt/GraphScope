use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use rayon::prelude::*;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter, SafeMutPtr};
use crate::dataframe::DataFrame;
use crate::graph::IndexType;
use crate::table::Table;
use crate::vertex_map::VertexMap;
use crate::types::LabelId;

use shm_container::SharedVec;

pub struct Csr<G: IndexType, I: Copy + Sized> {
    neighbors: SharedVec<G>,
    offsets: SharedVec<usize>,
    degree: SharedVec<i32>,

    // meta[0]: edge_num
    meta: SharedVec<usize>,
    ph: PhantomData<I>,
}

fn generate_new_offsets<I: IndexType>(
    vertex_num: usize, old_degree: &[i32], offsets: &mut SharedVec<usize>, parsed_vertices: &Vec<I>,
) -> (Vec<(usize, usize, i64)>, usize) {
    let old_vertex_num = old_degree.len();
    let deleted_list: Vec<(I, i32)> = (0..(old_vertex_num - 1))
        .into_par_iter()
        .filter_map(|idx| {
            let cap = (offsets[idx + 1] - offsets[idx]) as i32;
            let deg = old_degree[idx];
            if cap == deg {
                None
            } else {
                Some((I::new(idx), cap - deg))
            }
        })
        .collect();

    let mut diff_table = HashMap::<usize, i32>::new();

    for (v, diff) in deleted_list.iter() {
        diff_table.insert(v.index(), -diff);
    }
    let mut new_vertex_degree = vec![0_i32; vertex_num - old_vertex_num];
    for v in parsed_vertices.iter() {
        if v.index() < old_vertex_num {
            if let Some(val) = diff_table.get_mut(&v.index()) {
                *val += 1;
            } else {
                diff_table.insert(v.index(), 1);
            }
        } else if v.index() < vertex_num {
            new_vertex_degree[v.index() - old_vertex_num] += 1;
        }
    }
    let mut diff_list: Vec<(usize, i32)> = diff_table
        .par_iter()
        .map(|(x, y)| (*x, *y))
        .collect();
    diff_list.sort_by(|a, b| a.0.cmp(&b.0));
    let mut last_begin = 0_usize;
    let mut last_diff = 0_i64;
    let mut v_range_diff = vec![];
    for (v, diff) in diff_list.iter() {
        v_range_diff.push((last_begin, *v + 1, last_diff));
        last_begin = *v + 1;
        last_diff += *diff as i64;
    }
    if last_begin != old_vertex_num {
        v_range_diff.push((last_begin, old_vertex_num, last_diff));
    }

    let e_range_diff: Vec<(usize, usize, i64)> = v_range_diff
        .par_iter()
        .map(|(v_begin, v_end, diff)| {
            (offsets[*v_begin], offsets[*v_end - 1] + old_degree[*v_end - 1] as usize, *diff)
        })
        .collect();
    offsets.resize(vertex_num);
    let safe_offsets = SafeMutPtr::new(offsets);
    v_range_diff
        .into_par_iter()
        .for_each(|(v_begin, v_end, diff)| {
            let offsets_slice = safe_offsets.get_mut().as_mut_slice();
            if diff >= 0 {
                for v in v_begin..v_end {
                    offsets_slice[v] += diff as usize;
                }
            } else {
                let diff_abs = diff.abs() as usize;
                for v in v_begin..v_end {
                    offsets_slice[v] -= diff_abs;
                }
            }
        });
    if vertex_num != old_vertex_num {
        let last_vertex_degree = if let Some(diff) = diff_table.get(&(old_vertex_num - 1)) {
            old_degree[old_vertex_num - 1] + *diff
        } else {
            old_degree[old_vertex_num - 1]
        };
        offsets[old_vertex_num] = offsets[old_vertex_num - 1] + last_vertex_degree as usize;
        for v in old_vertex_num + 1..vertex_num {
            offsets[v] = offsets[v - 1] + (new_vertex_degree[v - old_vertex_num - 1] as usize);
        }
    }

    (
        e_range_diff,
        offsets[vertex_num - 1]
            + (if vertex_num == old_vertex_num {
                if let Some(diff) = diff_table.get(&(old_vertex_num - 1)) {
                    (old_degree[old_vertex_num - 1] + *diff) as usize
                } else {
                    old_degree[old_vertex_num - 1] as usize
                }
            } else {
                new_vertex_degree[vertex_num - old_vertex_num - 1] as usize
            }),
    )
}

impl<G: IndexType, I: IndexType> Csr<G, I> {
    pub fn load(prefix: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_meta", prefix).as_str(), format!("{}_meta", name).as_str());
        SharedVec::<G>::load(format!("{}_nbrs", prefix).as_str(), format!("{}_nbrs", name).as_str());
        SharedVec::<usize>::load(
            format!("{}_offsets", prefix).as_str(),
            format!("{}_offsets", name).as_str(),
        );
        SharedVec::<i32>::load(format!("{}_degree", prefix).as_str(), format!("{}_degree", name).as_str());
    }

    pub fn open(prefix: &str) -> Self {
        Self {
            neighbors: SharedVec::<G>::open(format!("{}_nbrs", prefix).as_str()),
            offsets: SharedVec::<usize>::open(format!("{}_offsets", prefix).as_str()),
            degree: SharedVec::<i32>::open(format!("{}_degree", prefix).as_str()),
            meta: SharedVec::<usize>::open(format!("{}_meta", prefix).as_str()),
            ph: PhantomData,
        }
    }
}

unsafe impl<G: IndexType, I: IndexType> Send for Csr<G, I> {}

unsafe impl<G: IndexType, I: IndexType> Sync for Csr<G, I> {}

impl<G: IndexType, I: IndexType> CsrTrait<G, I> for Csr<G, I> {
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

    fn get_edges(&self, u: I) -> Option<NbrIter<G>> {
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

    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<G>> {
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

    fn delete_edges(&mut self, edges: &Vec<(G, G)>, reverse: bool, vertex_map: &VertexMap<G, I>) -> Vec<(usize, usize)> {
        let offsets_slice = self.offsets.as_slice();

        let mut delete_map = HashMap::<G, HashSet<G>>::new();
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    if dst.index() < self.offsets.len() {
                        let mut set = HashSet::<G>::new();
                        set.insert(*src);
                        delete_map.insert(*dst, set);
                    }
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&src) {
                    set.insert(*dst);
                } else {
                    if src.index() < self.offsets.len() {
                        let mut set = HashSet::<G>::new();
                        set.insert(*dst);
                        delete_map.insert(*src, set);
                    }
                }
            }
        }

        let safe_degree_list = SafeMutPtr::new(&mut self.degree);
        let delete_counter = AtomicUsize::new(0);

        let shuffle_indices: Vec<(usize, usize)> = delete_map
            .par_iter()
            .flat_map(|(v, delete_set)| {
                let mut ret = vec![];
                let deg = safe_degree_list.get_mut()[v.index()];
                let mut found = 0;
                if let Some((_, v_lid)) = vertex_map.get_internal_id(*v) {
                    if deg != 0 {
                        let mut from = offsets_slice[v_lid.index()];
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
                            safe_degree_list.get_mut()[v_lid.index()] -= found;
                            delete_counter.fetch_add(found as usize, Ordering::Relaxed);
                        }
                    }
                }
                ret
            })
            .collect();

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

    fn delete_neighbors(&mut self, neighbors: &HashSet<G>) {
        let offsets_slice = self.offsets.as_slice();
        let degree_slice = self.degree.as_mut_slice();

        let deleted_counter = AtomicUsize::new(0);
        let safe_nbr_list = SafeMutPtr::new(&mut self.neighbors);

        degree_slice
            .par_iter_mut()
            .zip(offsets_slice.par_iter())
            .for_each(|(deg, offset)| {
                let nbr_list = safe_nbr_list.get_mut();
                if *deg > 0 {
                    let mut from = *offset;
                    let mut last = from + *deg as usize - 1;
                    let mut found = 0;

                    loop {
                        while (from < last) && !neighbors.contains(&nbr_list[from]) {
                            from += 1;
                        }
                        if neighbors.contains(&nbr_list[from]) {
                            found += 1;
                        }
                        if from >= last {
                            break;
                        }
                        while (from < last) && (neighbors.contains(&nbr_list[last])) {
                            last -= 1;
                            found += 1;
                        }
                        if from >= last {
                            break;
                        }
                        nbr_list[from] = nbr_list[last];
                        from += 1;
                        last -= 1;
                    }
                    if found > 0 {
                        *deg -= found;
                        deleted_counter.fetch_add(found as usize, Ordering::Relaxed);
                    }
                }
            });

        self.meta[0] -= deleted_counter.load(Ordering::Relaxed);
    }

    fn delete_neighbors_with_ret(&mut self, neighbors: &HashSet<G>) -> Vec<(usize, usize)> {
        let offsets_slice = self.offsets.as_slice();
        let degree_slice = self.degree.as_mut_slice();

        let deleted_counter = AtomicUsize::new(0);
        let safe_nbr_list = SafeMutPtr::new(&mut self.neighbors);

        let shuffle_indices: Vec<(usize, usize)> = degree_slice
            .par_iter_mut()
            .zip(offsets_slice.par_iter())
            .flat_map(|(deg, offset)| {
                let mut ret = vec![];
                let nbr_list = safe_nbr_list.get_mut();
                if *deg > 0 {
                    let mut from = *offset;
                    let mut last = from + *deg as usize - 1;
                    let mut found = 0;

                    loop {
                        while (from < last) && !neighbors.contains(&nbr_list[from]) {
                            from += 1;
                        }
                        if neighbors.contains(&nbr_list[from]) {
                            found += 1;
                        }
                        if from >= last {
                            break;
                        }
                        while (from < last) && (neighbors.contains(&nbr_list[last])) {
                            last -= 1;
                            found += 1;
                        }
                        if from >= last {
                            break;
                        }
                        ret.push((last, from));
                        nbr_list[from] = nbr_list[last];
                        from += 1;
                        last -= 1;
                    }
                    if found > 0 {
                        *deg -= found;
                        deleted_counter.fetch_add(found as usize, Ordering::Relaxed);
                    }
                }

                ret
            })
            .collect();

        self.meta[0] -= deleted_counter.load(Ordering::Relaxed);
        shuffle_indices
    }

    fn insert_edges_beta(
        &mut self, vertex_num: usize, edges: &Vec<(G, G)>, insert_edges_prop: Option<&DataFrame>,
        reverse: bool, edges_prop: Option<&mut Table>, vertex_map: &VertexMap<G, I>, label: LabelId,
    ) {
        let start = Instant::now();
        let old_vertex_num = self.degree.len();
        assert!(old_vertex_num <= vertex_num);
        let parsed_vertices: Vec<I> = if reverse {
            edges.par_iter().map(|edge| {
                if let Some((label_id, lid)) = vertex_map.get_internal_id(edge.1) {
                    if label_id == label {
                        lid
                    } else {
                        <I as IndexType>::max()
                    }
                } else {
                    <I as IndexType>::max()
                }
            }).collect()
        } else {
            edges.par_iter().map(|edge| {
                if let Some((label_id, lid)) = vertex_map.get_internal_id(edge.0) {
                    if label_id == label {
                        lid
                    } else {
                        <I as IndexType>::max()
                    }
                } else {
                    <I as IndexType>::max()
                }
            }).collect()
        };
        let (e_range_diff, new_edges_num) =
            generate_new_offsets(vertex_num, self.degree.as_slice(), &mut self.offsets, &parsed_vertices);
        let t0 = start.elapsed().as_secs_f64();

        let start = Instant::now();
        let pieces = e_range_diff.len();

        self.neighbors
            .inplace_parallel_range_move(new_edges_num, e_range_diff.as_slice());
        let t2 = start.elapsed().as_secs_f64();

        let mut t3 = 0_f64;
        let mut t4 = 0_f64;

        if let Some(it) = insert_edges_prop {
            if let Some(ep) = edges_prop {
                let start = Instant::now();
                ep.inplace_parallel_range_move(new_edges_num, e_range_diff.as_slice());
                t3 = start.elapsed().as_secs_f64();
                let start = Instant::now();
                self.degree.resize(vertex_num);
                self.degree.as_mut_slice()[old_vertex_num..vertex_num]
                    .par_iter_mut()
                    .for_each(|deg| {
                        *deg = 0;
                    });
                let mut insert_offsets = Vec::with_capacity(edges.len());
                if reverse {
                    for i in 0..edges.len() {
                        let v = parsed_vertices[i].index();
                        if v >= vertex_num {
                            insert_offsets.push(usize::MAX);
                        } else {
                            let x = self.degree[v] as usize;
                            self.degree[v] += 1;
                            let offset = self.offsets[v] + x;
                            insert_offsets.push(offset);
                            self.neighbors[offset] = edges[i].0;
                        }
                    }
                } else {
                    for i in 0..edges.len() {
                        let v = parsed_vertices[i].index();
                        if v >= vertex_num {
                            insert_offsets.push(usize::MAX);
                        } else {
                            let x = self.degree[v] as usize;
                            self.degree[v] += 1;
                            let offset = self.offsets[v] + x;
                            insert_offsets.push(offset);
                            self.neighbors[offset] = edges[i].1;
                        }
                    }
                }
                ep.insert_batch(&insert_offsets, it);
                t4 = start.elapsed().as_secs_f64();
            } else {
                panic!("not supposed to reach here...");
            }
        } else {
            let start = Instant::now();
            self.degree.resize(vertex_num);
            self.degree.as_mut_slice()[old_vertex_num..vertex_num]
                .par_iter_mut()
                .for_each(|deg| {
                    *deg = 0;
                });
            if reverse {
                for i in 0..edges.len() {
                    let v = parsed_vertices[i].index();
                    if v < vertex_num {
                        let x = self.degree[v] as usize;
                        self.degree[v] += 1;
                        let offset = self.offsets[v] + x;
                        self.neighbors[offset] = edges[i].0;
                    }
                }
            } else {
                for i in 0..edges.len() {
                    let v = parsed_vertices[i].index();
                    if v < vertex_num {
                        let x = self.degree[v] as usize;
                        self.degree[v] += 1;
                        let offset = self.offsets[v] + x;
                        self.neighbors[offset] = edges[i].1;
                    }
                }
            }
            t4 = start.elapsed().as_secs_f64();
        }

        self.meta[0] = new_edges_num;
        println!(
            "csr::insert_edges_beta: {}, {}, {}, {}, parallel: {}, vnum: {}, enum: {} - {:?}",
            t0,
            t2,
            t3,
            t4,
            pieces,
            self.degree.len(),
            self.neighbors.len(),
            e_range_diff[0],
        );
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}
