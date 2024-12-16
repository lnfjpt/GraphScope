use std::any::Any;
use std::collections::{HashMap, HashSet};

use bmcsr::csr::{SafeMutPtr, SafePtr};

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter};
use crate::vector::{SharedMutVec, SharedVec};
use crate::graph::IndexType;
use crate::table::Table;

pub struct Csr<I: Copy + Sized> {
    neighbors: SharedVec<I>,
    offsets: SharedVec<usize>,
    degree: SharedVec<i32>,

    edge_num: usize,
}

impl<I: IndexType> Csr<I> {
    pub fn load(prefix: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_meta", prefix).as_str(), format!("{}_meta", name).as_str());
        SharedVec::<I>::load(format!("{}_nbrs", prefix).as_str(), format!("{}_nbrs", name).as_str());
        SharedVec::<usize>::load(format!("{}_offsets", prefix).as_str(), format!("{}_offsets", name).as_str());
        SharedVec::<i32>::load(format!("{}_degree", prefix).as_str(), format!("{}_degree", name).as_str());
    }

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

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        let mut mut_vec = SharedMutVec::<i32>::open(self.degree.name());
        for vertex in vertices {
            let vertex = vertex.index();
            if vertex >= mut_vec.len() {
                continue;
            }
            self.edge_num -= mut_vec[vertex] as usize;
            mut_vec[vertex] = 0;
        }
    }

    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, table: Option<&mut Table>, p: u32,
    nbr_vertices: Option<&HashSet<I>>) {
        let mut delete_map = HashMap::<I, HashSet<I>>::new();
        let mut keys = vec![];
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    if dst.index() < self.offsets.len() {
                        let mut set = HashSet::<I>::new();
                        set.insert(*src);
                        delete_map.insert(*dst, set);
                        keys.push(*dst);
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
                        keys.push(*src);
                    }
                }
            }
        }
        keys.sort();

        let mut degree_list = SharedMutVec::<i32>::open(self.degree.name());
        let mut nbr_list = SharedMutVec::<I>::open(self.neighbors.name());

        let safe_offsets_ptr = SafePtr::new(&self.offsets);
        let safe_degree_ptr = SafeMutPtr::new(&mut degree_list);
        let safe_neighbors_ptr = SafeMutPtr::new(&mut nbr_list);
        let safe_keys_ptr = SafePtr::new(&keys);

        let keys_size = keys.len();
        let num_threads = p as usize;
        let chunk_size = (keys_size + num_threads - 1) / num_threads;
        let nbr_chunk_size = (self.degree.len() + num_threads - 1) / num_threads;

        let mut thread_deleted_edges = vec![0_usize; num_threads];

        let safe_delete_map_ptr = SafePtr::new(&delete_map);
        let safe_tde_ptr = SafeMutPtr::new(&mut thread_deleted_edges);

        if let Some(table) = table {
            let safe_table_ptr = SafeMutPtr::new(table);

            rayon::scope(|s| {
                for i in 0..num_threads {
                    let start_idx = i * chunk_size;
                    let end_idx = keys_size.min(start_idx + chunk_size);
                    let nbr_start_idx = i * nbr_chunk_size;
                    let nbr_end_idx = self.offsets.len().min(nbr_start_idx + nbr_chunk_size);
                    s.spawn(move |_| {
                        let keys_ref = safe_keys_ptr.get_ref();
                        let offsets_ref = safe_offsets_ptr.get_ref();
                        let degree_ref = safe_degree_ptr.get_mut();
                        let neighbors_ref = safe_neighbors_ptr.get_mut();
                        let table_ref = safe_table_ptr.get_mut();
                        let tde_ref = safe_tde_ptr.get_mut();
                        let delete_map_ref = safe_delete_map_ptr.get_ref();
                        let mut deleted_edges = 0;
                        let mut shuffle_indices = vec![];
                        for v_index in start_idx..end_idx {
                            let v = keys_ref[v_index];
                            let mut offset = offsets_ref[v.index()];
                            let deg = degree_ref[v.index()];

                            let set = delete_map_ref.get(&v).unwrap();
                            let mut end = offset + deg as usize;
                            while offset < (end - 1) {
                                let nbr = neighbors_ref[offset];
                                if set.contains(&nbr) {
                                    neighbors_ref[offset] = neighbors_ref[end - 1];
                                    // table_ref.move_row(end - 1, offset);
                                    shuffle_indices.push((end - 1, offset));
                                    end -= 1;
                                } else {
                                    offset += 1;
                                }
                            }
                            let nbr = neighbors_ref[end - 1];
                            if set.contains(&nbr) {
                                end -= 1;
                            }

                            let new_deg = (end - offsets_ref[v.index()]) as i32;
                            degree_ref[v.index()] = new_deg;

                            deleted_edges += (deg - new_deg) as usize;
                        }

                        if let Some(nbr_set) = nbr_vertices {
                            for index in nbr_start_idx..nbr_end_idx {
                                let mut offset = offsets_ref[index];
                                let deg = degree_ref[index];
                                if deg == 0 {
                                    continue;
                                }

                                let mut end = offset + deg as usize;
                                while offset < (end - 1) {
                                    let nbr = neighbors_ref[offset];
                                    if nbr_set.contains(&nbr) {
                                        neighbors_ref[offset] = neighbors_ref[end - 1];
                                        // table_ref.move_row(end - 1, offset);
                                        shuffle_indices.push((end - 1, offset));
                                        end -= 1;
                                    } else {
                                        offset += 1;
                                    }
                                }
                                let nbr = neighbors_ref[end - 1];
                                if nbr_set.contains(&nbr) {
                                    end -= 1;
                                }

                                let new_deg = (end - offsets_ref[index]) as i32;
                                degree_ref[index] = new_deg;

                                deleted_edges += (deg - new_deg) as usize;
                            }
                        }

                        tde_ref[i] = deleted_edges;
                        table_ref.reshuffle_rows(&shuffle_indices);
                    });
                }
            });
        } else {
            rayon::scope(|s| {
                for i in 0..num_threads {
                    let start_idx = i * chunk_size;
                    let end_idx = keys_size.min(start_idx + chunk_size);
                    let nbr_start_idx = i * nbr_chunk_size;
                    let nbr_end_idx = self.offsets.len().min(nbr_start_idx + nbr_chunk_size);

                    s.spawn(move |_| {
                        let keys_ref = safe_keys_ptr.get_ref();
                        let offsets_ref = safe_offsets_ptr.get_ref();
                        let degree_ref = safe_degree_ptr.get_mut();
                        let neighbors_ref = safe_neighbors_ptr.get_mut();
                        let tde_ref = safe_tde_ptr.get_mut();
                        let delete_map_ref = safe_delete_map_ptr.get_ref();
                        let mut deleted_edges = 0;
                        for v_index in start_idx..end_idx {
                            let v = keys_ref[v_index];
                            let mut offset = offsets_ref[v.index()];
                            let deg = degree_ref[v.index()];

                            let set = delete_map_ref.get(&v).unwrap();
                            let mut end = offset + deg as usize;
                            while offset < (end - 1) {
                                let nbr = neighbors_ref[offset];
                                if set.contains(&nbr) {
                                    neighbors_ref[offset] = neighbors_ref[end - 1];
                                    end -= 1;
                                } else {
                                    offset += 1;
                                }
                            }
                            let nbr = neighbors_ref[end - 1];
                            if set.contains(&nbr) {
                                end -= 1;
                            }

                            let new_deg = (end - offsets_ref[v.index()]) as i32;
                            degree_ref[v.index()] = new_deg;

                            deleted_edges += (deg - new_deg) as usize;
                        }

                        if let Some(nbr_set) = nbr_vertices {
                            for index in nbr_start_idx..nbr_end_idx {
                                let mut offset = offsets_ref[index];
                                let deg = degree_ref[index];
                                if deg == 0 {
                                    continue;
                                }

                                let mut end = offset + deg as usize;
                                while offset < (end - 1) {
                                    let nbr = neighbors_ref[offset];
                                    if nbr_set.contains(&nbr) {
                                        neighbors_ref[offset] = neighbors_ref[end - 1];
                                        end -= 1;
                                    } else {
                                        offset += 1;
                                    }
                                }
                                let nbr = neighbors_ref[end - 1];
                                if nbr_set.contains(&nbr) {
                                    end -= 1;
                                }

                                let new_deg = (end - offsets_ref[index]) as i32;
                                degree_ref[index] = new_deg;

                                deleted_edges += (deg - new_deg) as usize;
                            }
                        }

                        tde_ref[i] = deleted_edges;
                    });
                }
            });
        }

        for v in thread_deleted_edges.iter() {
            self.edge_num -= *v;
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}