use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::usize;

use rayon::prelude::*;

use crate::csr_trait::{CsrTrait, NbrIter, NbrOffsetIter, SafeMutPtr};
use crate::graph::IndexType;
use crate::table::Table;
use crate::types::LabelId;
use crate::vertex_map::{self, VertexMap};
use shm_container::SharedVec;

pub struct SCsr<G: Copy + Sized, I: Copy + Sized> {
    nbr_list: SharedVec<G>,

    // meta[0]: vertex_num
    // meta[1]: edge_num
    meta: SharedVec<usize>,
    ph: PhantomData<I>,
}

impl<G: IndexType, I: IndexType> SCsr<G, I> {
    pub fn new() -> Self {
        Self { nbr_list: SharedVec::<G>::new(), meta: SharedVec::<usize>::new(), ph: PhantomData }
    }

    pub fn load(prefix: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_meta", prefix).as_str(), format!("{}_meta", name).as_str());
        SharedVec::<G>::load(format!("{}_nbrs", prefix).as_str(), format!("{}_nbrs", name).as_str());
    }

    pub fn open(prefix: &str) -> Self {
        Self {
            nbr_list: SharedVec::<G>::open(format!("{}_nbrs", prefix).as_str()),
            meta: SharedVec::<usize>::open(format!("{}_meta", prefix).as_str()),
            ph: PhantomData,
        }
    }

    pub fn get_edge(&self, src: I) -> Option<G> {
        let nbr = self.nbr_list[src.index()];
        if nbr == <G as IndexType>::max() {
            None
        } else {
            Some(nbr)
        }
    }

    pub fn get_edge_with_offset(&self, src: I) -> Option<(G, usize)> {
        let nbr = self.nbr_list[src.index()];
        if nbr == <G as IndexType>::max() {
            None
        } else {
            Some((nbr, src.index()))
        }
    }
}

unsafe impl<G: IndexType, I: IndexType> Send for SCsr<G, I> {}

unsafe impl<G: IndexType, I: IndexType> Sync for SCsr<G, I> {}

impl<G: IndexType, I: IndexType> CsrTrait<G, I> for SCsr<G, I> {
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
        (self.nbr_list[u.index()] == <G as IndexType>::max()) as usize
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<G>> {
        if self.nbr_list[u.index()] == <G as IndexType>::max() {
            None
        } else {
            Some(NbrIter::new(unsafe { self.nbr_list.as_ptr().add(u.index()) }, unsafe {
                self.nbr_list.as_ptr().add(u.index() + 1)
            }))
        }
    }
    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<G>> {
        if self.nbr_list[u.index()] == <G as IndexType>::max() {
            None
        } else {
            Some(NbrOffsetIter::new(
                unsafe { self.nbr_list.as_ptr().add(u.index()) },
                unsafe { self.nbr_list.as_ptr().add(u.index() + 1) },
                u.index(),
            ))
        }
    }

    fn delete_edges(&mut self, edges: &Vec<(G, G)>, reverse: bool, vertex_map: &VertexMap<G, I>) -> Vec<(usize, usize)> {
        let mut delete_map = HashMap::<G, HashSet<G>>::new();
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    let mut set = HashSet::<G>::new();
                    set.insert(*src);
                    delete_map.insert(*dst, set);
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&src) {
                    set.insert(*dst);
                } else {
                    let mut set = HashSet::<G>::new();
                    set.insert(*dst);
                    delete_map.insert(*src, set);
                }
            }
        }

        let safe_nbr_list = SafeMutPtr::new(&mut self.nbr_list);

        let deleted_counter = AtomicUsize::new(0);
        delete_map
            .par_iter()
            .for_each(|(v, delete_set)| {
                if let Some((_, v_lid)) = vertex_map.get_internal_id(*v) {
                    let nbr = safe_nbr_list.get_mut()[v_lid.index()];
                    if nbr != <G as IndexType>::max() && delete_set.contains(&nbr) {
                        safe_nbr_list.get_mut()[v_lid.index()] = <G as IndexType>::max();
                        deleted_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

        self.meta[1] -= deleted_counter.load(Ordering::Relaxed);
        vec![]
    }

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        let vnum = self.vertex_num();
        for vertex in vertices {
            if *vertex < vnum {
                self.nbr_list[vertex.index()] = <G as IndexType>::max();
            }
        }
    }

    fn delete_neighbors(&mut self, neighbors: &HashSet<G>) {
        let nbrs_slice = self.nbr_list.as_mut_slice();
        let deleted_counter = AtomicUsize::new(0);

        nbrs_slice.par_iter_mut().for_each(|nbr| {
            if *nbr != <G as IndexType>::max() {
                if neighbors.contains(nbr) {
                    *nbr = <G as IndexType>::max();
                    deleted_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        self.meta[1] -= deleted_counter.load(Ordering::Relaxed);
    }

    fn delete_neighbors_with_ret(&mut self, neighbors: &HashSet<G>) -> Vec<(usize, usize)> {
        self.delete_neighbors(neighbors);
        vec![]
    }

    fn insert_edges_beta(
        &mut self, vertex_num: usize, edges: &Vec<(G, G)>,
        insert_edges_prop: Option<&crate::dataframe::DataFrame>, reverse: bool,
        edges_prop: Option<&mut Table>, vertex_map: &VertexMap<G, I>, label: LabelId,
    ) {
        let start = Instant::now();
        let old_length = self.nbr_list.len();
        self.nbr_list.resize(vertex_num);
        for i in old_length..vertex_num {
            self.nbr_list[i] = <G as IndexType>::max();
        }
        let t0 = start.elapsed().as_secs_f64();

        let mut t1 = 0_f64;
        let mut t2 = 0_f64;
        let mut t3 = 0_f64;
        let mut insert_counter = 0;
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
        if let Some(it) = insert_edges_prop {
            let start = Instant::now();
            let mut insert_offsets = Vec::with_capacity(edges.len());
            if reverse {
                for i in 0..edges.len() {
                    let v = parsed_vertices[i].index();
                    if v >= vertex_num {
                        insert_offsets.push(usize::MAX);
                    } else {
                        if self.nbr_list[v] == <G as IndexType>::max() {
                            insert_counter += 1;
                        }
                        self.nbr_list[v] = edges[i].0;
                        insert_offsets.push(v);
                    }
                }
            } else {
                for i in 0..edges.len() {
                    let v = parsed_vertices[i].index();
                    if v >= vertex_num {
                        insert_offsets.push(usize::MAX);
                    } else {
                        if self.nbr_list[v] == <G as IndexType>::max() {
                            insert_counter += 1;
                        }
                        self.nbr_list[v] = edges[i].1;
                        insert_offsets.push(v);
                    }
                }
            }
            t1 = start.elapsed().as_secs_f64();

            let start = Instant::now();
            if let Some(ep) = edges_prop {
                ep.resize(vertex_num);
                ep.insert_batch(&insert_offsets, it);
            }
            t2 = start.elapsed().as_secs_f64();
        } else {
            let start = Instant::now();
            if reverse {
                for i in 0..edges.len() {
                    let v = parsed_vertices[i].index();
                    if v < vertex_num {
                        if self.nbr_list[v] == <G as IndexType>::max() {
                            insert_counter += 1;
                        }
                        self.nbr_list[v] = edges[i].0;
                    }
                }
            } else {
                for i in 0..edges.len() {
                    let v = parsed_vertices[i].index();
                    if v < vertex_num {
                        if self.nbr_list[v] == <G as IndexType>::max() {
                            insert_counter += 1;
                        }
                        self.nbr_list[v] = edges[i].1;
                    }
                }
            }
            t3 = start.elapsed().as_secs_f64();
        }

        self.meta[0] = vertex_num;
        self.meta[1] += insert_counter;
        println!("scsr::insert_edges: {}, {}, {}, {}", t0, t1, t2, t3);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}
