use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::ptr;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::col_table::ColTable;
use crate::csr::{CsrBuildError, CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

pub struct BatchMutableCsr<I> {
    pub neighbors: Vec<I>,
    pub offsets: Vec<usize>,
    pub degree: Vec<i32>,

    edge_num: usize,
}

pub struct BatchMutableCsrBuilder<I> {
    neighbors: Vec<I>,
    offsets: Vec<usize>,
    insert_offsets: Vec<i32>,

    edge_num: usize,
}

impl<I: IndexType> BatchMutableCsrBuilder<I> {
    pub fn new() -> Self {
        BatchMutableCsrBuilder {
            neighbors: Vec::new(),
            offsets: Vec::new(),
            insert_offsets: Vec::new(),
            edge_num: 0,
        }
    }

    pub fn init(&mut self, degree: &Vec<i32>, _: f64) {
        let vertex_num = degree.len();
        let mut edge_num = 0_usize;
        for i in 0..vertex_num {
            edge_num += degree[i] as usize;
        }
        self.edge_num = 0;

        self.neighbors.resize(edge_num, I::new(0));
        self.offsets.resize(vertex_num, 0);
        self.insert_offsets.resize(vertex_num, 0);

        let mut offset = 0_usize;
        for i in 0..vertex_num {
            self.insert_offsets[i] = 0;
            self.offsets[i] = offset;
            offset += degree[i] as usize;
        }
    }

    pub fn put_edge(&mut self, src: I, dst: I) -> Result<usize, CsrBuildError> {
        let offset = self.offsets[src.index()] + self.insert_offsets[src.index()] as usize;
        self.neighbors[offset] = dst;
        self.insert_offsets[src.index()] += 1;
        self.edge_num += 1;
        Ok(offset)
    }

    pub fn finish(self) -> Result<BatchMutableCsr<I>, CsrBuildError> {
        Ok(BatchMutableCsr {
            neighbors: self.neighbors,
            offsets: self.offsets,
            degree: self.insert_offsets,
            edge_num: self.edge_num,
        })
    }
}

struct SafePtr<I>(*const I, PhantomData<I>);
unsafe impl<I> Send for SafePtr<I> {}
unsafe impl<I> Sync for SafePtr<I> {}

impl<I> Clone for SafePtr<I> {
    fn clone(&self) -> Self {
        SafePtr(self.0.clone(), PhantomData)
    }
}

impl<I> Copy for SafePtr<I> {}

struct SafeMutPtr<I>(*mut I, PhantomData<I>);
unsafe impl<I> Send for SafeMutPtr<I> {}
unsafe impl<I> Sync for SafeMutPtr<I> {}

impl<I> Clone for SafeMutPtr<I> {
    fn clone(&self) -> Self {
        SafeMutPtr(self.0.clone(), PhantomData)
    }
}

impl<I> Copy for SafeMutPtr<I> {}

impl<I: IndexType> BatchMutableCsr<I> {
    pub fn new() -> Self {
        BatchMutableCsr { neighbors: Vec::new(), offsets: Vec::new(), degree: Vec::new(), edge_num: 0 }
    }

    pub fn insert_edges(&self, vertex_num: usize, edges: &Vec<(I, I)>, reverse: bool, p: u32) -> Self {
        let t0 = Instant::now();
        let mut new_degree = vec![0; vertex_num];

        let ta = Instant::now();
        if reverse {
            for e in edges.iter() {
                new_degree[e.1.index()] += 1;
            }
        } else {
            for e in edges.iter() {
                new_degree[e.0.index()] += 1;
            }
        }
        let ta = ta.elapsed().as_secs_f32();

        let num_threads = p as usize;

        let tb = Instant::now();
        let old_vertex_num = self.offsets.len();
        let chunk_size = (vertex_num + num_threads - 1) / num_threads;

        let mut thread_offset = vec![0_usize; num_threads];
        let safe_thread_offset_ptr = SafeMutPtr(thread_offset.as_mut_ptr(), PhantomData);
        let mut new_offsets = vec![0_usize; vertex_num + 1];
        let new_offset_ptr = new_offsets.as_mut_ptr();
        let safe_new_offset_ptr = SafeMutPtr(new_offset_ptr, PhantomData);
        let new_degree_ptr = new_degree.as_mut_ptr();
        let safe_degree_ptr = SafeMutPtr(new_degree_ptr, PhantomData);

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = (start_idx + chunk_size).min(vertex_num);
                s.spawn(move |_| unsafe {
                    let offset_ptr = safe_new_offset_ptr.clone();
                    let toffset_ptr = safe_thread_offset_ptr.clone();
                    let d_ptr = safe_degree_ptr.clone();
                    let mut local_offset = 0_usize;
                    if end_idx > old_vertex_num {
                        if start_idx >= old_vertex_num {
                            for v in start_idx..end_idx {
                                *(offset_ptr.0.add(v)) = local_offset;
                                local_offset += (*d_ptr.0.add(v)) as usize;
                                *d_ptr.0.add(v) = 0;
                            }
                        } else {
                            for v in start_idx..old_vertex_num {
                                *(offset_ptr.0.add(v)) = local_offset;
                                local_offset += (*d_ptr.0.add(v) + self.degree[v]) as usize;
                                *d_ptr.0.add(v) = self.degree[v];
                            }
                            for v in old_vertex_num..end_idx {
                                *(offset_ptr.0.add(v)) = local_offset;
                                local_offset += (*d_ptr.0.add(v)) as usize;
                                *d_ptr.0.add(v) = 0;
                            }
                        }
                    } else {
                        for v in start_idx..end_idx {
                            *(offset_ptr.0.add(v)) = local_offset;
                            local_offset += (*d_ptr.0.add(v) + self.degree[v]) as usize;
                            *d_ptr.0.add(v) = self.degree[v];
                        }
                    }
                    *toffset_ptr.0.add(i) = local_offset;
                });
            }
        });
        let mut cur_offset = 0_usize;
        for i in 0..num_threads {
            let tmp = thread_offset[i] + cur_offset;
            thread_offset[i] = cur_offset;
            cur_offset = tmp;
        }

        let tb = tb.elapsed().as_secs_f32();

        let tc = Instant::now();
        let mut new_neighbors = vec![I::new(0); cur_offset];

        let new_neighbor_ptr = new_neighbors.as_mut_ptr();
        let old_neighbor_ptr = self.neighbors.as_ptr();

        let safe_neighbor_ptr = SafeMutPtr(new_neighbor_ptr, PhantomData);
        let safe_old_neighbor_ptr = SafePtr(old_neighbor_ptr, PhantomData);
        let tc = tc.elapsed().as_secs_f32();

        let t1 = Instant::now();
        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = (start_idx + chunk_size).min(vertex_num);
                let local_offset = thread_offset[i];
                s.spawn(move |_| unsafe {
                    let on_ptr = safe_old_neighbor_ptr.clone();
                    let n_ptr = safe_neighbor_ptr.clone();
                    let offset_ptr = safe_new_offset_ptr.clone();
                    if end_idx > old_vertex_num {
                        if start_idx >= old_vertex_num {
                            for v in start_idx..end_idx {
                                *offset_ptr.0.add(v) += local_offset;
                            }
                        } else {
                            for v in start_idx..old_vertex_num {
                                let offset = (*offset_ptr.0.add(v)) + local_offset;
                                *offset_ptr.0.add(v) = offset;

                                let from = on_ptr.0.add(self.offsets[v]);
                                let to = n_ptr.0.add(offset);
                                let deg = self.degree[v];
                                ptr::copy(from, to, deg as usize);
                            }
                            for v in old_vertex_num..end_idx {
                                *offset_ptr.0.add(v) += local_offset;
                            }
                        }
                    } else {
                        for v in start_idx..end_idx {
                            let offset = (*offset_ptr.0.add(v)) + local_offset;
                            *offset_ptr.0.add(v) = offset;

                            let from = on_ptr.0.add(self.offsets[v]);
                            let to = n_ptr.0.add(offset);
                            let deg = self.degree[v];
                            ptr::copy(from, to, deg as usize);
                        }
                    }
                });
            }
        });
        let t1 = t1.elapsed().as_secs_f64();

        let td = Instant::now();
        if reverse {
            for (src, dst) in edges.iter() {
                let offset = new_offsets[dst.index()] + new_degree[dst.index()] as usize;
                new_degree[dst.index()] += 1;
                new_neighbors[offset] = *src;
            }
        } else {
            for (src, dst) in edges.iter() {
                let offset = new_offsets[src.index()] + new_degree[src.index()] as usize;
                new_degree[src.index()] += 1;
                new_neighbors[offset] = *dst;
            }
        }
        let td = td.elapsed().as_secs_f32();

        let t0 = t0.elapsed().as_secs_f64();
        println!("csr parallel percent: {} / {} = {}", t1, t0, t1 / t0);
        println!("a = {}, b = {}, c = {}, d = {}", ta, tb, tc, td);

        Self { neighbors: new_neighbors, offsets: new_offsets, degree: new_degree, edge_num: cur_offset }
    }
}

unsafe impl<I: IndexType> Send for BatchMutableCsr<I> {}
unsafe impl<I: IndexType> Sync for BatchMutableCsr<I> {}

impl<I: IndexType> CsrTrait<I> for BatchMutableCsr<I> {
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
            self.degree[u] as usize
        }
    }

    fn serialize(&self, path: &String) {
        let file = File::create(path).unwrap();
        let mut writer = BufWriter::new(file);
        info!("edge_num = {}", self.edge_num);
        writer
            .write_u64::<LittleEndian>(self.edge_num as u64)
            .unwrap();

        info!("neighbor_size = {}", self.neighbors.len());
        writer
            .write_u64::<LittleEndian>(self.neighbors.len() as u64)
            .unwrap();
        for i in 0..self.neighbors.len() {
            self.neighbors[i].write(&mut writer).unwrap();
        }
        info!("offset_size = {}", self.offsets.len());
        writer
            .write_u64::<LittleEndian>(self.offsets.len() as u64)
            .unwrap();
        for i in 0..self.offsets.len() {
            writer
                .write_u64::<LittleEndian>(self.offsets[i] as u64)
                .unwrap();
        }
        info!("degree_size = {}", self.degree.len());
        writer
            .write_u64::<LittleEndian>(self.degree.len() as u64)
            .unwrap();
        for i in 0..self.degree.len() {
            writer
                .write_i32::<LittleEndian>(self.degree[i])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn deserialize(&mut self, path: &String) {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);

        self.edge_num = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("edge_num = {}", self.edge_num);

        let neighbor_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("neighbor_size = {}", neighbor_size);
        self.neighbors = Vec::with_capacity(neighbor_size);
        for _ in 0..neighbor_size {
            self.neighbors
                .push(I::read(&mut reader).unwrap());
        }

        let offset_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("offset_size = {}", offset_size);
        self.offsets = Vec::with_capacity(offset_size);
        for _ in 0..offset_size {
            self.offsets
                .push(reader.read_u64::<LittleEndian>().unwrap() as usize);
        }

        let degree_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("degree_size = {}", degree_size);
        self.degree = Vec::with_capacity(degree_size);
        for _ in 0..degree_size {
            self.degree
                .push(reader.read_i32::<LittleEndian>().unwrap());
        }
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start = self.offsets[u];
            let end = self.offsets[u] + self.degree[u] as usize;
            Some(NbrIter::new(&self.neighbors, start, end))
        }
    }

    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start = self.offsets[u];
            let end = self.offsets[u] + self.degree[u] as usize;
            Some(NbrOffsetIter::new(&self.neighbors, start, end))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        for vertex in vertices {
            let vertex = vertex.index();
            if vertex >= self.degree.len() {
                continue;
            }
            self.edge_num -= self.degree[vertex] as usize;
            self.degree[vertex] = 0;
        }
    }

    fn delete_edges(&mut self, edges: &HashSet<(I, I)>, reverse: bool) {
        if reverse {
            let mut src_set = HashSet::new();
            for (_, src) in edges {
                let src = src.index();
                if src >= self.vertex_num().index() {
                    continue;
                }
                src_set.insert(src);
            }
            for src in src_set {
                let mut offset = self.offsets[src];
                let mut end = self.offsets[src] + self.degree[src] as usize;
                while offset < (end - 1) {
                    if edges.contains(&(self.neighbors[offset], I::new(src))) {
                        self.neighbors[offset] = self.neighbors[end - 1];
                        end -= 1;
                    } else {
                        offset += 1;
                    }
                }
                if edges.contains(&(self.neighbors[end - 1], I::new(src))) {
                    end -= 1;
                }
                let new_degree = end - self.offsets[src];
                let degree_diff = self.degree[src] as usize - new_degree;
                self.degree[src] = new_degree as i32;
                self.edge_num -= degree_diff;
            }
        } else {
            let mut src_set = HashSet::new();
            for (src, _) in edges {
                let src = src.index();
                if src >= self.vertex_num().index() {
                    continue;
                }
                src_set.insert(src);
            }
            for src in src_set {
                let mut offset = self.offsets[src];
                let mut end = self.offsets[src] + self.degree[src] as usize;
                while offset < (end - 1) {
                    if edges.contains(&(I::new(src), self.neighbors[offset])) {
                        self.neighbors[offset] = self.neighbors[end - 1];
                        end -= 1;
                    } else {
                        offset += 1;
                    }
                }
                if edges.contains(&(I::new(src), self.neighbors[end - 1])) {
                    end -= 1;
                }
                let new_degree = end - self.offsets[src];
                let degree_diff = self.degree[src] as usize - new_degree;
                self.degree[src] = new_degree as i32;
                self.edge_num -= degree_diff;
            }
        }
    }

    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, p: u32) {
        let mut delete_map: HashMap<usize, HashSet<usize>> = HashMap::new();
        let mut keys = vec![];
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst.index()) {
                    set.insert(src.index());
                } else {
                    let mut set = HashSet::new();
                    set.insert(src.index());
                    delete_map.insert(dst.index(), set);
                    keys.push(*dst);
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&src.index()) {
                    set.insert(dst.index());
                } else {
                    let mut set = HashSet::new();
                    set.insert(dst.index());
                    delete_map.insert(src.index(), set);
                    keys.push(*src);
                }
            }
        }
        keys.sort();

        let offsets = self.offsets.as_ptr();
        let degree = self.degree.as_mut_ptr();
        let neighbors = self.neighbors.as_mut_ptr();

        let safe_offsets = SafePtr(offsets, PhantomData);
        let safe_degree = SafeMutPtr(degree, PhantomData);
        let safe_neighbors = SafeMutPtr(neighbors, PhantomData);
        let safe_keys = SafePtr(keys.as_ptr(), PhantomData);

        let keys_size = keys.len();
        let num_threads = p as usize;
        let chunk_size = (keys_size + num_threads - 1) / num_threads;

        let mut thread_deleted_edges = vec![0_usize; num_threads];

        let safe_delete_map = SafePtr(&delete_map as *const HashMap<usize, HashSet<usize>>, PhantomData);
        let safe_tde = SafeMutPtr(thread_deleted_edges.as_mut_ptr(), PhantomData);

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = keys_size.min(start_idx + chunk_size);
                s.spawn(move |_| unsafe {
                    let keys_ptr = safe_keys.clone();
                    let offsets_ptr = safe_offsets.clone();
                    let degree_ptr = safe_degree.clone();
                    let nbr_ptr = safe_neighbors.clone();
                    let tde_ptr = safe_tde.clone();

                    let delete_map = safe_delete_map.clone();
                    let delete_map_ref: &HashMap<usize, HashSet<usize>> = &*delete_map.0;
                    let mut deleted_edges = 0;
                    for v_index in start_idx..end_idx {
                        let v = *keys_ptr.0.add(v_index);
                        let mut offset = *offsets_ptr.0.add(v.index());
                        let deg = *degree_ptr.0.add(v.index());

                        let set = delete_map_ref.get(&v.index()).unwrap();
                        let mut end = offset + deg as usize;
                        while offset < (end - 1) {
                            let nbr = *(nbr_ptr.0.add(offset));
                            if set.contains(&nbr.index()) {
                                *(nbr_ptr.0.add(offset)) = *(nbr_ptr.0.add(end - 1));
                                end -= 1;
                            } else {
                                offset += 1;
                            }
                        }
                        let nbr = *(nbr_ptr.0.add(end - 1));
                        if set.contains(&nbr.index()) {
                            end -= 1;
                        }

                        let new_deg = (end - *offsets_ptr.0.add(v.index())) as i32;
                        *degree_ptr.0.add(v.index()) = new_deg;

                        deleted_edges += (deg - new_deg) as usize;
                    }

                    *tde_ptr.0.add(i) = deleted_edges;
                });
            }
        });

        for v in thread_deleted_edges.iter() {
            self.edge_num -= *v;
        }
    }

    fn delete_edges_with_props(&mut self, edges: &HashSet<(I, I)>, reverse: bool, table: &mut ColTable) {
        if reverse {
            let mut src_set = HashSet::new();
            for (_, src) in edges {
                let src = src.index();
                if src >= self.vertex_num().index() {
                    continue;
                }
                src_set.insert(src);
            }
            for src in src_set {
                let mut offset = self.offsets[src];
                let mut end = self.offsets[src] + self.degree[src] as usize;
                while offset < (end - 1) {
                    if edges.contains(&(self.neighbors[offset], I::new(src))) {
                        self.neighbors[offset] = self.neighbors[end - 1];
                        let row = table.get_row(end - 1).unwrap();
                        table.insert(offset, &row);

                        end -= 1;
                    } else {
                        offset += 1;
                    }
                }
                if edges.contains(&(self.neighbors[end - 1], I::new(src))) {
                    end -= 1;
                }
                let new_degree = end - self.offsets[src];
                let degree_diff = self.degree[src] as usize - new_degree;
                self.degree[src] = new_degree as i32;
                self.edge_num -= degree_diff;
            }
        } else {
            let mut src_set = HashSet::new();
            for (src, _) in edges {
                let src = src.index();
                if src >= self.vertex_num().index() {
                    continue;
                }
                src_set.insert(src);
            }
            for src in src_set {
                let mut offset = self.offsets[src];
                let mut end = self.offsets[src] + self.degree[src] as usize;
                while offset < (end - 1) {
                    if edges.contains(&(I::new(src), self.neighbors[offset])) {
                        self.neighbors[offset] = self.neighbors[end - 1];
                        let row = table.get_row(end - 1).unwrap();
                        table.insert(offset, &row);

                        end -= 1;
                    } else {
                        offset += 1;
                    }
                }
                if edges.contains(&(I::new(src), self.neighbors[end - 1])) {
                    end -= 1;
                }
                let new_degree = end - self.offsets[src];
                let degree_diff = self.degree[src] as usize - new_degree;
                self.degree[src] = new_degree as i32;
                self.edge_num -= degree_diff;
            }
        }
    }

    fn parallel_delete_edges_with_props(
        &mut self, edges: &Vec<(I, I)>, reverse: bool, table: &mut ColTable, p: u32,
    ) {
        let mut delete_map: HashMap<usize, HashSet<usize>> = HashMap::new();
        let mut keys = vec![];
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst.index()) {
                    set.insert(src.index());
                } else {
                    let mut set = HashSet::new();
                    set.insert(src.index());
                    delete_map.insert(dst.index(), set);
                    keys.push(*dst);
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&src.index()) {
                    set.insert(dst.index());
                } else {
                    let mut set = HashSet::new();
                    set.insert(dst.index());
                    delete_map.insert(src.index(), set);
                    keys.push(*src);
                }
            }
        }
        keys.sort();

        let offsets = self.offsets.as_ptr();
        let degree = self.degree.as_mut_ptr();
        let neighbors = self.neighbors.as_mut_ptr();

        let safe_offsets = SafePtr(offsets, PhantomData);
        let safe_degree = SafeMutPtr(degree, PhantomData);
        let safe_neighbors = SafeMutPtr(neighbors, PhantomData);
        let safe_keys = SafePtr(keys.as_ptr(), PhantomData);
        let safe_table = SafeMutPtr(table as *mut ColTable, PhantomData);

        let keys_size = keys.len();
        let num_threads = p as usize;
        let chunk_size = (keys_size + num_threads - 1) / num_threads;

        let mut thread_deleted_edges = vec![0_usize; num_threads];

        let safe_delete_map = SafePtr(&delete_map as *const HashMap<usize, HashSet<usize>>, PhantomData);
        let safe_tde = SafeMutPtr(thread_deleted_edges.as_mut_ptr(), PhantomData);

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = keys_size.min(start_idx + chunk_size);
                s.spawn(move |_| unsafe {
                    let keys_ptr = safe_keys.clone();
                    let offsets_ptr = safe_offsets.clone();
                    let degree_ptr = safe_degree.clone();
                    let nbr_ptr = safe_neighbors.clone();
                    let table_ptr = safe_table.clone();
                    let tde_ptr = safe_tde.clone();

                    let delete_map = safe_delete_map.clone();
                    let delete_map_ref: &HashMap<usize, HashSet<usize>> = &*delete_map.0;
                    let mut deleted_edges = 0;
                    let table_ref: &mut ColTable = &mut *table_ptr.0;
                    for v_index in start_idx..end_idx {
                        let v = *keys_ptr.0.add(v_index);
                        let mut offset = *offsets_ptr.0.add(v.index());
                        let deg = *degree_ptr.0.add(v.index());

                        let set = delete_map_ref.get(&v.index()).unwrap();
                        let mut end = offset + deg as usize;
                        while offset < (end - 1) {
                            let nbr = *(nbr_ptr.0.add(offset));
                            if set.contains(&nbr.index()) {
                                *(nbr_ptr.0.add(offset)) = *(nbr_ptr.0.add(end - 1));
                                let row = table_ref.get_row(end - 1).unwrap();
                                table_ref.insert(offset, &row);
                                end -= 1;
                            } else {
                                offset += 1;
                            }
                        }
                        let nbr = *(nbr_ptr.0.add(end - 1));
                        if set.contains(&nbr.index()) {
                            end -= 1;
                        }

                        let new_deg = (end - *offsets_ptr.0.add(v.index())) as i32;
                        *degree_ptr.0.add(v.index()) = new_deg;

                        deleted_edges += (deg - new_deg) as usize;
                    }

                    *tde_ptr.0.add(i) = deleted_edges;
                });
            }
        });

        for v in thread_deleted_edges.iter() {
            self.edge_num -= *v;
        }
    }
}
