use std::any::Any;
use std::fs::File;

use crate::csr::{CsrBuildError, CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use pegasus_common::codec::{ReadExt, WriteExt};

pub struct BatchMutableCsr<I> {
    neighbors: Vec<I>,
    offsets: Vec<usize>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
    edge_capacity: usize,
}

pub struct BatchMutableCsrBuilder<I> {
    neighbors: Vec<I>,
    offsets: Vec<usize>,
    insert_offsets: Vec<usize>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
    edge_capacity: usize,
}

impl<I: IndexType> BatchMutableCsrBuilder<I> {
    pub fn new() -> Self {
        BatchMutableCsrBuilder {
            neighbors: Vec::new(),
            offsets: Vec::new(),
            insert_offsets: Vec::new(),
            vertex_num: 0,
            edge_num: 0,
            vertex_capacity: 0,
            edge_capacity: 0,
        }
    }

    pub fn init(&mut self, degree: &Vec<i64>, reserve_rate: f64) {
        let vertex_num = degree.len();
        let mut edge_num = 0_usize;
        for i in 0..vertex_num {
            edge_num += degree[i] as usize;
        }

        self.vertex_num = vertex_num;
        self.edge_num = edge_num;

        self.vertex_capacity = vertex_num * reserve_rate as usize;
        self.edge_capacity = edge_num * reserve_rate as usize;

        self.neighbors
            .resize(self.edge_capacity, I::new(0));
        self.offsets.resize(self.vertex_capacity + 1, 0);
        self.insert_offsets.resize(self.vertex_num, 0);

        let mut offset = 0_usize;
        for i in 0..vertex_num {
            self.insert_offsets[i] = offset;
            self.offsets[i] = offset;
            offset += degree[i] as usize;
        }
        self.offsets[vertex_num] = offset;
    }

    pub fn put_edge(&mut self, src: I, dst: I) -> Result<usize, CsrBuildError> {
        let offset = self.insert_offsets[src.index()];
        if offset >= self.offsets[src.index() + 1] {
            return Err(CsrBuildError::OffsetOutOfCapacity);
        }
        self.neighbors[offset] = dst;
        self.insert_offsets[src.index()] += 1;
        Ok(offset)
    }

    pub fn finish(self) -> Result<BatchMutableCsr<I>, CsrBuildError> {
        for i in 0..self.vertex_num {
            if self.insert_offsets[i] != self.offsets[i + 1] {
                return Err(CsrBuildError::UnfinishedVertex);
            }
        }
        Ok(BatchMutableCsr {
            neighbors: self.neighbors,
            offsets: self.offsets,
            vertex_num: self.vertex_num,
            edge_num: self.edge_num,
            vertex_capacity: self.vertex_capacity,
            edge_capacity: self.edge_capacity,
        })
    }
}

impl<I: IndexType> BatchMutableCsr<I> {
    pub fn new() -> Self {
        BatchMutableCsr {
            neighbors: Vec::new(),
            offsets: Vec::new(),
            vertex_num: 0,
            edge_num: 0,
            vertex_capacity: 0,
            edge_capacity: 0,
        }
    }
}

unsafe impl<I: IndexType> Send for BatchMutableCsr<I> {}
unsafe impl<I: IndexType> Sync for BatchMutableCsr<I> {}

impl<I: IndexType> CsrTrait<I> for BatchMutableCsr<I> {
    fn vertex_num(&self) -> I {
        I::new(self.vertex_num)
    }

    fn edge_num(&self) -> usize {
        self.edge_num
    }

    fn degree(&self, u: I) -> usize {
        let u = u.index();
        (self.offsets[u + 1] - self.offsets[u]) as usize
    }

    fn serialize(&self, path: &String) {
        let mut file = File::create(path).unwrap();
        file.write_u64(self.vertex_num as u64).unwrap();
        file.write_u64(self.edge_num as u64).unwrap();
        file.write_u64(self.vertex_capacity as u64)
            .unwrap();
        file.write_u64(self.edge_capacity as u64)
            .unwrap();

        file.write_u64(self.neighbors.len() as u64)
            .unwrap();
        for i in 0..self.neighbors.len() {
            file.write_u64(self.neighbors[i].index() as u64)
                .unwrap();
        }

        file.write_u64(self.offsets.len() as u64)
            .unwrap();
        for i in 0..self.offsets.len() {
            file.write_u64(self.offsets[i] as u64).unwrap();
        }
    }

    fn deserialize(&mut self, path: &String) {
        let mut file = File::open(path).unwrap();
        self.vertex_num = file.read_u64().unwrap() as usize;
        self.edge_num = file.read_u64().unwrap() as usize;
        self.vertex_capacity = file.read_u64().unwrap() as usize;
        self.edge_capacity = file.read_u64().unwrap() as usize;

        let neighbor_size = file.read_u64().unwrap() as usize;
        self.neighbors
            .resize_with(neighbor_size, || I::new(0));
        for i in 0..neighbor_size {
            self.neighbors[i] = I::new(file.read_u64().unwrap() as usize);
        }

        let offset_size = file.read_u64().unwrap() as usize;
        self.offsets.resize_with(offset_size, || 0);
        for i in 0..offset_size {
            self.offsets[i] = file.read_u64().unwrap() as usize;
        }
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        let u = u.index();
        if u >= self.vertex_num {
            None
        } else {
            Some(NbrIter::new(
                &self.neighbors,
                self.offsets[u],
                self.offsets[u + 1],
            ))
        }
    }

    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>> {
        let u = u.index();
        if u >= self.vertex_num {
            None
        } else {
            Some(NbrOffsetIter::new(
                &self.neighbors,
                self.offsets[u],
                self.offsets[u + 1],
            ))
        }
    }

    fn as_any(&self) -> &dyn Any { self }
}
