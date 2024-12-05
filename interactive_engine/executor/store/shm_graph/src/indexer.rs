use crate::vector::SharedVec;
use crate::graph::IndexType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const INITIAL_SIZE: usize = 16;
const MAX_LOAD_FACTOR: f64 = 0.875;
pub struct Indexer<K : Copy + Sized> {
    keys: SharedVec<K>,
    indices: SharedVec<usize>,
}

unsafe impl<K : Copy + Sized> Sync for Indexer<K> {}
unsafe impl<K : Copy + Sized> Send for Indexer<K> {}

fn hash_integer<T: Hash>(value: T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

impl <K: Default + Eq + Copy + Sized + IndexType> Indexer<K> {
    pub fn open(prefix: &str) -> Self {
        let keys = SharedVec::<K>::open(&format!("{}_keys", prefix));
        let indices = SharedVec::<usize>::open(&format!("{}_indices", prefix));
        Self { keys, indices }
    }

    pub fn dump(prefix: &str, id_list: &Vec<K>) {
        SharedVec::dump_vec(&format!("{}_keys", prefix), &id_list);
        let mut len = INITIAL_SIZE;
        for _ in 0..64 {
            let rate = id_list.len() as f64 / len as f64;
            if rate > MAX_LOAD_FACTOR {
                len *= 2;
            } else {
                break;
            }
        }
        println!("len = {}, input.len() = {}", len, id_list.len());

        let mut indices = vec![usize::MAX; len];

        for (i, id) in id_list.iter().enumerate() {
            let hash = hash_integer(id.index());
            let mut index = (hash as usize) % len;
            while indices[index] != usize::MAX {
                index = (index + 1) % len;
            }
            indices[index] = i;
        }
        SharedVec::dump_vec(&format!("{}_indices", prefix), &indices);
    }

    pub fn new(prefix: &str, id_list: Vec<K>) -> Self {
        Self::dump(prefix, &id_list);
        Self {
            keys: SharedVec::<K>::open(&format!("{}_keys", prefix)),
            indices: SharedVec::<usize>::open(&format!("{}_indices", prefix)),
        }
    }

    pub fn get_key(&self, index: usize) -> Option<K> {
        self.keys.get(index)
    }

    pub fn get_index(&self, key: K) -> Option<usize> {
        let hash = hash_integer(key.index());
        let len = self.indices.len();
        let mut index = (hash as usize) % len;
        loop {
            let i = self.indices.get_unchecked(index);
            if i == usize::MAX {
                return None;
            }
            if self.keys.get_unchecked(i) == key {
                return Some(i);
            }
            index = (index + 1) % len;
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
}