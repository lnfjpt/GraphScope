use crate::graph::IndexType;
use shm_container::SharedVec;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::thread::panicking;

const INITIAL_SIZE: usize = 16;
const MAX_LOAD_FACTOR: f64 = 0.875;
pub struct Indexer<K: Copy + Sized> {
    keys: SharedVec<K>,
    indices: SharedVec<usize>,
}

unsafe impl<K: Copy + Sized> Sync for Indexer<K> {}
unsafe impl<K: Copy + Sized> Send for Indexer<K> {}

fn hash_integer<T: Hash>(value: T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

impl<K: Default + Eq + Copy + Sized + IndexType> Indexer<K> {
    pub fn load(prefix: &str, name: &str) {
        SharedVec::<K>::load(format!("{}_keys", prefix).as_str(), format!("{}_keys", name).as_str());
        SharedVec::<usize>::load(
            format!("{}_indices", prefix).as_str(),
            format!("{}_indices", name).as_str(),
        );
    }

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
        if index < self.keys.len() {
            Some(self.keys[index])
            // if self.keys[index] == <K as IndexType>::max() {
            //     return None;
            // } else {
            //     Some(self.keys[index])
            // }
        } else {
            None
        }
    }

    pub fn get_index(&self, key: K) -> Option<usize> {
        let hash = hash_integer(key.index());
        let len = self.indices.len();
        let mut index = (hash as usize) % len;
        loop {
            let i = self.indices[index];
            if i == usize::MAX {
                return None;
            }
            if self.keys[i] == key {
                return Some(i);
            }
            index = (index + 1) % len;
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn erase_indices(&mut self, indices: &Vec<usize>) -> usize {
        let mut num = 0_usize;
        for v in indices.iter() {
            if self.keys.len() > *v && self.keys[*v] != <K as IndexType>::max() {
                self.keys[*v] = <K as IndexType>::max();
                num += 1;
            }
        }
        num
    }

    pub fn insert_batch(&mut self, id_list: &Vec<K>) -> Vec<usize> {
        assert!(self.keys.len() + id_list.len() < self.indices.len());

        let old_keys_num = self.keys.len();
        self.keys.resize(old_keys_num + id_list.len());

        let indices_len = self.indices.len();

        let mut cur_lid = old_keys_num;

        let mut ret = vec![];

        for v in id_list.iter() {
            let hash = hash_integer(v.index());
            let mut index = (hash as usize) % indices_len;
            loop {
                if self.indices[index] == usize::MAX {
                    break;
                } else{
                    if self.keys[self.indices[index]] == *v {
                        break;
                    }
                }
                index = (index + 1) % indices_len;
            }
            // while self.indices[index] != usize::MAX && self.keys[self.indices[index]] != *v {
            //     index = (index + 1) % indices_len;
            // }
            if self.indices[index] == usize::MAX {
                self.indices[index] = cur_lid;
                self.keys[cur_lid] = *v;
                cur_lid += 1;
            }
            assert!(self.keys[self.indices[index]] == *v);
            ret.push(self.indices[index]);
        }

        self.keys.resize(cur_lid);

        ret
    }
}
