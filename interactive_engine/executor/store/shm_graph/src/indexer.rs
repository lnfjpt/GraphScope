use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use rayon::prelude::*;
use shm_container::SharedVec;

use crate::graph::IndexType;

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
    pub fn load(prefix: &str, name: &str) -> Self {
        let keys = SharedVec::<K>::load(
            format!("{}_keys", prefix).as_str(),
            format!("{}_keys", name).as_str(),
        );
        let indices_path = PathBuf::from(format!("{}_indices", prefix));
        let indices = if indices_path.exists() {
            SharedVec::<usize>::load(
                format!("{}_indices", prefix).as_str(),
                format!("{}_indices", name).as_str(),
            )
        } else {
            let mut len = INITIAL_SIZE;
            for _ in 0..64 {
                let rate = keys.len() as f64 / len as f64;
                if rate > MAX_LOAD_FACTOR {
                    len *= 2;
                } else {
                    break;
                }
            }

            let mut indices = SharedVec::<usize>::create(format!("{}_indices", name).as_str(), len);
            indices.as_mut_slice().par_iter_mut().for_each(|val| {
                *val = usize::MAX;
            });
            for (i, id) in keys.as_slice().iter().enumerate() {
                let hash = hash_integer(id.index());
                let mut index = (hash as usize) % len;
                while indices[index] != usize::MAX {
                    index = (index + 1) % len;
                }
                indices[index] = i;
            }

            indices
        };
        Self { keys, indices }
    }

    pub fn open(prefix: &str) -> Self {
        let keys = SharedVec::<K>::open(&format!("{}_keys", prefix));
        let indices = SharedVec::<usize>::open(&format!("{}_indices", prefix));
        Self { keys, indices }
    }

    pub fn dump(prefix: &str, id_list: &Vec<K>, lite: bool) {
        SharedVec::dump_vec(&format!("{}_keys", prefix), &id_list);
        if !lite {
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
    }

    pub fn new(prefix: &str, id_list: Vec<K>) -> Self {
        Self::dump(prefix, &id_list, false);
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

    fn rehash_to(&mut self, new_len: usize) {
        let mut cur_indices_size = self.indices.len().max(INITIAL_SIZE);
        loop {
            let rate = new_len as f64 / cur_indices_size as f64;
            if rate > MAX_LOAD_FACTOR {
                cur_indices_size *= 2;
            } else {
                break;
            }
        }

        self.indices.resize(cur_indices_size);
        self.indices
            .as_mut_slice()
            .par_iter_mut()
            .for_each(|x| *x = usize::MAX);

        for i in 0..self.keys.len() {
            let hash = hash_integer(self.keys[i].index());
            let mut index = (hash as usize) % cur_indices_size;
            while self.indices[index] != usize::MAX {
                index = (index + 1) % cur_indices_size;
            }
            self.indices[index] = i;
        }
    }

    pub fn insert_batch(&mut self, id_list: Vec<K>) -> Vec<usize> {
        if self.keys.len() + id_list.len() >= self.indices.len() {
            self.rehash_to(self.keys.len() + id_list.len());
        }
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
                } else {
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

    pub fn insert_batch_beta(&mut self, id_list: Vec<K>) -> Vec<usize> {
        if self.keys.len() + id_list.len() >= self.indices.len() {
            self.rehash_to(self.keys.len() + id_list.len());
        }
        assert!(self.keys.len() + id_list.len() < self.indices.len());

        let indices_len = self.indices.len();

        let mut ret: Vec<usize> = id_list
            .par_iter()
            .map(|v| {
                let hash = hash_integer(v.index());
                let mut index = (hash as usize) % indices_len;
                loop {
                    if self.indices[index] == usize::MAX {
                        break;
                    } else {
                        if self.keys[self.indices[index]] == *v {
                            break;
                        }
                    }
                    index = (index + 1) % indices_len;
                }
                index
            })
            .collect();

        let mut cur_lid = self.keys.len();
        self.keys.resize(cur_lid + id_list.len());

        for (i, v) in id_list.iter().enumerate() {
            let mut cur = ret[i];
            if self.indices[cur] == usize::MAX {
                self.indices[cur] = cur_lid;
                self.keys[cur_lid] = *v;
                ret[i] = cur_lid;
                cur_lid += 1;
            } else if self.keys[self.indices[cur]] == *v {
                ret[i] = self.indices[cur];
            } else {
                while self.indices[cur] != usize::MAX && self.keys[self.indices[cur]] != *v {
                    cur = (cur + 1) % indices_len;
                }
                if self.indices[cur] == usize::MAX {
                    self.indices[cur] = cur_lid;
                    self.keys[cur_lid] = *v;
                    ret[i] = cur_lid;
                    cur_lid += 1;
                } else {
                    ret[i] = self.indices[cur];
                }
            }
        }

        self.keys.resize(cur_lid);

        ret
    }
}
