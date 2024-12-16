use crate::vector::{MutPtrWrapper, SharedMutVec, SharedVec};
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
    pub fn load(prefix: &str, name: &str) {
        SharedVec::<K>::load(format!("{}_keys", prefix).as_str(), format!("{}_keys", name).as_str());
        SharedVec::<usize>::load(format!("{}_indices", prefix).as_str(), format!("{}_indices", name).as_str());
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

    pub fn erase_indices(&mut self, indices: &Vec<usize>) -> usize {
        let mut mut_keys = SharedMutVec::<K>::open(self.keys.name());
        let mut num = 0_usize;
        for v in indices.iter() {
            if mut_keys.len() <= *v && mut_keys[*v] != <K as IndexType>::max() {
                mut_keys[*v] = <K as IndexType>::max();
                num += 1;
            }
        }
        num
    }

    pub fn insert_batch(&mut self, id_list: &Vec<K>) -> Vec<usize> {
        assert!(self.keys.len() + id_list.len() < self.indices.len());

        let mut mut_keys = SharedMutVec::<K>::open(self.keys.name());
        let mut mut_indices = SharedMutVec::<usize>::open(self.indices.name());

        let old_keys_num = mut_keys.len();
        mut_keys.resize(old_keys_num + id_list.len());

        let indices_len = mut_indices.len();

        let mut cur_lid = old_keys_num;

        let mut ret = vec![];

        for v in id_list.iter() {
            let hash = hash_integer(v.index());
            let mut index = (hash as usize) % indices_len;
            while mut_indices[index] != usize::MAX && mut_keys[mut_indices[index]] == *v  {
                index = (index + 1) % indices_len;
            }
            if mut_keys[mut_indices[index]] == usize::MAX {
                mut_indices[index] = cur_lid;
                mut_keys[cur_lid] = *v;
                cur_lid += 1;
            }
            ret.push(mut_indices[index]);
        }

        mut_keys.resize(cur_lid);

        ret
    }
}