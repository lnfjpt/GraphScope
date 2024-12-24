use crate::hack::SafeMutPtr;
use libc::{
    c_void, close, fstat, ftruncate, mmap, mremap, munmap, open, shm_open, stat, MAP_SHARED,
    MREMAP_MAYMOVE, O_CREAT, O_RDWR, PROT_READ, PROT_WRITE, S_IRUSR, S_IWUSR,
};
use rayon::prelude::*;
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::File;
use std::hash::Hash;
use std::io::{Read, Write};
use std::ops::{Index, IndexMut};
use std::os::fd::RawFd;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct SharedVec<T: Copy + Sized> {
    fd: RawFd,
    name: String,

    addr: *mut T,
    size: usize,
}

unsafe impl<T: Copy + Sized> Sync for SharedVec<T> {}
unsafe impl<T: Copy + Sized> Send for SharedVec<T> {}

impl<T> SharedVec<T>
where
    T: Copy + Sized,
{
    pub fn new() -> Self {
        Self { fd: -1, name: "".to_string(), addr: ptr::null_mut(), size: 0 }
    }

    pub fn open(name: &str) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe { shm_open(cstr_name.as_ptr(), O_RDWR, S_IRUSR | S_IWUSR) };
        if fd == -1 {
            println!("shm_open {} failed, {}", name, std::io::Error::last_os_error());
        }

        let mut st: stat = unsafe { std::mem::zeroed() };
        unsafe {
            fstat(fd, &mut st);
        }
        let file_size = st.st_size as usize;

        if file_size != 0 {
            let addr = unsafe {
                mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
            };
            if addr as *mut c_void == libc::MAP_FAILED {
                println!("mmap {}-{} failed, {}", name, file_size, std::io::Error::last_os_error());
            }

            Self { fd, name: name.to_string(), addr, size: file_size / std::mem::size_of::<T>() }
        } else {
            Self { fd, name: name.to_string(), addr: ptr::null_mut() as *mut T, size: 0 }
        }
    }

    pub fn create(name: &str, len: usize) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe { shm_open(cstr_name.as_ptr(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR) };
        if fd == -1 {
            println!("shm_open failed, {}", std::io::Error::last_os_error());
        }
        let file_size = len * std::mem::size_of::<T>();
        let ret = unsafe { ftruncate(fd, file_size as i64) };
        if ret == -1 {
            println!("ftruncate failed, {}", std::io::Error::last_os_error());
        }
        if file_size != 0 {
            let addr = unsafe {
                mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
            };
            if addr as *mut c_void == libc::MAP_FAILED {
                println!("mmap failed, {}", std::io::Error::last_os_error());
            }

            Self { fd, name: name.to_string(), addr, size: len }
        } else {
            Self { fd, name: name.to_string(), addr: ptr::null_mut() as *mut T, size: 0 }
        }
    }

    pub fn load(path: &str, name: &str) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe { shm_open(cstr_name.as_ptr(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR) };
        if fd == -1 {
            println!("shm_open failed, {}", std::io::Error::last_os_error());
        }

        let mut input_file = File::open(path).unwrap();
        let input_file_size = input_file.metadata().unwrap().len() as usize;

        let result = unsafe { ftruncate(fd, input_file_size as i64) };
        if result == -1 {
            println!("ftruncate failed, {}", std::io::Error::last_os_error());
        }

        if input_file_size != 0 {
            let addr = unsafe {
                mmap(ptr::null_mut(), input_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
            };

            if addr as *mut c_void == libc::MAP_FAILED {
                println!("mmap failed...");
            }

            let slice = unsafe { std::slice::from_raw_parts_mut(addr as *mut u8, input_file_size) };
            input_file.read_exact(slice).unwrap();
            Self { fd, name: name.to_string(), addr, size: input_file_size / std::mem::size_of::<T>() }
        } else {
            Self { fd, name: name.to_string(), addr: ptr::null_mut(), size: 0 }
        }
    }

    pub fn dump_vec(name: &str, vec: &Vec<T>) {
        println!("dump to: {}", name);
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe { open(cstr_name.as_ptr(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR) };
        if fd == -1 {
            println!("open failed, {}", std::io::Error::last_os_error());
        }

        let file_size = vec.len() * std::mem::size_of::<T>();
        println!("file_size = {}", file_size);
        let result = unsafe { ftruncate(fd, file_size as i64) };
        if result == -1 {
            println!("ftruncate failed, {}", std::io::Error::last_os_error());
        }

        if file_size != 0 {
            let addr = unsafe {
                mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
            };
            if addr as *mut c_void == libc::MAP_FAILED {
                println!("mmap failed...");
            }

            let dst_slice = unsafe { std::slice::from_raw_parts_mut(addr, vec.len()) };
            let src_slice = vec.as_slice();
            dst_slice.copy_from_slice(src_slice);
        }

        unsafe { close(fd) };
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn as_ptr(&self) -> *const T {
        self.addr
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.addr
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.addr, self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.addr, self.size) }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn resize(&mut self, new_len: usize) {
        let old_file_size = self.len() * std::mem::size_of::<T>();
        let new_file_size = new_len * std::mem::size_of::<T>();
        if new_file_size == old_file_size {
            return;
        } else {
            let result = unsafe { ftruncate(self.fd, new_file_size as i64) };
            if result == -1 {
                println!("ftruncate failed, {}", std::io::Error::last_os_error());
            }
            if self.addr == ptr::null_mut() {
                self.addr = unsafe {
                    mmap(ptr::null_mut(), new_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, self.fd, 0)
                        as *mut T
                };
                if self.addr as *mut c_void == libc::MAP_FAILED {
                    println!("mmap failed, {}", std::io::Error::last_os_error());
                }
            } else {
                let new_addr = unsafe {
                    mremap(self.addr as *mut c_void, old_file_size, new_file_size, MREMAP_MAYMOVE) as *mut T
                };

                self.addr = new_addr;
            }
            self.size = new_len;
        }
    }

    pub fn resize_without_keep_data(&mut self, new_len: usize) {
        if new_len != self.size {
            unsafe {
                munmap(self.addr as *mut c_void, self.size * std::mem::size_of::<T>());
            }
            let file_size = new_len * std::mem::size_of::<T>();
            let ret = unsafe { ftruncate(self.fd, file_size as i64) };
            if ret == -1 {
                println!("ftruncate failed, {}", std::io::Error::last_os_error());
            }
            self.addr = unsafe {
                mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, self.fd, 0) as *mut T
            };
            if self.addr as *mut c_void == libc::MAP_FAILED {
                println!("mmap failed, {}", std::io::Error::last_os_error());
            }
            self.size = new_len;
        }
    }
}

impl<T> SharedVec<T>
where
    T: Copy + Sized + Send + Sync,
{
    pub fn parallel_move(&mut self, indices: &Vec<(usize, usize)>) {
        let safe_mut_slice = SafeMutPtr::new(self);
        indices.par_iter().for_each(|(from, to)| {
            safe_mut_slice.get_mut()[*to] = safe_mut_slice.get_mut()[*from];
        });
    }
    pub fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        assert!(old_degree.len() == old_offsets.len());
        let self_slice = self.as_slice();
        let ret: Vec<T> = self_slice.par_iter().copied().collect();

        for (i, deg) in old_degree.iter().enumerate() {
            let end_offset = new_offsets[i] + *deg as usize;
            if end_offset > new_size {
                panic!("end_offset = {}, new_size = {}", end_offset, new_size);
            }
        }

        self.resize_without_keep_data(new_size);

        let safe_self = SafeMutPtr::new(self);

        let old_edges = ret.as_slice();
        old_degree
            .par_iter()
            .zip(old_offsets.par_iter())
            .enumerate()
            .for_each(|(idx, (deg, offset))| {
                let slice_in = &old_edges[*offset..(*offset + *deg as usize)];
                let slice_out = &mut safe_self.get_mut().as_mut_slice()
                    [new_offsets[idx]..(new_offsets[idx] + *deg as usize)];
                slice_out.copy_from_slice(slice_in);
            });
    }
}

impl<T> SharedVec<T>
where
    T: Copy + Sized + Send + Sync + Eq + Hash,
{
    pub fn parallel_replace_within(&mut self, set: &HashSet<T>, val: T) {
        let ms = unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) };
        ms.par_iter_mut().for_each(|x| {
            if set.contains(x) {
                *x = val;
            }
        });
    }

    pub fn parallel_replace_within_and_count(&mut self, set: &HashSet<T>, val: T) -> usize {
        let ms = unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) };
        let counter = AtomicUsize::new(0);
        ms.par_iter_mut().for_each(|x| {
            if set.contains(x) {
                *x = val;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        counter.load(Ordering::Relaxed)
    }
}

impl<T: Copy + Sized> Drop for SharedVec<T> {
    fn drop(&mut self) {
        unsafe {
            munmap(self.addr as *mut c_void, self.size * std::mem::size_of::<T>());
            close(self.fd);
        }
    }
}

impl<T: Copy + Sized> Index<usize> for SharedVec<T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.addr.add(index) }
    }
}

impl<T: Copy + Sized> IndexMut<usize> for SharedVec<T> {
    #[inline(always)]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        unsafe { &mut *self.addr.add(index) }
    }
}

pub struct SharedStringVec {
    offset: SharedVec<usize>,
    length: SharedVec<u16>,
    content: SharedVec<u8>,
}

impl SharedStringVec {
    pub fn load(path: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_offset", path).as_str(), format!("{}_offset", name).as_str());
        SharedVec::<u16>::load(format!("{}_length", path).as_str(), format!("{}_length", name).as_str());
        SharedVec::<u8>::load(format!("{}_content", path).as_str(), format!("{}_content", name).as_str());
    }

    pub fn open(name: &str) -> Self {
        Self {
            offset: SharedVec::<usize>::open(format!("{}_offset", name).as_str()),
            length: SharedVec::<u16>::open(format!("{}_length", name).as_str()),
            content: SharedVec::<u8>::open(format!("{}_content", name).as_str()),
        }
    }

    pub fn dump_vec(name: &str, str_vec: &Vec<String>) {
        let mut offset_vec = Vec::<usize>::with_capacity(str_vec.len());
        let mut length_vec = Vec::<u16>::with_capacity(str_vec.len());
        let mut content_vec = vec![];
        for s in str_vec.iter() {
            offset_vec.push(content_vec.len());
            length_vec.push(s.as_bytes().len() as u16);
            content_vec.write_all(s.as_bytes()).unwrap();
        }

        SharedVec::<usize>::dump_vec(format!("{}_offset", name).as_str(), &offset_vec);
        SharedVec::<u16>::dump_vec(format!("{}_length", name).as_str(), &length_vec);
        SharedVec::<u8>::dump_vec(format!("{}_content", name).as_str(), &content_vec);
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if index < self.offset.len() {
            let begin = self.offset[index];
            let end = begin + self.length[index] as usize;
            let begin_ptr = unsafe { self.content.as_ptr().add(begin) };
            let slice = unsafe { std::slice::from_raw_parts(begin_ptr, end - begin) };
            match std::str::from_utf8(slice) {
                Ok(s) => Some(s),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &str {
        let begin = self.offset[index];
        let end = begin + self.length[index] as usize;
        let begin_ptr = unsafe { self.content.as_ptr().add(begin) };
        let slice = unsafe { std::slice::from_raw_parts(begin_ptr, end - begin) };
        unsafe { std::str::from_utf8_unchecked(slice) }
    }

    pub fn len(&self) -> usize {
        self.offset.len()
    }

    pub fn parallel_move(&mut self, indices: &Vec<(usize, usize)>) {
        self.offset.parallel_move(indices);
        self.length.parallel_move(indices);
    }

    pub fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.offset
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
        self.length
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    pub fn resize(&mut self, new_len: usize) {
        let old_size = self.offset.len();
        self.offset.resize(new_len);
        self.length.resize(new_len);
        if old_size < new_len {
            for i in old_size..new_len {
                self.length[i] = 0 as u16;
            }
        }
    }

    pub fn batch_set(&mut self, indices: &Vec<usize>, v: &Vec<String>) {
        // TODO: parallelize
        assert!(indices.len() == v.len());
        let old_size = self.content.len();
        let mut new_size = old_size;
        for (i, idx) in indices.iter().enumerate() {
            if *idx != usize::MAX {
                self.offset[*idx] = new_size;
                self.length[*idx] = v[i].len() as u16;
                new_size += v[i].len();
            }
        }

        self.content.resize(new_size);
        let mut sl = &mut self.content.as_mut_slice()[old_size..new_size];
        for (idx, s) in v.iter().enumerate() {
            if indices[idx] != usize::MAX {
                sl.write_all(s.as_bytes()).unwrap();
            }
        }
    }
}

impl Index<usize> for SharedStringVec {
    type Output = str;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        self.get_unchecked(index)
    }
}
