use crate::csr_trait::SafeMutPtr;
use libc::{
    c_void, close, fstat, ftruncate, mmap, mremap, munmap, shm_open, stat, MAP_SHARED,
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

pub struct PtrWrapper<T> {
    pub inner: *const T,
}

unsafe impl<T> Send for PtrWrapper<T> {}
unsafe impl<T> Sync for PtrWrapper<T> {}

pub struct MutPtrWrapper<T> {
    pub inner: *mut T,
}

unsafe impl<T> Send for MutPtrWrapper<T> {}
unsafe impl<T> Sync for MutPtrWrapper<T> {}

pub struct SharedVec<T: Copy + Sized> {
    fd: RawFd,
    name: String,

    ptr: MutPtrWrapper<T>,
    size: usize,
}

impl<T> SharedVec<T>
where
    T: Copy + Sized,
{
    pub fn new() -> Self {
        Self { fd: -1, name: "".to_string(), ptr: MutPtrWrapper { inner: ptr::null_mut() }, size: 0 }
    }

    pub fn open(name: &str) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe { shm_open(cstr_name.as_ptr(), O_RDWR, S_IRUSR | S_IWUSR) };
        if fd == -1 {
            println!("shm_open failed, {}", std::io::Error::last_os_error());
        }

        let mut st: stat = unsafe { std::mem::zeroed() };
        unsafe {
            fstat(fd, &mut st);
        }
        let file_size = st.st_size as usize;

        let addr = unsafe {
            mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
        };
        if addr as *mut c_void == libc::MAP_FAILED {
            println!("mmap failed, {}", std::io::Error::last_os_error());
        }

        Self {
            fd,
            name: name.to_string(),
            ptr: MutPtrWrapper { inner: addr },
            size: file_size / std::mem::size_of::<T>(),
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
        let addr = unsafe {
            mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
        };
        if addr as *mut c_void == libc::MAP_FAILED {
            println!("mmap failed, {}", std::io::Error::last_os_error());
        }

        Self { fd, name: name.to_string(), ptr: MutPtrWrapper { inner: addr }, size: len }
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
            Self {
                fd,
                name: name.to_string(),
                ptr: MutPtrWrapper { inner: addr },
                size: input_file_size / std::mem::size_of::<T>(),
            }
        } else {
            Self { fd, name: name.to_string(), ptr: MutPtrWrapper { inner: ptr::null_mut() }, size: 0 }
        }
    }

    pub fn dump_vec(name: &str, vec: &Vec<T>) {
        let mut v = Self::create(name, vec.len());
        for i in 0..vec.len() {
            v[i] = vec[i];
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn as_ptr(&self) -> *const T {
        self.ptr.inner
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.inner
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.inner, self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.inner, self.size) }
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
            let new_addr = unsafe {
                mremap(self.ptr.inner as *mut c_void, old_file_size, new_file_size, MREMAP_MAYMOVE)
                    as *mut T
            };
            self.ptr.inner = new_addr;
            self.size = new_len;
        }
    }

    pub fn resize_without_keep_data(&mut self, new_len: usize) {
        if new_len != self.size {
            unsafe {
                munmap(
                    self.ptr.inner as *mut c_void,
                    self.size * std::mem::size_of::<T>(),
                );
            }
            let file_size = new_len * std::mem::size_of::<T>();
            let ret = unsafe { ftruncate(self.fd, file_size as i64) };
            if ret == -1 {
                println!("ftruncate failed, {}", std::io::Error::last_os_error());
            }
            self.ptr.inner = unsafe {
                mmap(
                    ptr::null_mut(),
                    file_size,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    self.fd,
                    0,
                ) as *mut T
            };
            if self.ptr.inner as *mut c_void == libc::MAP_FAILED {
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
    pub fn inplace_parallel_shuffle(&mut self, offsets: &Vec<usize>) {
        let sl = unsafe { std::slice::from_raw_parts(self.ptr.inner, self.len()) };
        let result_vec: Vec<T> = offsets.par_iter().map(|&idx| sl[idx]).collect();

        self.resize(offsets.len());
        let target_slice = unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), offsets.len()) };
        result_vec
            .par_iter()
            .zip(target_slice.par_iter_mut())
            .for_each(|(&b_item, a_item)| {
                *a_item = b_item;
            });
    }

    pub fn parallel_shuffle(&self, name: &str, offsets: &Vec<usize>) -> Self {
        let num = offsets.len();
        let mut ret = Self::create(name, num);
        let ms = unsafe { std::slice::from_raw_parts_mut(ret.as_mut_ptr(), num) };
        let sl = unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) };

        offsets
            .par_iter()
            .zip(ms.par_iter_mut())
            .for_each(|(&idx, slot)| {
                if idx < sl.len() {
                    *slot = sl[idx];
                }
            });

        ret
    }

    pub fn parallel_move(&mut self, indices: &Vec<(usize, usize)>) {
        let safe_mut_slice = SafeMutPtr::new(self);
        indices.par_iter().for_each(|(from, to)| {
            safe_mut_slice.get_mut()[*to] = safe_mut_slice.get_mut()[*from];
        });
    }
    pub fn inplace_parallel_chunk_move(
        &mut self,
        new_size: usize,
        old_offsets: &[usize],
        old_degree: &[i32],
        new_offsets: &[usize],
    ) {
        assert!(old_degree.len() == old_offsets.len());
        let self_slice = self.as_slice();
        let ret: Vec<T> = self_slice.par_iter().copied().collect();

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
            munmap(self.ptr.inner as *mut c_void, self.size * std::mem::size_of::<T>());
            close(self.fd);
        }
    }
}

impl<T: Copy + Sized> Index<usize> for SharedVec<T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.ptr.inner.add(index) }
    }
}

impl<T: Copy + Sized> IndexMut<usize> for SharedVec<T> {
    #[inline(always)]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        unsafe { &mut *self.ptr.inner.add(index) }
    }
}

pub struct SharedStringVec {
    offset: SharedVec<usize>,
    content: SharedVec<u8>,
}

impl SharedStringVec {
    pub fn load(path: &str, name: &str) {
        SharedVec::<usize>::load(format!("{}_offset", path).as_str(), format!("{}_offset", name).as_str());
        SharedVec::<u8>::load(format!("{}_content", path).as_str(), format!("{}_content", name).as_str());
    }

    pub fn open(name: &str) -> Self {
        Self {
            offset: SharedVec::<usize>::open(format!("{}_offset", name).as_str()),
            content: SharedVec::<u8>::open(format!("{}_content", name).as_str()),
        }
    }

    pub fn dump_vec(name: &str, str_vec: &Vec<String>) {
        let mut offset_vec = Vec::<usize>::with_capacity(str_vec.len() + 1);
        let mut content_vec = vec![];
        offset_vec.push(0);
        for s in str_vec.iter() {
            content_vec.write_all(s.as_bytes()).unwrap();
            offset_vec.push(content_vec.len());
        }

        SharedVec::<usize>::dump_vec(format!("{}_offset", name).as_str(), &offset_vec);
        SharedVec::<u8>::dump_vec(format!("{}_content", name).as_str(), &content_vec);
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if index < self.offset.len() - 1 {
            let begin = self.offset[index];
            let end = self.offset[index + 1];
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
        let end = self.offset[index + 1];
        let begin_ptr = unsafe { self.content.as_ptr().add(begin) };
        let slice = unsafe { std::slice::from_raw_parts(begin_ptr, end - begin) };
        unsafe { std::str::from_utf8_unchecked(slice) }
    }

    pub fn len(&self) -> usize {
        if self.offset.len() <= 1 {
            0
        } else {
            self.offset.len() - 1
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
