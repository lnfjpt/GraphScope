use memmap2::{Mmap, MmapMut};
use std::ops::Index;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
};

struct PtrWrapper<T> {
    pub inner: *const T,
}

unsafe impl<T> Send for PtrWrapper<T> {}
unsafe impl<T> Sync for PtrWrapper<T> {}
pub struct SharedVec<T: Copy + Sized> {
    data: Mmap,
    ptr: PtrWrapper<T>,
    size: usize,
}

impl<T> SharedVec<T>
where
    T: Copy + Sized,
{
    pub fn open(name: &str) -> Self {
        println!("opening file {}", name);
        let file = File::open(name).unwrap();
        let data = unsafe { Mmap::map(&file).unwrap() };
        let ptr = PtrWrapper { inner: data.as_ptr() as *const T};
        let size = data.len() / std::mem::size_of::<T>();
        Self { data, ptr, size }
    }

    pub fn dump_vec(name: &str, vec: &Vec<T>) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name)
            .unwrap();
        file.set_len((vec.len() * std::mem::size_of::<T>()) as u64)
            .unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let src_slice = vec.as_slice();
        let dst_slice = &mut mmap[..] as &mut [u8];
        unsafe {
            std::ptr::copy_nonoverlapping(
                src_slice.as_ptr() as *const u8,
                dst_slice.as_mut_ptr(),
                vec.len() * std::mem::size_of::<T>(),
            );
        }
    }

    pub fn get(&self, index: usize) -> Option<T> {
        if index < self.size {
            Some(unsafe { *self.ptr.inner.add(index) })
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> T {
        unsafe { *self.ptr.inner.add(index) }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn as_ptr(&self) -> *const T {
        self.ptr.inner
    }
}

impl<T: Copy + Sized> Index<usize> for SharedVec<T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.ptr.inner.add(index) }
    }
}

pub struct SharedStringVec {
    offset: SharedVec<usize>,
    content: SharedVec<u8>,
}

impl SharedStringVec {
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
            let begin = self.offset.get_unchecked(index);
            let end = self.offset.get_unchecked(index + 1);
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
        let begin = self.offset.get_unchecked(index);
        let end = self.offset.get_unchecked(index + 1);
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