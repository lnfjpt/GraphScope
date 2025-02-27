use memmap2::{MmapMut, MmapOptions};

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};
use std::path::Path;
use std::ptr;

pub struct MmapVec<T: Copy + Sized> {
    mmap: MmapMut,
    length: usize,
    capacity: usize,
    filename: String,

    ph: PhantomData<T>,
}

unsafe impl<T: Copy + Sized> Sync for MmapVec<T> {}

unsafe impl<T: Copy + Sized> Send for MmapVec<T> {}

impl<T> MmapVec<T>
    where
        T: Copy + Sized,
{
    pub fn new(filename: &str, initial_capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;

        let mmap_capacity = std::mem::size_of::<T>() * initial_capacity;
        let mmap = unsafe { MmapOptions::new().len(mmap_capacity).map_mut(&file)? };

        Ok(Self {
            mmap,
            length: 0,
            capacity: initial_capacity,
            filename: filename.to_string(),
            ph: PhantomData,
        })
    }

    pub fn load(path: &str, name: &str) {
        std::fs::copy(Path::new(&path), Path::new(&name)).expect("Failed to load mmap vec file");
    }

    pub fn open(filename: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(filename)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let length = mmap.len();
        let capacity = length;
        Ok(Self {
            mmap,
            length,
            capacity,
            filename: filename.to_string(),
            ph: PhantomData,
        })
    }

    pub fn len(&self) -> usize { self.length }

    pub fn as_ptr(&self) -> *const T {
        let ptr = self.mmap.as_ptr() as *const T;
        ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        let ptr = self.mmap.as_mut_ptr() as *mut T;
        ptr
    }

    pub fn as_slice(&self) -> &[T] {
        let ptr = self.mmap.as_ptr() as *const T;
        unsafe { std::slice::from_raw_parts(ptr, self.length) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        let ptr = self.mmap.as_mut_ptr() as *mut T;
        unsafe { std::slice::from_raw_parts_mut(ptr, self.length) }
    }

    pub fn resize(&mut self, new_len: usize) {
        let old_file_size = self.len() * std::mem::size_of::<T>();
        let new_file_size = new_len * std::mem::size_of::<T>();
        if new_file_size == old_file_size {
            return;
        } else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.filename).expect("Failed to open file");
            file.set_len(new_file_size as u64).expect("Failed to set file length");
            self.mmap = unsafe { MmapOptions::new().map_mut(&file).expect("Failed to create mmap") };
        }
    }
}

impl<T: Copy + Sized> Index<usize> for MmapVec<T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        let ptr = self.mmap.as_ptr() as *const T;
        unsafe { &*ptr.add(index) }
    }
}

impl<T: Copy + Sized> IndexMut<usize> for MmapVec<T> {
    #[inline(always)]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let ptr = self.mmap.as_mut_ptr() as *mut T;
        unsafe { &mut *ptr.add(index) }
    }
}

pub struct MmapStringVec {
    offset_mmap: MmapMut,
    length_mmap: MmapMut,
    content_mmap: MmapMut,
    filename: String,
}

impl MmapStringVec {
    pub fn load(path: &str, name: &str) {
        let offset_filename = path.to_owned() + "_offset";
        let length_filename = path.to_owned() + "_length";
        let content_filename = path.to_owned() + "_content";
        let target_offset_filename = name.to_owned() + "_offset";
        let target_length_filename = name.to_owned() + "_length";
        let target_content_filename = name.to_owned() + "_content";
        std::fs::copy(Path::new(&offset_filename), Path::new(&target_offset_filename)).expect("Failed to load offset file");
        std::fs::copy(Path::new(&length_filename), Path::new(&target_length_filename)).expect("Failed to load length file");
        std::fs::copy(Path::new(&content_filename), Path::new(&target_content_filename)).expect("Failed to load content file");
    }

    pub fn open(filename: &str) -> io::Result<Self> {
        let offset_filename = filename.to_owned() + "_offset";
        let length_filename = filename.to_owned() + "_length";
        let content_filename = filename.to_owned() + "_content";
        let offset_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(offset_filename)?;
        let offset_mmap = unsafe { MmapOptions::new().map_mut(&offset_file)? };
        let length_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(length_filename)?;
        let length_mmap = unsafe { MmapOptions::new().map_mut(&length_file)? };
        let content_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(content_filename)?;
        let content_mmap = unsafe { MmapOptions::new().map_mut(&content_file)? };
        Ok(Self {
            offset_mmap,
            length_mmap,
            content_mmap,
            filename: filename.to_string(),
        })
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if index < self.offset_mmap.len() / std::mem::size_of::<usize>() {
            let offset_ptr = self.offset_mmap.as_ptr() as *const usize;
            let begin = unsafe { *offset_ptr.add(index) };
            let length_ptr = self.length_mmap.as_ptr() as *const u16;
            let end = begin + unsafe { *length_ptr.add(index) as usize };
            let begin_ptr = unsafe { self.content_mmap.as_ptr().add(begin) };
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
        let offset_ptr = self.offset_mmap.as_ptr() as *const usize;
        let begin = unsafe { *offset_ptr.add(index) };
        let length_ptr = self.length_mmap.as_ptr() as *const u16;
        let end = begin + unsafe { *length_ptr.add(index) as usize };
        let begin_ptr = unsafe { self.content_mmap.as_ptr().add(begin) };
        let slice = unsafe { std::slice::from_raw_parts(begin_ptr, end - begin) };
        unsafe { std::str::from_utf8_unchecked(slice) }
    }

    pub fn len(&self) -> usize {
        self.offset_mmap.len() / std::mem::size_of::<usize>()
    }

    pub fn resize(&mut self, new_len: usize) {
        let old_len = self.offset_mmap.len() / std::mem::size_of::<usize>();
        let offset_size = new_len * std::mem::size_of::<usize>();
        let length_size = new_len * std::mem::size_of::<u16>();
        let offset_filename = self.filename.to_owned() + "_offset";
        let length_filename = self.filename.to_owned() + "_length";
        let mut offset_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(offset_filename).expect("Failed to open offset file");
        offset_file.set_len(offset_size as u64).expect("Failed to set offset file length");
        self.offset_mmap = unsafe { MmapOptions::new().map_mut(&offset_file).expect("Failed to create offset mmap") };
        let mut length_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(length_filename).expect("Failed to open length file");
        length_file.set_len(length_size as u64).expect("Failed to set length file length");
        self.length_mmap = unsafe { MmapOptions::new().map_mut(&length_file).expect("Failed to create length mmap") };
        if new_len > old_len {
            let length_ptr = self.length_mmap.as_mut_ptr() as *mut u16;
            unsafe {
                for i in old_len..new_len {
                    *length_ptr.add(i) = 0;
                }
            }
        }
    }

    pub fn batch_set(&mut self, indices: &Vec<usize>, v: &Vec<String>) {
        assert!(indices.len() == v.len());
        let old_size = self.content_mmap.len();
        let mut new_size = old_size;
        let offset_ptr = self.offset_mmap.as_mut_ptr() as *mut usize;
        let length_ptr = self.length_mmap.as_mut_ptr() as *mut u16;
        for (i, idx) in indices.iter().enumerate() {
            if *idx != usize::MAX {
                unsafe {
                    *offset_ptr.add(*idx) = new_size;
                    *length_ptr.add(*idx) = v[i].len() as u16;
                    new_size += v[i].len();
                }
            }
        }

        let content_filename = self.filename.to_owned() + "_content";
        let mut content_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(content_filename).expect("Failed to open content file");
        content_file.set_len(new_size as u64).expect("Failed to set content file length");
        self.content_mmap = unsafe { MmapOptions::new().map_mut(&content_file).expect("Failed to create content mmap") };
        let mut sl = {
            let mut content_ptr = self.content_mmap.as_mut_ptr();
            unsafe {
                &mut std::slice::from_raw_parts_mut(content_ptr, self.content_mmap.len())[old_size..new_size]
            }
        };
    }
}

impl Index<usize> for MmapStringVec {
    type Output = str;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        self.get_unchecked(index)
    }
}