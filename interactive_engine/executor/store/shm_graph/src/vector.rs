use memmap2::{Mmap, MmapMut};
use std::ffi::CString;
use std::ops::{Index, IndexMut};
use std::os::fd::RawFd;
use std::ptr;
use std::{
    fs::{File, OpenOptions},
    io::{Write, Read},
};
use libc::{c_int, c_void, MAP_SHARED, PROT_READ, PROT_WRITE, shm_open, ftruncate, mmap, munmap, mremap, close, O_CREAT, O_RDWR, O_RDONLY, stat, off_t, fstat, S_IRUSR, S_IWUSR, S_IXUSR, MREMAP_MAYMOVE};

pub struct PtrWrapper<T> {
    pub inner: *const T,
}

unsafe impl<T> Send for PtrWrapper<T> {}
unsafe impl<T> Sync for PtrWrapper<T> {}
pub struct SharedVec<T: Copy + Sized> {
    fd: RawFd,
    name: String,

    ptr: PtrWrapper<T>,
    size: usize,
}

impl<T> SharedVec<T>
where
    T: Copy + Sized,
{
    pub fn new() -> Self {
        Self {
            fd: -1,
            name: "".to_string(),
            ptr: PtrWrapper { inner: ptr::null() },
            size: 0,
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

            unsafe { munmap(addr as *mut c_void, input_file_size); }
        }

        unsafe { close(fd); }

        let ret = Self::open(name);
        ret
    }

    pub fn open(name: &str) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe {
            shm_open(cstr_name.as_ptr(), O_RDONLY, 0o666)
        };

        let mut st: stat = unsafe { std::mem::zeroed() };
        unsafe { fstat(fd, &mut st); }
        let file_size = st.st_size as usize;

        let addr = unsafe {
            mmap(ptr::null_mut(), file_size, PROT_READ, MAP_SHARED, fd, 0) as *const T
        };

        Self {
            fd,
            name: name.to_string(),
            ptr: PtrWrapper { inner: addr },
            size: file_size / std::mem::size_of::<T>(),
        }
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

    pub fn name(&self) -> &str {
        self.name.as_str()
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

pub struct MutPtrWrapper<T> {
    pub inner: *mut T,
}

unsafe impl<T> Send for MutPtrWrapper<T> {}
unsafe impl<T> Sync for MutPtrWrapper<T> {}

pub struct SharedMutVec<T: Copy + Sized> {
    fd: RawFd,
    name: String,

    ptr: MutPtrWrapper<T>,
    size: usize,
}

impl<T> SharedMutVec<T>
where
    T: Copy + Sized,
{
    pub fn create(name: &str, len: usize) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd = unsafe {
            shm_open(cstr_name.as_ptr(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        };
        let file_size = len * std::mem::size_of::<T>();
        unsafe { ftruncate(fd, file_size as i64); }
        let addr = unsafe {
            mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T 
        };

        Self {
            fd,
            name: name.to_string(),
            ptr: MutPtrWrapper { inner: addr },
            size: len,
        }
    }

    pub fn open(name: &str) -> Self {
        let cstr_name = CString::new(name).unwrap();
        let fd: RawFd = unsafe {
            shm_open(cstr_name.as_ptr(), O_RDWR, S_IRUSR | S_IWUSR)
        };
        let mut st: stat = unsafe { std::mem::zeroed() };
        unsafe { fstat(fd, &mut st); }
        let file_size = st.st_size as usize;

        let addr = unsafe {
            mmap(ptr::null_mut(), file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) as *mut T
        };

        Self {
            fd,
            name: name.to_string(),
            ptr: MutPtrWrapper { inner: addr },
            size: file_size / std::mem::size_of::<T>(),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn set(&mut self, index: usize, val: T) {
        unsafe { *self.ptr.inner.add(index) = val };
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

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.inner
    }

    pub fn resize(&mut self, new_len: usize) {
        let old_file_size = self.len() * std::mem::size_of::<T>();
        let new_file_size = new_len * std::mem::size_of::<T>();
        if new_file_size == old_file_size {
            return ;
        } else {
            let result = unsafe { ftruncate(self.fd, new_file_size as i64) };
            if result == -1 {
                println!("ftruncate failed, {}", std::io::Error::last_os_error());
            }
            let new_addr = unsafe {
                mremap(self.ptr.inner as *mut c_void, old_file_size, new_file_size, MREMAP_MAYMOVE) as *mut T
            };
            self.ptr.inner = new_addr;
            self.size = new_len;
        }
    }
}

impl<T: Copy + Sized> Drop for SharedMutVec<T> {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr.inner as *mut c_void, self.size * std::mem::size_of::<T>()); 
            close(self.fd);
        }
    }
}

impl<T: Copy + Sized> Index<usize> for SharedMutVec<T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.ptr.inner.add(index) }
    }
}

impl<T: Copy + Sized> IndexMut<usize> for SharedMutVec<T> {
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
