use std::marker::PhantomData;
pub(crate) struct SafeMutPtr<I>(*mut I, PhantomData<I>);

unsafe impl<I> Send for SafeMutPtr<I> {}

unsafe impl<I> Sync for SafeMutPtr<I> {}

impl<I> SafeMutPtr<I> {
    pub(crate) fn new(ptr: &mut I) -> Self {
        Self { 0: ptr as *mut I, 1: PhantomData }
    }

    pub(crate) fn get_mut(&self) -> &mut I {
        unsafe { &mut *self.0 }
    }
}

impl<I> Clone for SafeMutPtr<I> {
    fn clone(&self) -> Self {
        SafeMutPtr(self.0.clone(), PhantomData)
    }
}

impl<I> Copy for SafeMutPtr<I> {}