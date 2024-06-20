use std::{pin::Pin, sync::RwLock};

pub const DEFAULT_PAGE_SIZE: usize = 4096 * 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(usize);

impl PageId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    pub fn new_invalid() -> Self {
        Self(usize::MAX)
    }

    pub fn is_valid(&self) -> bool {
        self.0 != usize::MAX
    }

    pub fn offset(&self, page_size: usize) -> usize {
        self.0 * page_size
    }
}

#[derive(Debug)]
pub struct Page {
    page_id: PageId,
    is_dirty: bool,
    pin_count: usize,
    data: Pin<Box<[u8]>>,
}

impl Page {
    pub fn new_raw(page_size: usize) -> Self {
        let buf = vec![0; page_size].into_boxed_slice();
        Self {
            page_id: PageId::new_invalid(),
            is_dirty: false,
            pin_count: 0,
            data: Pin::new(buf),
        }
    }

    pub fn new(page_size: usize) -> RwLock<Self> {
        RwLock::new(Self::new_raw(page_size))
    }

    pub fn allocate_page(&mut self, page_id: PageId) {
        self.page_id = page_id;
        self.pin_count += 1;
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    pub fn set_dirty(&mut self) {
        self.is_dirty = true;
    }

    pub fn clear_dirty(&mut self) {
        self.is_dirty = false;
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        self.pin_count = self.pin_count.saturating_sub(1);
    }

    pub fn deallocate_page(&mut self) {
        self.page_id = PageId::new_invalid();
        self.is_dirty = false;
    }

    pub fn page_id(&self) -> Option<PageId> {
        if self.page_id == PageId::new_invalid() {
            None
        } else {
            Some(self.page_id)
        }
    }

    pub fn is_allocated(&self) -> bool {
        self.page_id != PageId::new_invalid()
    }

    pub fn page_size(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id() {
        let page_id = PageId::new(42);
        assert_eq!(page_id.0, 42);
        let page_size = 4096;
        assert_eq!(page_id.offset(page_size), 42 * page_size);
    }
}
