pub const DEFAULT_PAGE_SIZE: usize = 4096 * 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

pub struct Page {
    pub page_id: PageId,
    pub data: Box<[u8]>,
}

impl Page {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_id: PageId::new_invalid(),
            data: vec![0; page_size].into_boxed_slice(),
        }
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
