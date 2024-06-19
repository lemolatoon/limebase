use crate::{Page, PageGuard, PageId, ReadPageGuard, WritePageGuard};

pub trait BufferPoolManager {
    fn new(pool_size: usize) -> Self;
    fn get_pool_size(&self) -> usize;
    fn get_pages(&self) -> &[Page];
    fn get_pages_mut(&mut self) -> &mut [Page];
    fn new_page(&self) -> anyhow::Result<(PageId, Page)>;
    fn new_page_guarded(&self) -> anyhow::Result<(PageId, PageGuard)>;
    fn fetch_page(&self, page_id: PageId) -> anyhow::Result<Page>;
    fn fetch_page_basic(&self, page_id: PageId) -> anyhow::Result<PageGuard>;
    fn fetch_page_read(&self, page_id: PageId) -> anyhow::Result<ReadPageGuard>;
    fn fetch_page_write(&self, page_id: PageId) -> anyhow::Result<WritePageGuard>;
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool;
    fn flush_page(&self, page_id: PageId) -> bool;
    fn flush_all_pages(&self);
    fn delete_page(&self, page_id: PageId) -> bool;
}
