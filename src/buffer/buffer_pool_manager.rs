use std::{
    collections::LinkedList,
    sync::{
        atomic::{self, AtomicUsize},
        Mutex, RwLock,
    },
};

use dashmap::DashMap;

use crate::{
    storage::disk::{DiskManager, LimeBaseDiskManager},
    Page, PageId,
};

pub trait BufferPoolManager {
    fn get_pool_size(&self) -> usize;
    fn get_pages(&self) -> &[RwLock<Page>];
    fn new_page(&self) -> anyhow::Result<Option<(PageId, &RwLock<Page>)>>;
    fn fetch_page(&self, page_id: PageId) -> anyhow::Result<Option<&RwLock<Page>>>;
    /// Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already 0, return false.
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool;
    fn flush_page(&self, page_id: PageId) -> anyhow::Result<bool>;
    fn flush_all_pages(&self) -> anyhow::Result<()>;
    fn delete_page(&self, page_id: PageId) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FrameId(usize);

impl FrameId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

pub struct BufferPoolManagerImpl<'a> {
    pages: Box<[RwLock<Page>]>,
    next_page_id: AtomicUsize,
    page_table: DashMap<PageId, FrameId>,
    // NOTE: is there lock-free linked list in Rust?
    /// list of free frames that don't have any pages on them.
    free_list: Mutex<LinkedList<FrameId>>,
    disk_manager: &'a LimeBaseDiskManager,
}

impl<'a> BufferPoolManagerImpl<'a> {
    pub fn new(pool_size: usize, disk_manager: &'a LimeBaseDiskManager) -> Self {
        let mut pages = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pages.push(Page::new(disk_manager.page_size()));
        }
        let pages = pages.into_boxed_slice();
        let free_list = (0..pool_size).map(FrameId::new).collect();
        Self {
            pages,
            next_page_id: AtomicUsize::new(0),
            page_table: DashMap::new(),
            free_list: Mutex::new(free_list),
            disk_manager,
        }
    }

    fn free_frame(&self) -> Option<FrameId> {
        let mut free_list = self.free_list.lock().unwrap();
        free_list.pop_front()
    }

    fn allocate_page(&self) -> PageId {
        let page_id = self.next_page_id.fetch_add(1, atomic::Ordering::AcqRel);
        PageId::new(page_id)
    }

    fn deallocate_page(&self, _page_id: PageId) {
        // currently noop
    }
}

impl<'a> BufferPoolManager for BufferPoolManagerImpl<'a> {
    fn get_pool_size(&self) -> usize {
        self.pages.len()
    }

    fn get_pages(&self) -> &[RwLock<Page>] {
        &self.pages
    }

    fn new_page(&self) -> anyhow::Result<Option<(PageId, &RwLock<Page>)>> {
        let Some(frame_id) = self.free_frame() else {
            // TODO: evict a unpinned page
            return Ok(None);
        };

        let page_id = self.allocate_page();
        let page = &self.pages[frame_id.0];
        {
            let mut page_guard = page.write().unwrap();
            page_guard.allocate_page(page_id);
            self.page_table.insert(page_id, frame_id);

            drop(page_guard);
        }

        Ok(Some((page_id, page)))
    }

    fn fetch_page(&self, page_id: PageId) -> anyhow::Result<Option<&RwLock<Page>>> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = &self.pages[frame_id.0];
            return Ok(Some(page));
        }

        let Some(frame_id) = self.free_frame() else {
            // TODO: evict a unpinned page
            return Ok(None);
        };

        {
            let mut page_guard = self.pages[frame_id.0].write().unwrap();

            self.disk_manager
                .read_page(page_id, page_guard.data_mut())?;

            page_guard.allocate_page(page_id);

            drop(page_guard);
        }

        Ok(Some(&self.pages[frame_id.0]))
    }

    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool {
        let Some(frame_id) = self.page_table.get(&page_id) else {
            // the page is not in the page table
            return false;
        };
        let mut page_guard = self.pages[frame_id.0].write().unwrap();
        if !page_guard.is_pinned() {
            return false;
        }
        page_guard.unpin();
        if is_dirty {
            page_guard.set_dirty();
        }

        true
    }

    fn flush_page(&self, page_id: PageId) -> anyhow::Result<bool> {
        let Some(frame_id) = self.page_table.get(&page_id) else {
            return Ok(false);
        };
        let mut page_guard = self.pages[frame_id.0].write().unwrap();
        self.disk_manager.write_page(page_id, page_guard.data())?;
        page_guard.clear_dirty();

        drop(page_guard);
        Ok(true)
    }

    fn flush_all_pages(&self) -> anyhow::Result<()> {
        let guards = self.pages.iter().map(|page| page.write().unwrap());
        for guard in guards {
            let Some(page_id) = guard.page_id() else {
                continue;
            };
            self.disk_manager.write_page(page_id, guard.data())?;
        }

        Ok(())
    }

    fn delete_page(&self, page_id: PageId) -> bool {
        let Some(frame_id) = self.page_table.get(&page_id) else {
            return false;
        };
        let mut page_guard = self.pages[frame_id.0].write().unwrap();
        if page_guard.is_pinned() {
            return false;
        }

        let mut free_list = self.free_list.lock().unwrap();
        free_list.push_back(*frame_id);

        self.deallocate_page(page_id);
        page_guard.deallocate_page();

        drop(page_guard);
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::page::DEFAULT_PAGE_SIZE;

    use super::*;

    #[test]
    fn test_binary_data() {
        let tempdir = tempfile::tempdir().unwrap();
        let filename = tempdir.path().join("test.db");
        const BUFFER_POOL_SIZE: usize = 10;
        let disk_manager = LimeBaseDiskManager::new(DEFAULT_PAGE_SIZE, filename).unwrap();
        let buffer_pool_manager = BufferPoolManagerImpl::new(BUFFER_POOL_SIZE, &disk_manager);

        let ret = buffer_pool_manager.new_page().unwrap();

        // The buffer pool is empty. We should be able to create a new page.
        assert!(
            ret.is_some(),
            "The buffer pool is empty. We should be able to create a new page."
        );
        let (page_id, page0) = ret.unwrap();
        assert_eq!(
            page_id,
            PageId::new(0),
            "The buffer pool is empty. We should be able to create a new page."
        );

        let mut random_binary_data = (0..DEFAULT_PAGE_SIZE)
            .map(|_| rand::random::<u8>())
            .collect::<Vec<_>>();

        // Insert terminal characters both in the midddle and at end
        random_binary_data[BUFFER_POOL_SIZE / 2] = 0;
        random_binary_data[BUFFER_POOL_SIZE - 1] = 0;

        // Once we have a page, We should be able to read and write content.
        {
            let mut page_guard = page0.write().unwrap();
            page_guard.data_mut().copy_from_slice(&random_binary_data);
        }
        {
            let page_guard = page0.read().unwrap();
            assert_eq!(
                page_guard.data(),
                &random_binary_data,
                "Once we have a page, We should be able to read and write content."
            );
        }

        // We should be able to create new pages until we fill up the buffer pool.
        for _ in 1..BUFFER_POOL_SIZE {
            assert!(
                buffer_pool_manager.new_page().unwrap().is_some(),
                "We should be able to create new pages until we fill up the buffer pool."
            );
        }

        // After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages.
        for page_id in (0..5).map(PageId::new) {
            assert!(
                buffer_pool_manager.unpin_page(page_id, true),
                "{:?} should be able to unpin",
                page_id
            );

            buffer_pool_manager.flush_page(page_id).unwrap();
        }
        for _ in 0..5 {
            let ret = buffer_pool_manager.new_page().unwrap();
            assert!(
                ret.is_some(),
                "After unpinning pages {{0, 1, 2, 3, 4}}, we should be able to create 5 new pages."
            );
            let (page_id, _page) = ret.unwrap();
            // Unpin the page here to allow future fetching
            buffer_pool_manager.unpin_page(page_id, true);
        }

        let page_id0 = PageId::new(0);
        // We should be able to fetch the data we wrote a while ago.
        let page0 = buffer_pool_manager.fetch_page(page_id0).unwrap();
        assert!(
            page0.is_some(),
            "We should be able to fetch the data we wrote a while ago."
        );
        let page0 = page0.unwrap();
        {
            let page_guard = page0.read().unwrap();
            assert_eq!(
                page_guard.data(),
                &random_binary_data,
                "We should be able to fetch the data we wrote a while ago."
            );
        }
        assert!(
            buffer_pool_manager.unpin_page(page_id0, true),
            "We should be able to unpin page0"
        );
    }
}
