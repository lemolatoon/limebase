use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    sync::RwLock,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageId(u64);

impl PageId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn offset(&self, page_size: u64) -> u64 {
        self.0 * page_size
    }
}

pub trait DiskManager: Sized + Sync + Send {
    fn new(filename: impl AsRef<Path>) -> io::Result<Self>;
    fn page_size(&self) -> u64;
    fn read_page(&self, page_id: PageId, data: &mut [u8]) -> anyhow::Result<()>;
    fn write_page(&self, page_id: PageId, data: &[u8]) -> anyhow::Result<()>;
}

pub const DEFAULT_PAGE_SIZE: u64 = 4096 * 2;

pub struct BasicDiskManager<const PAGE_SIZE: u64> {
    file: RwLock<File>,
}

impl<const PAGE_SIZE: u64> DiskManager for BasicDiskManager<PAGE_SIZE> {
    fn new(filename: impl AsRef<Path>) -> io::Result<Self> {
        let file = if filename.as_ref().exists() {
            OpenOptions::new().read(true).append(true).open(filename)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(filename)?
        };
        Ok(Self {
            file: RwLock::new(file),
        })
    }

    fn page_size(&self) -> u64 {
        PAGE_SIZE
    }

    fn read_page(&self, page_id: PageId, data: &mut [u8]) -> anyhow::Result<()> {
        let offset = page_id.offset(self.page_size());
        let Ok(mut file) = self.file.write() else {
            anyhow::bail!("failed to acquire write lock");
        };
        file.seek(io::SeekFrom::Start(offset))?;
        file.read_exact(data)?;

        Ok(())
    }

    fn write_page(&self, page_id: PageId, data: &[u8]) -> anyhow::Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        let Ok(mut file) = self.file.write() else {
            anyhow::bail!("failed to acquire write lock");
        };
        file.seek(io::SeekFrom::Start(offset))?;
        file.write_all(data)?;

        Ok(())
    }
}

pub type LimeBaseDiskManager = BasicDiskManager<{ DEFAULT_PAGE_SIZE }>;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn test_page_id() {
        let page_id = PageId::new(42);
        assert_eq!(page_id.0, 42);
        let page_size = 4096;
        assert_eq!(page_id.offset(page_size), 42 * page_size);
    }

    #[test]
    fn test_basic_disk_manager_page_size() {
        let tempdir = tempfile::tempdir().unwrap();
        let disk_manager = LimeBaseDiskManager::new(tempdir.path().join("test.db")).unwrap();
        assert_eq!(disk_manager.page_size(), DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_basic_disk_manager_read_write_page() {
        let mut rng = rand::thread_rng();
        let tempdir = tempfile::tempdir().unwrap();
        let disk_manager = LimeBaseDiskManager::new(tempdir.path().join("test.db")).unwrap();
        const N_PAGES: usize = 10;
        let mut data = [[0; DEFAULT_PAGE_SIZE as usize]; N_PAGES];
        for (i, page_buf) in data.iter_mut().enumerate() {
            rng.fill(page_buf.as_mut_slice());
            disk_manager
                .write_page(PageId::new(i as u64), page_buf)
                .unwrap();
        }

        for _ in 0..N_PAGES {
            let i = rng.gen_range(0..N_PAGES);
            let mut buf = [0; DEFAULT_PAGE_SIZE as usize];
            disk_manager
                .read_page(PageId::new(i as u64), &mut buf)
                .unwrap();
            assert_eq!(buf, data[i]);

            // Randomly replace a page with new data
            let replace_page = rng.gen_bool(0.8);
            if replace_page {
                let random_page = rng.gen_range(0..N_PAGES);
                rng.fill(data[random_page].as_mut_slice());
                disk_manager
                    .write_page(PageId::new(random_page as u64), &data[random_page])
                    .unwrap();
            }
        }

        drop(disk_manager);

        // Reopen the disk manager and check if the data is still there
        let disk_manager = LimeBaseDiskManager::new(tempdir.path().join("test.db")).unwrap();
        for i in 0..N_PAGES {
            let mut buf = [0; DEFAULT_PAGE_SIZE as usize];
            disk_manager
                .read_page(PageId::new(i as u64), &mut buf)
                .unwrap();
            assert_eq!(buf, data[i]);
        }
    }
}
