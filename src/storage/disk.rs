use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    sync::RwLock,
};

use crate::PageId;

pub trait DiskManager: Sized + Sync + Send {
    fn new(page_size: usize, filename: impl AsRef<Path>) -> io::Result<Self>;
    fn page_size(&self) -> usize;
    fn read_page(&self, page_id: PageId, data: &mut [u8]) -> anyhow::Result<()>;
    fn write_page(&self, page_id: PageId, data: &[u8]) -> anyhow::Result<()>;
}

pub struct BasicDiskManager {
    page_size: usize,
    file: RwLock<File>,
}

impl DiskManager for BasicDiskManager {
    fn new(page_size: usize, filename: impl AsRef<Path>) -> io::Result<Self> {
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
            page_size,
            file: RwLock::new(file),
        })
    }

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn read_page(&self, page_id: PageId, data: &mut [u8]) -> anyhow::Result<()> {
        let offset = page_id.offset(self.page_size()) as u64;
        let Ok(mut file) = self.file.write() else {
            anyhow::bail!("failed to acquire write lock");
        };
        file.seek(io::SeekFrom::Start(offset))?;
        file.read_exact(data)?;

        Ok(())
    }

    fn write_page(&self, page_id: PageId, data: &[u8]) -> anyhow::Result<()> {
        let offset = page_id.offset(self.page_size()) as u64;
        let Ok(mut file) = self.file.write() else {
            anyhow::bail!("failed to acquire write lock");
        };
        file.seek(io::SeekFrom::Start(offset))?;
        file.write_all(data)?;

        Ok(())
    }
}

pub type LimeBaseDiskManager = BasicDiskManager;

#[cfg(test)]
mod tests {

    use crate::storage::page::page::DEFAULT_PAGE_SIZE;

    use super::*;
    use rand::prelude::*;

    #[test]
    fn test_basic_disk_manager_page_size() {
        let tempdir = tempfile::tempdir().unwrap();
        let disk_manager =
            LimeBaseDiskManager::new(DEFAULT_PAGE_SIZE, tempdir.path().join("test.db")).unwrap();
        assert_eq!(disk_manager.page_size(), DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_basic_disk_manager_read_write_page() {
        let mut rng = rand::thread_rng();
        let tempdir = tempfile::tempdir().unwrap();
        let disk_manager =
            LimeBaseDiskManager::new(DEFAULT_PAGE_SIZE, tempdir.path().join("test.db")).unwrap();
        const N_PAGES: usize = 10;
        let mut data = [[0; DEFAULT_PAGE_SIZE as usize]; N_PAGES];
        for (i, page_buf) in data.iter_mut().enumerate() {
            rng.fill(page_buf.as_mut_slice());
            disk_manager.write_page(PageId::new(i), page_buf).unwrap();
        }

        for _ in 0..N_PAGES {
            let i = rng.gen_range(0..N_PAGES);
            let mut buf = [0; DEFAULT_PAGE_SIZE as usize];
            disk_manager.read_page(PageId::new(i), &mut buf).unwrap();
            assert_eq!(buf, data[i]);

            // Randomly replace a page with new data
            let replace_page = rng.gen_bool(0.8);
            if replace_page {
                let random_page = rng.gen_range(0..N_PAGES);
                rng.fill(data[random_page].as_mut_slice());
                disk_manager
                    .write_page(PageId::new(random_page), &data[random_page])
                    .unwrap();
            }
        }

        drop(disk_manager);

        // Reopen the disk manager and check if the data is still there
        let disk_manager =
            LimeBaseDiskManager::new(DEFAULT_PAGE_SIZE, tempdir.path().join("test.db")).unwrap();
        for i in 0..N_PAGES {
            let mut buf = [0; DEFAULT_PAGE_SIZE as usize];
            disk_manager.read_page(PageId::new(i), &mut buf).unwrap();
            assert_eq!(buf, data[i]);
        }
    }
}
