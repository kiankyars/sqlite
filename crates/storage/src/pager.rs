//! Pager: page-level I/O with an in-memory buffer pool.
//!
//! The pager manages reading and writing fixed-size pages from/to the database
//! file. It maintains a buffer pool with LRU eviction and dirty-page tracking.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::header::FileHeader;

/// Default number of pages in the buffer pool.
const DEFAULT_POOL_SIZE: usize = 256;

/// A page number (0-based). Page 0 contains the file header.
pub type PageNum = u32;

/// A single in-memory page frame.
struct Frame {
    /// The page data.
    data: Vec<u8>,
    /// Whether this page has been modified since last flush.
    dirty: bool,
    /// Pin count — a pinned page cannot be evicted.
    pin_count: u32,
    /// Access counter for LRU tracking.
    last_access: u64,
}

/// The pager manages page I/O between disk and a fixed-size buffer pool.
pub struct Pager {
    file: File,
    header: FileHeader,
    page_size: usize,
    /// Buffer pool: page_num -> frame.
    pool: HashMap<PageNum, Frame>,
    /// Maximum number of frames in the pool.
    max_frames: usize,
    /// Monotonically increasing access counter for LRU.
    access_counter: u64,
}

impl Pager {
    /// Open or create a database file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with_pool_size(path, DEFAULT_POOL_SIZE)
    }

    /// Open or create a database file with a specific buffer pool size.
    pub fn open_with_pool_size<P: AsRef<Path>>(path: P, max_frames: usize) -> io::Result<Self> {
        let path = path.as_ref();
        let exists = path.exists() && std::fs::metadata(path)?.len() > 0;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let header = if exists {
            FileHeader::read_from(&mut file)?
        } else {
            let header = FileHeader::default();
            // Write the initial header + fill page 0.
            let mut page0 = vec![0u8; header.page_size as usize];
            header.serialize(&mut page0);
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&page0)?;
            file.sync_all()?;
            header
        };

        let page_size = header.page_size as usize;

        Ok(Self {
            file,
            header,
            page_size,
            pool: HashMap::new(),
            max_frames,
            access_counter: 0,
        })
    }

    /// Returns a reference to the file header.
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// Returns a mutable reference to the file header.
    pub fn header_mut(&mut self) -> &mut FileHeader {
        &mut self.header
    }

    /// Returns the page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the total number of pages currently in the database.
    pub fn page_count(&self) -> u32 {
        self.header.page_count
    }

    /// Read a page into the buffer pool and return a reference to its data.
    pub fn read_page(&mut self, page_num: PageNum) -> io::Result<&[u8]> {
        self.ensure_loaded(page_num)?;
        self.touch(page_num);
        Ok(&self.pool.get(&page_num).unwrap().data)
    }

    /// Get a mutable reference to a page's data. Marks the page as dirty.
    pub fn write_page(&mut self, page_num: PageNum) -> io::Result<&mut [u8]> {
        self.ensure_loaded(page_num)?;
        self.touch(page_num);
        let frame = self.pool.get_mut(&page_num).unwrap();
        frame.dirty = true;
        Ok(&mut frame.data)
    }

    /// Allocate a new page. Returns the page number.
    ///
    /// If the freelist has pages, pops one from the freelist.
    /// Otherwise, extends the file by one page.
    pub fn allocate_page(&mut self) -> io::Result<PageNum> {
        // For now, always extend the file (freelist reuse comes later).
        let page_num = self.header.page_count;
        self.header.page_count += 1;

        // Create a zeroed page in the pool.
        let data = vec![0u8; self.page_size];
        let frame = Frame {
            data,
            dirty: true,
            pin_count: 0,
            last_access: self.next_access(),
        };
        self.maybe_evict()?;
        self.pool.insert(page_num, frame);

        // Update the header on disk.
        self.flush_header()?;

        Ok(page_num)
    }

    /// Pin a page (prevent eviction).
    pub fn pin(&mut self, page_num: PageNum) {
        if let Some(frame) = self.pool.get_mut(&page_num) {
            frame.pin_count += 1;
        }
    }

    /// Unpin a page (allow eviction).
    pub fn unpin(&mut self, page_num: PageNum) {
        if let Some(frame) = self.pool.get_mut(&page_num) {
            frame.pin_count = frame.pin_count.saturating_sub(1);
        }
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> io::Result<()> {
        let dirty_pages: Vec<PageNum> = self
            .pool
            .iter()
            .filter(|(_, f)| f.dirty)
            .map(|(&pn, _)| pn)
            .collect();

        for page_num in dirty_pages {
            self.flush_page(page_num)?;
        }

        self.flush_header()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Flush a single page to disk.
    fn flush_page(&mut self, page_num: PageNum) -> io::Result<()> {
        let frame = self.pool.get_mut(&page_num).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "page not in buffer pool")
        })?;

        let offset = page_num as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&frame.data)?;
        frame.dirty = false;
        Ok(())
    }

    /// Write the file header to page 0.
    fn flush_header(&mut self) -> io::Result<()> {
        // If page 0 is in the pool, update it there.
        if let Some(frame) = self.pool.get_mut(&0) {
            self.header.serialize(&mut frame.data);
            frame.dirty = true;
            // Flush page 0.
            let offset = 0u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&frame.data)?;
            frame.dirty = false;
        } else {
            // Write header directly to disk.
            self.file.seek(SeekFrom::Start(0))?;
            self.header.write_to(&mut self.file)?;
        }
        Ok(())
    }

    /// Ensure a page is loaded into the buffer pool.
    fn ensure_loaded(&mut self, page_num: PageNum) -> io::Result<()> {
        if self.pool.contains_key(&page_num) {
            return Ok(());
        }

        if page_num >= self.header.page_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("page {} out of range (page_count={})", page_num, self.header.page_count),
            ));
        }

        self.maybe_evict()?;

        let mut data = vec![0u8; self.page_size];
        let offset = page_num as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut data)?;

        let frame = Frame {
            data,
            dirty: false,
            pin_count: 0,
            last_access: self.next_access(),
        };
        self.pool.insert(page_num, frame);
        Ok(())
    }

    /// Evict a page from the pool if at capacity.
    fn maybe_evict(&mut self) -> io::Result<()> {
        while self.pool.len() >= self.max_frames {
            // Find the LRU unpinned page.
            let victim = self
                .pool
                .iter()
                .filter(|(_, f)| f.pin_count == 0)
                .min_by_key(|(_, f)| f.last_access)
                .map(|(&pn, _)| pn);

            match victim {
                Some(page_num) => {
                    // Flush if dirty before evicting.
                    if self.pool.get(&page_num).unwrap().dirty {
                        self.flush_page(page_num)?;
                    }
                    self.pool.remove(&page_num);
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "buffer pool full: all pages are pinned",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Increment and return the access counter.
    fn next_access(&mut self) -> u64 {
        self.access_counter += 1;
        self.access_counter
    }

    /// Update the access counter for a page.
    fn touch(&mut self, page_num: PageNum) {
        let ts = self.next_access();
        if let Some(frame) = self.pool.get_mut(&page_num) {
            frame.last_access = ts;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("ralph_pager_tests");
        fs::create_dir_all(&dir).ok();
        dir.join(name)
    }

    fn cleanup(path: &std::path::Path) {
        fs::remove_file(path).ok();
    }

    #[test]
    fn create_new_database() {
        let path = temp_db_path("create_new.db");
        cleanup(&path);

        let pager = Pager::open(&path).unwrap();
        assert_eq!(pager.page_count(), 1);
        assert_eq!(pager.page_size(), crate::header::DEFAULT_PAGE_SIZE as usize);

        cleanup(&path);
    }

    #[test]
    fn reopen_existing_database() {
        let path = temp_db_path("reopen.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            pager.allocate_page().unwrap();
            pager.flush_all().unwrap();
        }

        let pager = Pager::open(&path).unwrap();
        assert_eq!(pager.page_count(), 2);

        cleanup(&path);
    }

    #[test]
    fn read_write_page() {
        let path = temp_db_path("read_write.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let pg = pager.allocate_page().unwrap();
        assert_eq!(pg, 1);

        // Write data to the new page.
        {
            let data = pager.write_page(pg).unwrap();
            data[0..5].copy_from_slice(b"hello");
        }

        // Read it back.
        {
            let data = pager.read_page(pg).unwrap();
            assert_eq!(&data[0..5], b"hello");
        }

        pager.flush_all().unwrap();
        cleanup(&path);
    }

    #[test]
    fn data_persists_across_reopen() {
        let path = temp_db_path("persist.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            let pg = pager.allocate_page().unwrap();
            let data = pager.write_page(pg).unwrap();
            data[0..6].copy_from_slice(b"world!");
            pager.flush_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let data = pager.read_page(1).unwrap();
            assert_eq!(&data[0..6], b"world!");
        }

        cleanup(&path);
    }

    #[test]
    fn allocate_multiple_pages() {
        let path = temp_db_path("multi_alloc.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();
        let p3 = pager.allocate_page().unwrap();

        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
        assert_eq!(p3, 3);
        assert_eq!(pager.page_count(), 4);

        pager.flush_all().unwrap();
        cleanup(&path);
    }

    #[test]
    fn read_page_out_of_range() {
        let path = temp_db_path("out_of_range.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let result = pager.read_page(99);
        assert!(result.is_err());

        cleanup(&path);
    }

    #[test]
    fn lru_eviction() {
        let path = temp_db_path("lru_evict.db");
        cleanup(&path);

        // Pool of 4 frames.
        let mut pager = Pager::open_with_pool_size(&path, 4).unwrap();

        // Allocate 5 pages (+ page 0 = 6 total, but pool fits 4).
        for _ in 0..5 {
            pager.allocate_page().unwrap();
        }

        // Write something to each page so they're dirty and get flushed on eviction.
        for pg in 1..=5 {
            let data = pager.write_page(pg).unwrap();
            data[0] = pg as u8;
        }

        // Pool should have at most 4 frames.
        assert!(pager.pool.len() <= 4);

        // Re-read all pages—evicted ones should be re-loaded from disk.
        for pg in 1..=5 {
            let data = pager.read_page(pg).unwrap();
            assert_eq!(data[0], pg as u8);
        }

        pager.flush_all().unwrap();
        cleanup(&path);
    }

    #[test]
    fn pin_prevents_eviction() {
        let path = temp_db_path("pin_evict.db");
        cleanup(&path);

        let mut pager = Pager::open_with_pool_size(&path, 3).unwrap();

        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();

        pager.pin(p1);
        pager.pin(p2);
        // Page 0 is not pinned and may be evicted.

        // This should succeed — page 0 can be evicted.
        let p3 = pager.allocate_page().unwrap();
        assert_eq!(p3, 3);

        pager.unpin(p1);
        pager.unpin(p2);
        pager.flush_all().unwrap();
        cleanup(&path);
    }

    #[test]
    fn header_survives_flush() {
        let path = temp_db_path("header_flush.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            pager.header_mut().schema_root = 7;
            pager.allocate_page().unwrap();
            pager.allocate_page().unwrap();
            pager.flush_all().unwrap();
        }

        {
            let pager = Pager::open(&path).unwrap();
            assert_eq!(pager.header().schema_root, 7);
            assert_eq!(pager.page_count(), 3);
        }

        cleanup(&path);
    }
}
