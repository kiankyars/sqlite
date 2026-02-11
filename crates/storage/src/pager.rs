//! Pager: page-level I/O with an in-memory buffer pool.
//!
//! The pager manages reading and writing fixed-size pages from/to the database
//! file. It maintains a buffer pool with LRU eviction and dirty-page tracking.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::header::FileHeader;
use crate::wal::Wal;

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
    wal: Wal,
    header: FileHeader,
    header_dirty: bool,
    page_size: usize,
    /// Buffer pool: page_num -> frame.
    pool: HashMap<PageNum, Frame>,
    /// Maximum number of frames in the pool.
    max_frames: usize,
    /// Monotonically increasing access counter for LRU.
    access_counter: u64,
    /// Monotonically increasing transaction ID for WAL commits.
    next_txn_id: u64,
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

        let mut header = if exists {
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

        let mut page_size = header.page_size as usize;
        let mut wal = Wal::open(path, header.page_size)?;

        // Replay any committed WAL frames that were not checkpointed before the
        // previous process exited. Truncate WAL afterward so startup is idempotent.
        wal.recover(&mut file, page_size)?;

        file.seek(SeekFrom::Start(0))?;
        header = FileHeader::read_from(&mut file)?;
        if header.page_size as usize != page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "database page size {} changed during WAL recovery (expected {})",
                    header.page_size, page_size
                ),
            ));
        }
        page_size = header.page_size as usize;

        Ok(Self {
            file,
            wal,
            header,
            header_dirty: false,
            page_size,
            pool: HashMap::new(),
            max_frames,
            access_counter: 0,
            next_txn_id: 1,
        })
    }

    /// Returns a reference to the file header.
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// Returns a mutable reference to the file header.
    pub fn header_mut(&mut self) -> &mut FileHeader {
        self.header_dirty = true;
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
        if page_num == 0 && self.header_dirty {
            self.stage_header_page()?;
        }
        self.ensure_loaded(page_num)?;
        self.touch(page_num);
        Ok(&self.pool.get(&page_num).unwrap().data)
    }

    /// Get a mutable reference to a page's data. Marks the page as dirty.
    pub fn write_page(&mut self, page_num: PageNum) -> io::Result<&mut [u8]> {
        if page_num == 0 && self.header_dirty {
            self.stage_header_page()?;
        }
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
        let page_num = if self.header.freelist_head != 0 {
            let page_num = self.header.freelist_head;
            if page_num >= self.header.page_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "freelist head {} out of range (page_count={})",
                        page_num, self.header.page_count
                    ),
                ));
            }

            self.ensure_loaded(page_num)?;
            let next_head = {
                let frame = self.pool.get(&page_num).unwrap();
                u32::from_be_bytes(frame.data[0..4].try_into().unwrap())
            };
            if next_head != 0 && next_head >= self.header.page_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "freelist next pointer {} out of range (page_count={})",
                        next_head, self.header.page_count
                    ),
                ));
            }

            self.header.freelist_head = next_head;
            self.header.freelist_count = self.header.freelist_count.saturating_sub(1);

            let ts = self.next_access();
            let frame = self.pool.get_mut(&page_num).unwrap();
            frame.data.fill(0);
            frame.dirty = true;
            frame.last_access = ts;
            page_num
        } else {
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
            page_num
        };

        self.header_dirty = true;

        Ok(page_num)
    }

    /// Add an existing page to the freelist so it can be reused by future
    /// allocations.
    pub fn free_page(&mut self, page_num: PageNum) -> io::Result<()> {
        if page_num == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot free header page 0",
            ));
        }
        if page_num >= self.header.page_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "page {} out of range (page_count={})",
                    page_num, self.header.page_count
                ),
            ));
        }
        if self.freelist_contains(page_num)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("page {} is already on freelist", page_num),
            ));
        }

        let next_head = self.header.freelist_head;
        self.ensure_loaded(page_num)?;
        let ts = self.next_access();
        let frame = self.pool.get_mut(&page_num).unwrap();
        frame.data.fill(0);
        frame.data[0..4].copy_from_slice(&next_head.to_be_bytes());
        frame.dirty = true;
        frame.last_access = ts;

        self.header.freelist_head = page_num;
        self.header.freelist_count = self.header.freelist_count.saturating_add(1);
        self.header_dirty = true;
        Ok(())
    }

    /// Commit all dirty pages through WAL and then apply them to the database file.
    pub fn commit(&mut self) -> io::Result<()> {
        self.flush_all()
    }

    /// Checkpoint committed WAL frames into the database file and truncate WAL.
    pub fn checkpoint(&mut self) -> io::Result<usize> {
        let has_dirty_pages = self.header_dirty || self.pool.values().any(|frame| frame.dirty);
        if has_dirty_pages {
            self.flush_all()?;
        }
        self.wal.checkpoint(&mut self.file, self.page_size)
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
        if self.header_dirty {
            self.stage_header_page()?;
        }

        let mut dirty_pages: Vec<PageNum> = self
            .pool
            .iter()
            .filter(|(_, f)| f.dirty)
            .map(|(&pn, _)| pn)
            .collect();
        dirty_pages.sort_unstable();

        if dirty_pages.is_empty() {
            self.file.sync_all()?;
            return Ok(());
        }

        let mut wal_pages = Vec::with_capacity(dirty_pages.len());
        for page_num in &dirty_pages {
            let data = self
                .pool
                .get(page_num)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "page not in buffer pool"))?
                .data
                .clone();
            wal_pages.push((*page_num, data));
        }

        let txn_id = self.next_txn_id;
        self.next_txn_id += 1;
        self.wal.append_txn(txn_id, &wal_pages)?;

        for (page_num, data) in wal_pages {
            let offset = page_num as u64 * self.page_size as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&data)?;

            if let Some(frame) = self.pool.get_mut(&page_num) {
                frame.dirty = false;
            }
        }

        self.file.sync_all()?;
        self.header_dirty = false;
        Ok(())
    }

    /// Flush a single page to disk.
    fn flush_page(&mut self, page_num: PageNum) -> io::Result<()> {
        let frame = self
            .pool
            .get_mut(&page_num)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "page not in buffer pool"))?;

        let offset = page_num as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&frame.data)?;
        frame.dirty = false;
        Ok(())
    }

    /// Stage the in-memory header into page 0 and mark the page dirty.
    fn stage_header_page(&mut self) -> io::Result<()> {
        self.ensure_loaded(0)?;
        let ts = self.next_access();
        let frame = self
            .pool
            .get_mut(&0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "header page not in pool"))?;
        self.header.serialize(&mut frame.data);
        frame.dirty = true;
        frame.last_access = ts;
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
                format!(
                    "page {} out of range (page_count={})",
                    page_num, self.header.page_count
                ),
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

    /// Returns true when the page is already linked on the freelist.
    fn freelist_contains(&mut self, target: PageNum) -> io::Result<bool> {
        let mut current = self.header.freelist_head;
        let mut seen = 0u32;
        while current != 0 {
            if current >= self.header.page_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "freelist page {} out of range (page_count={})",
                        current, self.header.page_count
                    ),
                ));
            }
            if current == target {
                return Ok(true);
            }

            self.ensure_loaded(current)?;
            let next = {
                let frame = self.pool.get(&current).unwrap();
                u32::from_be_bytes(frame.data[0..4].try_into().unwrap())
            };
            if next != 0 && next >= self.header.page_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "freelist next pointer {} out of range (page_count={})",
                        next, self.header.page_count
                    ),
                ));
            }

            current = next;
            seen = seen.saturating_add(1);
            if seen > self.header.page_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "freelist loop detected",
                ));
            }
        }
        Ok(false)
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
    use std::io::{Seek, SeekFrom, Write};

    use crate::wal::{checksum32, wal_path_for, Wal, WAL_HEADER_SIZE, WAL_MAGIC};

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("ralph_pager_tests");
        fs::create_dir_all(&dir).ok();
        dir.join(name)
    }

    fn cleanup(path: &std::path::Path) {
        fs::remove_file(path).ok();
        fs::remove_file(wal_path_for(path)).ok();
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

    #[test]
    fn allocate_reuses_freelist_before_extension() {
        let path = temp_db_path("freelist_reuse.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();
        let p3 = pager.allocate_page().unwrap();
        assert_eq!((p1, p2, p3), (1, 2, 3));
        assert_eq!(pager.page_count(), 4);

        {
            let page2 = pager.write_page(2).unwrap();
            page2[0..4].copy_from_slice(&3u32.to_be_bytes());
        }
        {
            let page3 = pager.write_page(3).unwrap();
            page3[0..4].copy_from_slice(&0u32.to_be_bytes());
        }
        pager.header_mut().freelist_head = 2;
        pager.header_mut().freelist_count = 2;
        pager.flush_all().unwrap();

        let reused_2 = pager.allocate_page().unwrap();
        assert_eq!(reused_2, 2);
        assert_eq!(pager.page_count(), 4);
        assert_eq!(pager.header().freelist_head, 3);
        assert_eq!(pager.header().freelist_count, 1);

        let reused_3 = pager.allocate_page().unwrap();
        assert_eq!(reused_3, 3);
        assert_eq!(pager.page_count(), 4);
        assert_eq!(pager.header().freelist_head, 0);
        assert_eq!(pager.header().freelist_count, 0);

        let extended = pager.allocate_page().unwrap();
        assert_eq!(extended, 4);
        assert_eq!(pager.page_count(), 5);

        cleanup(&path);
    }

    #[test]
    fn allocate_zeroes_reused_freelist_page() {
        let path = temp_db_path("freelist_zeroed.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let page_num = pager.allocate_page().unwrap();
        assert_eq!(page_num, 1);

        {
            let page = pager.write_page(page_num).unwrap();
            page.fill(0xAA);
            page[0..4].copy_from_slice(&0u32.to_be_bytes());
        }
        pager.header_mut().freelist_head = page_num;
        pager.header_mut().freelist_count = 1;
        pager.flush_all().unwrap();

        let reused = pager.allocate_page().unwrap();
        assert_eq!(reused, 1);
        let page = pager.read_page(reused).unwrap();
        assert!(page.iter().all(|b| *b == 0));

        cleanup(&path);
    }

    #[test]
    fn free_page_adds_to_freelist_and_allocate_reuses_it() {
        let path = temp_db_path("freelist_free_and_reuse.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();
        let p3 = pager.allocate_page().unwrap();
        assert_eq!((p1, p2, p3), (1, 2, 3));

        pager.free_page(p2).unwrap();
        pager.free_page(p3).unwrap();
        assert_eq!(pager.header().freelist_head, p3);
        assert_eq!(pager.header().freelist_count, 2);

        let reused_1 = pager.allocate_page().unwrap();
        let reused_2 = pager.allocate_page().unwrap();
        assert_eq!((reused_1, reused_2), (p3, p2));
        assert_eq!(pager.header().freelist_head, 0);
        assert_eq!(pager.header().freelist_count, 0);

        cleanup(&path);
    }

    #[test]
    fn free_page_rejects_invalid_and_duplicate_pages() {
        let path = temp_db_path("freelist_free_invalid.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();
        assert_eq!((p1, p2), (1, 2));

        let err = pager.free_page(0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let err = pager.free_page(99).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        pager.free_page(p1).unwrap();
        pager.free_page(p2).unwrap();
        let err = pager.free_page(p1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        cleanup(&path);
    }

    #[test]
    fn free_page_persists_across_reopen() {
        let path = temp_db_path("freelist_free_persist.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            let p1 = pager.allocate_page().unwrap();
            let p2 = pager.allocate_page().unwrap();
            let p3 = pager.allocate_page().unwrap();
            assert_eq!((p1, p2, p3), (1, 2, 3));

            pager.free_page(p2).unwrap();
            pager.free_page(p3).unwrap();
            pager.flush_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            assert_eq!(pager.header().freelist_head, 3);
            assert_eq!(pager.header().freelist_count, 2);

            let reused_1 = pager.allocate_page().unwrap();
            let reused_2 = pager.allocate_page().unwrap();
            assert_eq!((reused_1, reused_2), (3, 2));
            assert_eq!(pager.header().freelist_head, 0);
            assert_eq!(pager.header().freelist_count, 0);
            assert_eq!(pager.page_count(), 4);
        }

        cleanup(&path);
    }

    #[test]
    fn flush_writes_wal_page_frames_and_commit_record() {
        let path = temp_db_path("wal_commit_record.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let page_num = pager.allocate_page().unwrap();
        {
            let page = pager.write_page(page_num).unwrap();
            page[0..4].copy_from_slice(b"wal!");
        }
        pager.commit().unwrap();

        let wal_bytes = fs::read(wal_path_for(&path)).unwrap();
        assert!(wal_bytes.len() > WAL_HEADER_SIZE);
        assert_eq!(&wal_bytes[0..8], WAL_MAGIC);

        let mut offset = WAL_HEADER_SIZE;
        let mut page_frames = 0usize;
        let mut txn_id: Option<u64> = None;
        loop {
            let frame_type = wal_bytes[offset];
            offset += 1;

            match frame_type {
                1 => {
                    let frame_txn =
                        u64::from_be_bytes(wal_bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let page_num =
                        u32::from_be_bytes(wal_bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let payload_len =
                        u32::from_be_bytes(wal_bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let checksum =
                        u32::from_be_bytes(wal_bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let payload_end = offset + payload_len as usize;
                    let payload = &wal_bytes[offset..payload_end];
                    offset = payload_end;

                    let mut header = Vec::with_capacity(1 + 8 + 4 + 4);
                    header.push(frame_type);
                    header.extend_from_slice(&frame_txn.to_be_bytes());
                    header.extend_from_slice(&page_num.to_be_bytes());
                    header.extend_from_slice(&payload_len.to_be_bytes());
                    assert_eq!(checksum, checksum32(&[&header, payload]));

                    if let Some(existing_txn) = txn_id {
                        assert_eq!(frame_txn, existing_txn);
                    } else {
                        txn_id = Some(frame_txn);
                    }
                    page_frames += 1;
                }
                2 => {
                    let frame_txn =
                        u64::from_be_bytes(wal_bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let frame_count =
                        u32::from_be_bytes(wal_bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let checksum =
                        u32::from_be_bytes(wal_bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    let mut header = Vec::with_capacity(1 + 8 + 4);
                    header.push(frame_type);
                    header.extend_from_slice(&frame_txn.to_be_bytes());
                    header.extend_from_slice(&frame_count.to_be_bytes());
                    assert_eq!(checksum, checksum32(&[&header]));
                    assert_eq!(Some(frame_txn), txn_id);
                    assert_eq!(frame_count as usize, page_frames);
                    assert_eq!(offset, wal_bytes.len());
                    break;
                }
                other => panic!("unexpected WAL frame type {other}"),
            }
        }

        assert!(page_frames >= 1);
        cleanup(&path);
    }

    #[test]
    fn multiple_flushes_append_multiple_wal_transactions() {
        let path = temp_db_path("wal_append.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let page_num = pager.allocate_page().unwrap();

        {
            let page = pager.write_page(page_num).unwrap();
            page[0] = 1;
        }
        pager.flush_all().unwrap();
        let wal_len_after_first = fs::metadata(wal_path_for(&path)).unwrap().len();

        {
            let page = pager.write_page(page_num).unwrap();
            page[0] = 2;
        }
        pager.flush_all().unwrap();
        let wal_len_after_second = fs::metadata(wal_path_for(&path)).unwrap().len();

        assert!(wal_len_after_second > wal_len_after_first);
        cleanup(&path);
    }

    #[test]
    fn open_recovers_committed_wal_frames() {
        let path = temp_db_path("wal_recover_on_open.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            let page_num = pager.allocate_page().unwrap();
            {
                let page = pager.write_page(page_num).unwrap();
                page[0..4].copy_from_slice(b"orig");
            }
            pager.commit().unwrap();
        }

        {
            let mut wal = Wal::open(&path, crate::header::DEFAULT_PAGE_SIZE).unwrap();
            let mut payload = vec![0u8; crate::header::DEFAULT_PAGE_SIZE as usize];
            payload[0..4].copy_from_slice(b"reco");
            wal.append_txn(100, &[(1, payload)]).unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let page = pager.read_page(1).unwrap();
            assert_eq!(&page[0..4], b"reco");
        }

        let wal_len = fs::metadata(wal_path_for(&path)).unwrap().len() as usize;
        assert_eq!(wal_len, WAL_HEADER_SIZE);
        cleanup(&path);
    }

    #[test]
    fn recovery_ignores_uncommitted_wal_tail() {
        let path = temp_db_path("wal_recover_ignores_tail.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            let page_num = pager.allocate_page().unwrap();
            {
                let page = pager.write_page(page_num).unwrap();
                page[0..4].copy_from_slice(b"base");
            }
            pager.commit().unwrap();
        }

        let wal_path = wal_path_for(&path);
        {
            let mut wal_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_path)
                .unwrap();
            wal_file.seek(SeekFrom::End(0)).unwrap();

            let txn_id = 101u64;
            let page_num = 1u32;
            let mut payload = vec![0u8; crate::header::DEFAULT_PAGE_SIZE as usize];
            payload[0..4].copy_from_slice(b"tail");
            let payload_len = payload.len() as u32;

            let mut frame_header = Vec::with_capacity(1 + 8 + 4 + 4);
            frame_header.push(1u8);
            frame_header.extend_from_slice(&txn_id.to_be_bytes());
            frame_header.extend_from_slice(&page_num.to_be_bytes());
            frame_header.extend_from_slice(&payload_len.to_be_bytes());
            let checksum = checksum32(&[&frame_header, &payload]);

            wal_file.write_all(&frame_header).unwrap();
            wal_file.write_all(&checksum.to_be_bytes()).unwrap();
            wal_file.write_all(&payload).unwrap();
            wal_file.sync_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let page = pager.read_page(1).unwrap();
            assert_eq!(&page[0..4], b"base");
        }

        let wal_len = fs::metadata(wal_path_for(&path)).unwrap().len() as usize;
        assert_eq!(wal_len, WAL_HEADER_SIZE);
        cleanup(&path);
    }

    #[test]
    fn checkpoint_truncates_wal_and_preserves_data() {
        let path = temp_db_path("wal_checkpoint.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let page_num = pager.allocate_page().unwrap();
        {
            let page = pager.write_page(page_num).unwrap();
            page[0..4].copy_from_slice(b"ckpt");
        }
        pager.commit().unwrap();

        let wal_path = wal_path_for(&path);
        let wal_len_before = fs::metadata(&wal_path).unwrap().len() as usize;
        assert!(wal_len_before > WAL_HEADER_SIZE);

        let checkpointed = pager.checkpoint().unwrap();
        assert!(checkpointed >= 1);

        let wal_len_after = fs::metadata(&wal_path).unwrap().len() as usize;
        assert_eq!(wal_len_after, WAL_HEADER_SIZE);
        drop(pager);

        let mut reopened = Pager::open(&path).unwrap();
        let page = reopened.read_page(page_num).unwrap();
        assert_eq!(&page[0..4], b"ckpt");

        cleanup(&path);
    }

    #[test]
    fn open_reloads_header_after_wal_recovery() {
        let path = temp_db_path("wal_recover_header_page.db");
        cleanup(&path);

        let recovered_page0 = {
            let mut pager = Pager::open(&path).unwrap();
            pager.allocate_page().unwrap();
            pager.commit().unwrap();

            let mut recovered_page0 = pager.read_page(0).unwrap().to_vec();
            let mut recovered_header = pager.header().clone();
            recovered_header.schema_root = 77;
            recovered_header.serialize(&mut recovered_page0);
            recovered_page0
        };

        {
            let mut wal = Wal::open(&path, crate::header::DEFAULT_PAGE_SIZE).unwrap();
            wal.append_txn(200, &[(0, recovered_page0)]).unwrap();
        }

        let pager = Pager::open(&path).unwrap();
        assert_eq!(pager.header().schema_root, 77);
        cleanup(&path);
    }
}
