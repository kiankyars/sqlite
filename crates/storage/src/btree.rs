//! B+tree implementation for rowid-keyed tables and secondary indexes.
//!
//! Each B+tree is rooted at a specific page. The tree uses the pager for all
//! page I/O. Interior nodes store (key, left_child) pairs plus a right_child.
//! Leaf nodes store (key, payload) pairs and are linked via next_leaf pointers
//! for efficient range scans.
//!
//! ## Page Layout
//!
//! ### Common header (8 bytes)
//! ```text
//! [0]     page_type: u8  (1 = interior, 2 = leaf)
//! [1..3]  cell_count: u16 (big-endian)
//! [3..5]  cell_content_offset: u16 (start of cell content area, grows downward)
//! [5..9]  type-specific: u32
//!           Interior: right_child page number
//!           Leaf: next_leaf page number (0 = none)
//! ```
//!
//! ### Cell offset array
//! Starts at byte 9, each entry is a u16 offset into the page where the cell
//! data begins. Entries are kept sorted by key.
//!
//! ### Cell format (Interior)
//! ```text
//! [0..4]  left_child: u32 (page number)
//! [4..12] key: i64 (big-endian, for correct sort order)
//! ```
//! Total: 12 bytes per cell.
//!
//! ### Cell format (Leaf)
//! ```text
//! [0..8]  key: i64 (big-endian)
//! [8..12] payload_size: u32
//! [12..]  payload: [u8; payload_size]
//! ```

use std::io;

use crate::pager::{PageNum, Pager};

/// Page type markers.
const PAGE_TYPE_INTERIOR: u8 = 1;
const PAGE_TYPE_LEAF: u8 = 2;

/// Size of the page header in bytes.
const PAGE_HEADER_SIZE: usize = 9;

/// Size of a cell pointer in the offset array.
const CELL_PTR_SIZE: usize = 2;

/// Size of an interior cell (left_child + key).
const INTERIOR_CELL_SIZE: usize = 12;

/// Minimum size of a leaf cell header (key + payload_size, without payload).
const LEAF_CELL_HEADER_SIZE: usize = 12;

/// A B+tree handle, rooted at a given page.
pub struct BTree<'a> {
    pager: &'a mut Pager,
    root_page: PageNum,
}

/// Result of a point lookup.
pub type LookupResult = Option<Vec<u8>>;

/// An entry yielded during iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: i64,
    pub payload: Vec<u8>,
}

impl<'a> BTree<'a> {
    /// Create a new B+tree handle. The `root_page` must already be allocated
    /// and initialized (see `create`).
    pub fn new(pager: &'a mut Pager, root_page: PageNum) -> Self {
        Self { pager, root_page }
    }

    /// Allocate a new root page and initialize it as an empty leaf.
    pub fn create(pager: &mut Pager) -> io::Result<PageNum> {
        let page_size = pager.page_size();
        let page_num = pager.allocate_page()?;
        {
            let page = pager.write_page(page_num)?;
            init_leaf(page, page_size);
        }
        Ok(page_num)
    }

    /// Insert a key-value pair. If the key already exists, the payload is updated.
    pub fn insert(&mut self, key: i64, payload: &[u8]) -> io::Result<()> {
        let result = self.insert_into(self.root_page, key, payload)?;
        if let InsertResult::Split { median_key, new_page } = result {
            // Root was split — create a new root.
            let page_size = self.pager.page_size();
            let new_root = self.pager.allocate_page()?;
            let old_root = self.root_page;
            {
                let page = self.pager.write_page(new_root)?;
                init_interior(page, page_size);
                // Set right_child to new_page.
                set_right_child(page, new_page);
                // Insert one cell: (left_child=old_root, key=median_key).
                insert_interior_cell(page, page_size, old_root, median_key);
            }
            self.root_page = new_root;
        }
        Ok(())
    }

    /// Look up a key. Returns the payload if found.
    pub fn lookup(&mut self, key: i64) -> io::Result<LookupResult> {
        self.lookup_in(self.root_page, key)
    }

    /// Return all entries in key order via leaf-linked scan.
    pub fn scan_all(&mut self) -> io::Result<Vec<Entry>> {
        // Find the leftmost leaf.
        let leftmost = self.find_leftmost_leaf(self.root_page)?;
        self.scan_from_leaf(leftmost, None, None)
    }

    /// Range scan: return entries where min_key <= key <= max_key.
    pub fn scan_range(&mut self, min_key: i64, max_key: i64) -> io::Result<Vec<Entry>> {
        // Find the leaf that would contain min_key.
        let leaf = self.find_leaf(self.root_page, min_key)?;
        self.scan_from_leaf(leaf, Some(min_key), Some(max_key))
    }

    /// Returns the current root page number. This may change after insert
    /// if the root was split.
    pub fn root_page(&self) -> PageNum {
        self.root_page
    }

    // ─── Internal helpers ────────────────────────────────────────────────

    /// Recursive insert. Returns whether a split occurred.
    fn insert_into(
        &mut self,
        page_num: PageNum,
        key: i64,
        payload: &[u8],
    ) -> io::Result<InsertResult> {
        let page_type = {
            let page = self.pager.read_page(page_num)?;
            page[0]
        };

        match page_type {
            PAGE_TYPE_LEAF => self.insert_into_leaf(page_num, key, payload),
            PAGE_TYPE_INTERIOR => self.insert_into_interior(page_num, key, payload),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown page type: {}", other),
            )),
        }
    }

    fn insert_into_leaf(
        &mut self,
        page_num: PageNum,
        key: i64,
        payload: &[u8],
    ) -> io::Result<InsertResult> {
        let page_size = self.pager.page_size();

        // Check if key already exists (update in place).
        {
            let page = self.pager.read_page(page_num)?;
            if let Some(idx) = find_cell_by_key_leaf(page, key) {
                // Update: only if payload fits in the same slot.
                // For simplicity, we delete and re-insert.
                let page = self.pager.write_page(page_num)?;
                delete_leaf_cell(page, page_size, idx);
                // Fall through to insert below.
            }
        }

        let cell_size = LEAF_CELL_HEADER_SIZE + payload.len();

        // Check if there's room in this leaf.
        let has_room = {
            let page = self.pager.read_page(page_num)?;
            leaf_has_room(page, page_size, cell_size)
        };

        if has_room {
            let page = self.pager.write_page(page_num)?;
            insert_leaf_cell(page, page_size, key, payload);
            Ok(InsertResult::Ok)
        } else {
            // Split the leaf.
            self.split_leaf(page_num, key, payload)
        }
    }

    fn insert_into_interior(
        &mut self,
        page_num: PageNum,
        key: i64,
        payload: &[u8],
    ) -> io::Result<InsertResult> {
        // Find the child to descend into.
        let child = {
            let page = self.pager.read_page(page_num)?;
            find_child(page, key)
        };

        let result = self.insert_into(child, key, payload)?;

        match result {
            InsertResult::Ok => Ok(InsertResult::Ok),
            InsertResult::Split { median_key, new_page } => {
                let page_size = self.pager.page_size();

                let has_room = {
                    let page = self.pager.read_page(page_num)?;
                    interior_has_room(page, page_size)
                };

                if has_room {
                    let page = self.pager.write_page(page_num)?;
                    insert_interior_cell_with_right(page, page_size, median_key, new_page);
                    Ok(InsertResult::Ok)
                } else {
                    self.split_interior(page_num, median_key, new_page)
                }
            }
        }
    }

    fn split_leaf(
        &mut self,
        page_num: PageNum,
        new_key: i64,
        new_payload: &[u8],
    ) -> io::Result<InsertResult> {
        let page_size = self.pager.page_size();

        // Collect all entries from the current leaf + the new entry.
        let mut entries: Vec<(i64, Vec<u8>)> = {
            let page = self.pager.read_page(page_num)?;
            read_all_leaf_entries(page)
        };
        entries.push((new_key, new_payload.to_vec()));
        entries.sort_by_key(|(k, _)| *k);

        let split_point = entries.len() / 2;
        let left_entries = &entries[..split_point];
        let right_entries = &entries[split_point..];
        let median_key = right_entries[0].0;

        // Get the old next_leaf pointer.
        let old_next_leaf = {
            let page = self.pager.read_page(page_num)?;
            get_next_leaf(page)
        };

        // Allocate a new right sibling page.
        let new_page = self.pager.allocate_page()?;

        // Write right sibling.
        {
            let page = self.pager.write_page(new_page)?;
            init_leaf(page, page_size);
            set_next_leaf(page, old_next_leaf);
            for (k, p) in right_entries {
                insert_leaf_cell(page, page_size, *k, p);
            }
        }

        // Rewrite left page (current page).
        {
            let page = self.pager.write_page(page_num)?;
            init_leaf(page, page_size);
            set_next_leaf(page, new_page);
            for (k, p) in left_entries {
                insert_leaf_cell(page, page_size, *k, p);
            }
        }

        Ok(InsertResult::Split {
            median_key,
            new_page,
        })
    }

    fn split_interior(
        &mut self,
        page_num: PageNum,
        new_key: i64,
        new_child: PageNum,
    ) -> io::Result<InsertResult> {
        let page_size = self.pager.page_size();

        // Collect all interior entries + the new one.
        let (mut entries, old_right_child) = {
            let page = self.pager.read_page(page_num)?;
            let entries = read_all_interior_entries(page);
            let rc = get_right_child(page);
            (entries, rc)
        };

        // Insert the new entry: (new_key, new_child) means new_child is the
        // right sibling of the child whose split produced new_key.
        // We need to find where new_key fits and adjust child pointers.
        let insert_pos = entries.iter().position(|(k, _)| *k > new_key).unwrap_or(entries.len());

        // The new_child becomes the right pointer for new_key's cell.
        // The left pointer is the previous right pointer at that position.
        if insert_pos < entries.len() {
            // Insert before entries[insert_pos].
            // new entry's left_child = entries[insert_pos].left_child
            // entries[insert_pos].left_child = new_child
            let old_left = entries[insert_pos].1;
            entries.insert(insert_pos, (new_key, old_left));
            entries[insert_pos + 1].1 = new_child;
        } else {
            // Insert at the end; new entry's left_child = old_right_child.
            entries.push((new_key, old_right_child));
            // The new right_child of the page becomes new_child.
            // We'll handle this below.
        }

        // Determine new right child.
        let final_right_child = if insert_pos >= entries.len() - 1 {
            new_child
        } else {
            old_right_child
        };

        let split_point = entries.len() / 2;
        let left_entries = &entries[..split_point];
        let median = entries[split_point].0;
        let right_left_child = entries[split_point].1;
        let right_entries = &entries[split_point + 1..];

        let new_page = self.pager.allocate_page()?;

        // Write right page.
        {
            let page = self.pager.write_page(new_page)?;
            init_interior(page, page_size);
            set_right_child(page, final_right_child);
            for &(k, lc) in right_entries {
                insert_interior_cell(page, page_size, lc, k);
            }
            // The leftmost child of the right page is right_left_child,
            // which needs special handling: if there are entries, the first
            // cell's left_child is it. If no entries, right_child is it.
            if right_entries.is_empty() {
                set_right_child(page, final_right_child);
            }
        }

        // Rewrite left page.
        {
            let page = self.pager.write_page(page_num)?;
            init_interior(page, page_size);
            set_right_child(page, right_left_child);
            for &(k, lc) in left_entries {
                insert_interior_cell(page, page_size, lc, k);
            }
        }

        Ok(InsertResult::Split {
            median_key: median,
            new_page,
        })
    }

    fn lookup_in(&mut self, page_num: PageNum, key: i64) -> io::Result<LookupResult> {
        let page = self.pager.read_page(page_num)?;
        let page_type = page[0];

        match page_type {
            PAGE_TYPE_LEAF => {
                // Search for the key in this leaf.
                let page = self.pager.read_page(page_num)?;
                if let Some(idx) = find_cell_by_key_leaf(page, key) {
                    let payload = read_leaf_payload(page, idx);
                    Ok(Some(payload))
                } else {
                    Ok(None)
                }
            }
            PAGE_TYPE_INTERIOR => {
                let child = {
                    let page = self.pager.read_page(page_num)?;
                    find_child(page, key)
                };
                self.lookup_in(child, key)
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown page type: {}", other),
            )),
        }
    }

    fn find_leftmost_leaf(&mut self, page_num: PageNum) -> io::Result<PageNum> {
        let page = self.pager.read_page(page_num)?;
        match page[0] {
            PAGE_TYPE_LEAF => Ok(page_num),
            PAGE_TYPE_INTERIOR => {
                // Descend into the leftmost child.
                let child = if get_cell_count(page) > 0 {
                    let offset = get_cell_offset(page, 0);
                    u32::from_be_bytes(page[offset..offset + 4].try_into().unwrap())
                } else {
                    get_right_child(page)
                };
                self.find_leftmost_leaf(child)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown page type",
            )),
        }
    }

    fn find_leaf(&mut self, page_num: PageNum, key: i64) -> io::Result<PageNum> {
        let page = self.pager.read_page(page_num)?;
        match page[0] {
            PAGE_TYPE_LEAF => Ok(page_num),
            PAGE_TYPE_INTERIOR => {
                let child = find_child(page, key);
                self.find_leaf(child, key)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown page type",
            )),
        }
    }

    fn scan_from_leaf(
        &mut self,
        start_leaf: PageNum,
        min_key: Option<i64>,
        max_key: Option<i64>,
    ) -> io::Result<Vec<Entry>> {
        let mut results = Vec::new();
        let mut current = start_leaf;

        loop {
            let (entries, next_leaf) = {
                let page = self.pager.read_page(current)?;
                let entries = read_all_leaf_entries(page);
                let next = get_next_leaf(page);
                (entries, next)
            };

            for (key, payload) in entries {
                if let Some(min) = min_key {
                    if key < min {
                        continue;
                    }
                }
                if let Some(max) = max_key {
                    if key > max {
                        return Ok(results);
                    }
                }
                results.push(Entry { key, payload });
            }

            if next_leaf == 0 {
                break;
            }
            current = next_leaf;
        }

        Ok(results)
    }
}

enum InsertResult {
    Ok,
    Split {
        median_key: i64,
        new_page: PageNum,
    },
}

// ─── Page-level helpers ──────────────────────────────────────────────────────

fn init_leaf(page: &mut [u8], page_size: usize) {
    page.fill(0);
    page[0] = PAGE_TYPE_LEAF;
    // cell_count = 0
    set_u16(page, 1, 0);
    // cell_content_offset = page_size (empty, content grows downward from end)
    set_u16(page, 3, page_size as u16);
    // next_leaf = 0
    set_u32(page, 5, 0);
}

fn init_interior(page: &mut [u8], page_size: usize) {
    page.fill(0);
    page[0] = PAGE_TYPE_INTERIOR;
    set_u16(page, 1, 0);
    set_u16(page, 3, page_size as u16);
    set_u32(page, 5, 0); // right_child
}

fn get_cell_count(page: &[u8]) -> usize {
    get_u16(page, 1) as usize
}

fn set_cell_count(page: &mut [u8], count: usize) {
    set_u16(page, 1, count as u16);
}

fn get_cell_content_offset(page: &[u8]) -> usize {
    get_u16(page, 3) as usize
}

fn set_cell_content_offset(page: &mut [u8], offset: usize) {
    set_u16(page, 3, offset as u16);
}

fn get_right_child(page: &[u8]) -> PageNum {
    get_u32(page, 5)
}

fn set_right_child(page: &mut [u8], child: PageNum) {
    set_u32(page, 5, child);
}

fn get_next_leaf(page: &[u8]) -> PageNum {
    get_u32(page, 5)
}

fn set_next_leaf(page: &mut [u8], next: PageNum) {
    set_u32(page, 5, next);
}

fn get_cell_offset(page: &[u8], idx: usize) -> usize {
    get_u16(page, PAGE_HEADER_SIZE + idx * CELL_PTR_SIZE) as usize
}

fn set_cell_offset(page: &mut [u8], idx: usize, offset: usize) {
    set_u16(page, PAGE_HEADER_SIZE + idx * CELL_PTR_SIZE, offset as u16);
}

/// Free space available for new cells in this page.
fn free_space(page: &[u8]) -> usize {
    let cell_count = get_cell_count(page);
    let offset_array_end = PAGE_HEADER_SIZE + cell_count * CELL_PTR_SIZE;
    let content_start = get_cell_content_offset(page);
    if content_start > offset_array_end {
        content_start - offset_array_end
    } else {
        0
    }
}

fn leaf_has_room(page: &[u8], _page_size: usize, cell_size: usize) -> bool {
    // Need space for: cell data + one cell pointer.
    free_space(page) >= cell_size + CELL_PTR_SIZE
}

fn interior_has_room(page: &[u8], _page_size: usize) -> bool {
    free_space(page) >= INTERIOR_CELL_SIZE + CELL_PTR_SIZE
}

/// Insert a leaf cell (key + payload) into the page, maintaining sorted order.
fn insert_leaf_cell(page: &mut [u8], _page_size: usize, key: i64, payload: &[u8]) {
    let cell_count = get_cell_count(page);
    let cell_size = LEAF_CELL_HEADER_SIZE + payload.len();

    // Allocate cell space (grows downward).
    let new_content_offset = get_cell_content_offset(page) - cell_size;
    set_cell_content_offset(page, new_content_offset);

    // Write cell data.
    let off = new_content_offset;
    page[off..off + 8].copy_from_slice(&key.to_be_bytes());
    page[off + 8..off + 12].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    page[off + 12..off + 12 + payload.len()].copy_from_slice(payload);

    // Find insertion position in sorted order.
    let insert_pos = find_insert_pos_leaf(page, cell_count, key);

    // Shift cell offset entries to make room.
    for i in (insert_pos..cell_count).rev() {
        let existing = get_cell_offset(page, i);
        set_cell_offset(page, i + 1, existing);
    }

    // Write the new cell offset.
    set_cell_offset(page, insert_pos, new_content_offset);
    set_cell_count(page, cell_count + 1);
}

/// Delete a leaf cell at the given index. This is a simple slot removal.
/// Does not reclaim cell content space (fragmentation is acceptable for now).
fn delete_leaf_cell(page: &mut [u8], _page_size: usize, idx: usize) {
    let cell_count = get_cell_count(page);
    // Shift cell offsets down.
    for i in idx..cell_count - 1 {
        let next = get_cell_offset(page, i + 1);
        set_cell_offset(page, i, next);
    }
    set_cell_count(page, cell_count - 1);
}

fn find_insert_pos_leaf(page: &[u8], cell_count: usize, key: i64) -> usize {
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let cell_key = i64::from_be_bytes(page[offset..offset + 8].try_into().unwrap());
        if cell_key > key {
            return i;
        }
    }
    cell_count
}

fn find_cell_by_key_leaf(page: &[u8], key: i64) -> Option<usize> {
    let cell_count = get_cell_count(page);
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let cell_key = i64::from_be_bytes(page[offset..offset + 8].try_into().unwrap());
        if cell_key == key {
            return Some(i);
        }
        if cell_key > key {
            return None; // Sorted, so key can't exist after this.
        }
    }
    None
}

fn read_leaf_payload(page: &[u8], idx: usize) -> Vec<u8> {
    let offset = get_cell_offset(page, idx);
    let payload_size =
        u32::from_be_bytes(page[offset + 8..offset + 12].try_into().unwrap()) as usize;
    page[offset + 12..offset + 12 + payload_size].to_vec()
}

fn read_all_leaf_entries(page: &[u8]) -> Vec<(i64, Vec<u8>)> {
    let cell_count = get_cell_count(page);
    let mut entries = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let key = i64::from_be_bytes(page[offset..offset + 8].try_into().unwrap());
        let payload_size =
            u32::from_be_bytes(page[offset + 8..offset + 12].try_into().unwrap()) as usize;
        let payload = page[offset + 12..offset + 12 + payload_size].to_vec();
        entries.push((key, payload));
    }
    entries
}

/// Insert an interior cell (left_child, key). Used when building interior nodes.
fn insert_interior_cell(page: &mut [u8], _page_size: usize, left_child: PageNum, key: i64) {
    let cell_count = get_cell_count(page);

    let new_content_offset = get_cell_content_offset(page) - INTERIOR_CELL_SIZE;
    set_cell_content_offset(page, new_content_offset);

    // Write cell data.
    let off = new_content_offset;
    page[off..off + 4].copy_from_slice(&left_child.to_be_bytes());
    page[off + 4..off + 12].copy_from_slice(&key.to_be_bytes());

    // Find sorted position.
    let insert_pos = find_insert_pos_interior(page, cell_count, key);

    // Shift offsets.
    for i in (insert_pos..cell_count).rev() {
        let existing = get_cell_offset(page, i);
        set_cell_offset(page, i + 1, existing);
    }

    set_cell_offset(page, insert_pos, new_content_offset);
    set_cell_count(page, cell_count + 1);
}

/// Insert an interior cell for a key that was promoted from a child split.
/// The new_child becomes the right sibling at the correct position.
fn insert_interior_cell_with_right(
    page: &mut [u8],
    page_size: usize,
    key: i64,
    new_child: PageNum,
) {
    let cell_count = get_cell_count(page);
    let insert_pos = find_insert_pos_interior(page, cell_count, key);

    // The new cell's left_child is whatever was the right pointer at insert_pos.
    // After insertion, the right pointer at insert_pos+1 should be new_child.
    // But since we use the right_child field for the rightmost pointer,
    // we need to be careful about the logic.

    if insert_pos == cell_count {
        // Inserting at the end: left_child = current right_child.
        let old_right = get_right_child(page);
        insert_interior_cell(page, page_size, old_right, key);
        set_right_child(page, new_child);
    } else {
        // Inserting in the middle: left_child = left_child of cell at insert_pos.
        let old_left_child = {
            let off = get_cell_offset(page, insert_pos);
            u32::from_be_bytes(page[off..off + 4].try_into().unwrap())
        };
        insert_interior_cell(page, page_size, old_left_child, key);
        // Now update the cell at insert_pos+1 to point to new_child as its left_child.
        // Actually, we just inserted at insert_pos, so the old cell at insert_pos
        // is now at insert_pos+1. We need to update its left_child to new_child.
        let off = get_cell_offset(page, insert_pos + 1);
        page[off..off + 4].copy_from_slice(&new_child.to_be_bytes());
    }
}

fn find_insert_pos_interior(page: &[u8], cell_count: usize, key: i64) -> usize {
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let cell_key = i64::from_be_bytes(page[offset + 4..offset + 12].try_into().unwrap());
        if cell_key > key {
            return i;
        }
    }
    cell_count
}

fn read_all_interior_entries(page: &[u8]) -> Vec<(i64, PageNum)> {
    let cell_count = get_cell_count(page);
    let mut entries = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let left_child = u32::from_be_bytes(page[offset..offset + 4].try_into().unwrap());
        let key = i64::from_be_bytes(page[offset + 4..offset + 12].try_into().unwrap());
        entries.push((key, left_child));
    }
    entries
}

/// Find which child page to descend into for a given key.
fn find_child(page: &[u8], key: i64) -> PageNum {
    let cell_count = get_cell_count(page);
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let cell_key = i64::from_be_bytes(page[offset + 4..offset + 12].try_into().unwrap());
        if key < cell_key {
            // Go to left child of this cell.
            return u32::from_be_bytes(page[offset..offset + 4].try_into().unwrap());
        }
    }
    // Key >= all keys: go to right child.
    get_right_child(page)
}

// ─── Byte helpers ─────────────────────────────────────────────────────────────

fn get_u16(page: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(page[offset..offset + 2].try_into().unwrap())
}

fn set_u16(page: &mut [u8], offset: usize, val: u16) {
    page[offset..offset + 2].copy_from_slice(&val.to_be_bytes());
}

fn get_u32(page: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(page[offset..offset + 4].try_into().unwrap())
}

fn set_u32(page: &mut [u8], offset: usize, val: u32) {
    page[offset..offset + 4].copy_from_slice(&val.to_be_bytes());
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("ralph_btree_tests");
        fs::create_dir_all(&dir).ok();
        dir.join(name)
    }

    fn cleanup(path: &std::path::Path) {
        fs::remove_file(path).ok();
    }

    #[test]
    fn create_and_lookup_empty() {
        let path = temp_db_path("btree_empty.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        assert_eq!(tree.lookup(1).unwrap(), None);

        cleanup(&path);
    }

    #[test]
    fn insert_and_lookup_single() {
        let path = temp_db_path("btree_single.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        tree.insert(42, b"hello world").unwrap();

        assert_eq!(tree.lookup(42).unwrap(), Some(b"hello world".to_vec()));
        assert_eq!(tree.lookup(99).unwrap(), None);

        cleanup(&path);
    }

    #[test]
    fn insert_and_lookup_multiple() {
        let path = temp_db_path("btree_multi.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        tree.insert(10, b"ten").unwrap();
        tree.insert(5, b"five").unwrap();
        tree.insert(15, b"fifteen").unwrap();
        tree.insert(1, b"one").unwrap();
        tree.insert(20, b"twenty").unwrap();

        assert_eq!(tree.lookup(10).unwrap(), Some(b"ten".to_vec()));
        assert_eq!(tree.lookup(5).unwrap(), Some(b"five".to_vec()));
        assert_eq!(tree.lookup(15).unwrap(), Some(b"fifteen".to_vec()));
        assert_eq!(tree.lookup(1).unwrap(), Some(b"one".to_vec()));
        assert_eq!(tree.lookup(20).unwrap(), Some(b"twenty".to_vec()));
        assert_eq!(tree.lookup(99).unwrap(), None);

        cleanup(&path);
    }

    #[test]
    fn update_existing_key() {
        let path = temp_db_path("btree_update.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        tree.insert(1, b"original").unwrap();
        assert_eq!(tree.lookup(1).unwrap(), Some(b"original".to_vec()));

        tree.insert(1, b"updated").unwrap();
        assert_eq!(tree.lookup(1).unwrap(), Some(b"updated".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn scan_all_entries() {
        let path = temp_db_path("btree_scan.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        tree.insert(30, b"thirty").unwrap();
        tree.insert(10, b"ten").unwrap();
        tree.insert(20, b"twenty").unwrap();

        let entries = tree.scan_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, 10);
        assert_eq!(entries[1].key, 20);
        assert_eq!(entries[2].key, 30);

        cleanup(&path);
    }

    #[test]
    fn insert_triggers_leaf_split() {
        let path = temp_db_path("btree_split.db");
        cleanup(&path);

        // Use a small pool but default page size.
        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        // Insert enough entries to fill a 4KB leaf and trigger a split.
        // Each leaf cell: 12 bytes header + payload.
        // With 100-byte payloads: 112 bytes per cell.
        // 4096 - 9 (header) ≈ 4087 bytes usable.
        // 4087 / (112 + 2) ≈ 35 cells before split.
        let payload = vec![0xAB; 100];
        for i in 0..50 {
            tree.insert(i, &payload).unwrap();
        }

        // All entries should still be findable.
        for i in 0..50 {
            let result = tree.lookup(i).unwrap();
            assert_eq!(result, Some(payload.clone()), "key {} not found after split", i);
        }

        // Scan should return all entries in order.
        let entries = tree.scan_all().unwrap();
        assert_eq!(entries.len(), 50);
        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, idx as i64);
        }

        cleanup(&path);
    }

    #[test]
    fn range_scan() {
        let path = temp_db_path("btree_range.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        for i in 0..20 {
            tree.insert(i * 10, &(i as u32).to_be_bytes()).unwrap();
        }

        let entries = tree.scan_range(50, 120).unwrap();
        let keys: Vec<i64> = entries.iter().map(|e| e.key).collect();
        assert_eq!(keys, vec![50, 60, 70, 80, 90, 100, 110, 120]);

        cleanup(&path);
    }

    #[test]
    fn large_insert_triggers_multiple_splits() {
        let path = temp_db_path("btree_multi_split.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        // Insert 200 entries to trigger multiple levels of splits.
        let payload = vec![0xCD; 50];
        for i in 0..200 {
            tree.insert(i, &payload).unwrap();
        }

        // Verify all entries exist.
        for i in 0..200 {
            assert!(
                tree.lookup(i).unwrap().is_some(),
                "key {} not found after multiple splits",
                i
            );
        }

        // Verify scan order.
        let entries = tree.scan_all().unwrap();
        assert_eq!(entries.len(), 200);
        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, idx as i64);
        }

        cleanup(&path);
    }

    #[test]
    fn insert_reverse_order() {
        let path = temp_db_path("btree_reverse.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        let payload = vec![0xEF; 30];
        for i in (0..100).rev() {
            tree.insert(i, &payload).unwrap();
        }

        let entries = tree.scan_all().unwrap();
        assert_eq!(entries.len(), 100);
        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, idx as i64);
        }

        cleanup(&path);
    }

    #[test]
    fn persistence_after_flush() {
        let path = temp_db_path("btree_persist.db");
        cleanup(&path);

        let root;
        {
            let mut pager = Pager::open(&path).unwrap();
            root = BTree::create(&mut pager).unwrap();
            let mut tree = BTree::new(&mut pager, root);

            tree.insert(1, b"alpha").unwrap();
            tree.insert(2, b"beta").unwrap();
            tree.insert(3, b"gamma").unwrap();

            pager.flush_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let mut tree = BTree::new(&mut pager, root);

            assert_eq!(tree.lookup(1).unwrap(), Some(b"alpha".to_vec()));
            assert_eq!(tree.lookup(2).unwrap(), Some(b"beta".to_vec()));
            assert_eq!(tree.lookup(3).unwrap(), Some(b"gamma".to_vec()));
        }

        cleanup(&path);
    }
}
