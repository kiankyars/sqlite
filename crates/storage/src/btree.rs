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

use std::collections::HashSet;
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
/// Minimum leaf occupancy ratio before delete-time rebalance is triggered.
const LEAF_MIN_UTILIZATION_NUMERATOR: usize = 35;
const LEAF_MIN_UTILIZATION_DENOMINATOR: usize = 100;
/// Minimum interior occupancy ratio before delete-time rebalance is triggered.
const INTERIOR_MIN_UTILIZATION_NUMERATOR: usize = 35;
const INTERIOR_MIN_UTILIZATION_DENOMINATOR: usize = 100;

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

struct LeafEntry {
    key: i64,
    total_len: usize,
    local_payload: Vec<u8>,
    overflow_page: Option<PageNum>,
}

fn max_local_payload(page_size: usize) -> usize {
    page_size / 4
}

fn write_overflow_chain(pager: &mut Pager, data: &[u8]) -> io::Result<PageNum> {
    let page_size = pager.page_size();
    let capacity = page_size - 4;
    let mut current_page = pager.allocate_page()?;
    let first_page = current_page;

    let mut written = 0;
    while written < data.len() {
        let remaining = data.len() - written;
        let chunk_size = std::cmp::min(remaining, capacity);
        let next_page = if remaining > chunk_size {
            pager.allocate_page()?
        } else {
            0
        };

        let page = pager.write_page(current_page)?;
        page[0..4].copy_from_slice(&next_page.to_be_bytes());
        page[4..4 + chunk_size].copy_from_slice(&data[written..written + chunk_size]);

        written += chunk_size;
        current_page = next_page;
    }
    Ok(first_page)
}

fn read_overflow_chain(
    pager: &mut Pager,
    start_page: PageNum,
    len: usize,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len);
    let mut current_page = start_page;
    let page_size = pager.page_size();
    let capacity = page_size - 4;

    while out.len() < len {
        if current_page == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "overflow chain incomplete",
            ));
        }
        let page = pager.read_page(current_page)?;
        let next_page = u32::from_be_bytes(page[0..4].try_into().unwrap());
        let needed = len - out.len();
        let available = std::cmp::min(needed, capacity);
        out.extend_from_slice(&page[4..4 + available]);
        current_page = next_page;
    }
    Ok(out)
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

    /// Reclaim all pages that belong to a tree rooted at `root_page`.
    ///
    /// Pages are freed in post-order (children before parent) so the page
    /// graph is fully traversed before reclamation.
    pub fn reclaim_tree(pager: &mut Pager, root_page: PageNum) -> io::Result<usize> {
        let mut pages = Vec::new();
        let mut visited = HashSet::new();
        Self::collect_tree_pages(pager, root_page, &mut pages, &mut visited)?;

        for page_num in pages.iter().rev().copied() {
            pager.free_page(page_num)?;
        }
        Ok(pages.len())
    }

    /// Insert a key-value pair. If the key already exists, the payload is updated.
    pub fn insert(&mut self, key: i64, payload: &[u8]) -> io::Result<()> {
        let result = self.insert_into(self.root_page, key, payload)?;
        if let InsertResult::Split {
            median_key,
            new_page,
        } = result
        {
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

    /// Delete a key from the tree. Returns true if a row was deleted.
    pub fn delete(&mut self, key: i64) -> io::Result<bool> {
        let result = self.delete_from(self.root_page, key, true)?;
        if result.deleted {
            self.compact_root_if_possible()?;
        }
        Ok(result.deleted)
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
                // TODO: Free old overflow pages if any.
                let page = self.pager.write_page(page_num)?;
                delete_leaf_cell(page, page_size, idx);
                // Fall through to insert below.
            }
        }

        let max_local = max_local_payload(page_size);
        let entry = if payload.len() > max_local {
            let overflow_page = write_overflow_chain(self.pager, &payload[max_local..])?;
            LeafEntry {
                key,
                total_len: payload.len(),
                local_payload: payload[..max_local].to_vec(),
                overflow_page: Some(overflow_page),
            }
        } else {
            LeafEntry {
                key,
                total_len: payload.len(),
                local_payload: payload.to_vec(),
                overflow_page: None,
            }
        };

        let cell_size = LEAF_CELL_HEADER_SIZE + entry.local_payload.len() + if entry.overflow_page.is_some() { 4 } else { 0 };

        // Check if there's room in this leaf.
        let has_room = {
            let page = self.pager.read_page(page_num)?;
            leaf_has_room(page, page_size, cell_size)
        };

        if has_room {
            let page = self.pager.write_page(page_num)?;
            insert_leaf_cell(page, page_size, &entry);
            Ok(InsertResult::Ok)
        } else {
            // Split the leaf.
            self.split_leaf(page_num, entry)
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
            InsertResult::Split {
                median_key,
                new_page,
            } => {
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
        new_entry: LeafEntry,
    ) -> io::Result<InsertResult> {
        let page_size = self.pager.page_size();

        // Collect all entries from the current leaf + the new entry.
        let mut entries: Vec<LeafEntry> = {
            let page = self.pager.read_page(page_num)?;
            read_all_leaf_entries_raw(page)
        };
        entries.push(new_entry);
        entries.sort_by_key(|e| e.key);

        let split_point = entries.len() / 2;
        let left_entries = &entries[..split_point];
        let right_entries = &entries[split_point..];
        let median_key = right_entries[0].key;

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
            for entry in right_entries {
                insert_leaf_cell(page, page_size, entry);
            }
        }

        // Rewrite left page (current page).
        {
            let page = self.pager.write_page(page_num)?;
            init_leaf(page, page_size);
            set_next_leaf(page, new_page);
            for entry in left_entries {
                insert_leaf_cell(page, page_size, entry);
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
        let insert_pos = entries
            .iter()
            .position(|(k, _)| *k > new_key)
            .unwrap_or(entries.len());

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
                    let entry = read_leaf_entry_raw(page, idx);
                    let payload = self.resolve_overflow(entry)?;
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

    fn resolve_overflow(&mut self, entry: LeafEntry) -> io::Result<Vec<u8>> {
        if let Some(overflow_page) = entry.overflow_page {
            let mut payload = entry.local_payload;
            let overflow_len = entry.total_len - payload.len();
            let overflow_data = read_overflow_chain(self.pager, overflow_page, overflow_len)?;
            payload.extend(overflow_data);
            Ok(payload)
        } else {
            Ok(entry.local_payload)
        }
    }

    fn delete_from(
        &mut self,
        page_num: PageNum,
        key: i64,
        is_root: bool,
    ) -> io::Result<DeleteResult> {
        let page = self.pager.read_page(page_num)?;
        let page_type = page[0];

        match page_type {
            PAGE_TYPE_LEAF => {
                let idx = {
                    let page = self.pager.read_page(page_num)?;
                    find_cell_by_key_leaf(page, key)
                };
                if let Some(idx) = idx {
                    let page_size = self.pager.page_size();
                    let page = self.pager.write_page(page_num)?;
                    delete_leaf_cell(page, page_size, idx);
                    let underflow = !is_root && leaf_is_underfull(page, page_size);
                    Ok(DeleteResult {
                        deleted: true,
                        underflow,
                    })
                } else {
                    Ok(DeleteResult {
                        deleted: false,
                        underflow: false,
                    })
                }
            }
            PAGE_TYPE_INTERIOR => {
                let child_idx = {
                    let page = self.pager.read_page(page_num)?;
                    find_child_index(page, key)
                };
                let child = {
                    let page = self.pager.read_page(page_num)?;
                    get_child_at_index(page, child_idx)
                };

                let child_result = self.delete_from(child, key, false)?;
                if !child_result.deleted {
                    return Ok(DeleteResult {
                        deleted: false,
                        underflow: false,
                    });
                }

                if child_result.underflow {
                    self.rebalance_underflowing_child(page_num, child_idx)?;
                }

                let underflow = if is_root {
                    false
                } else {
                    let page_size = self.pager.page_size();
                    let page = self.pager.read_page(page_num)?;
                    interior_is_underfull(page, page_size)
                };

                Ok(DeleteResult {
                    deleted: true,
                    underflow,
                })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown page type: {}", other),
            )),
        }
    }

    /// If the root is an interior page with no separator keys, copy its only
    /// child into the root page. This preserves the externally-visible root
    /// page number even when the tree height shrinks.
    fn compact_root_if_possible(&mut self) -> io::Result<()> {
        loop {
            let (page_type, cell_count, only_child) = {
                let root = self.pager.read_page(self.root_page)?;
                (root[0], get_cell_count(root), get_right_child(root))
            };

            if page_type != PAGE_TYPE_INTERIOR || cell_count > 0 || only_child == 0 {
                break;
            }

            let child_bytes = self.pager.read_page(only_child)?.to_vec();
            let root = self.pager.write_page(self.root_page)?;
            root.copy_from_slice(&child_bytes);
            self.pager.free_page(only_child)?;
        }

        Ok(())
    }

    fn rebalance_underflowing_child(
        &mut self,
        parent_page_num: PageNum,
        child_idx: usize,
    ) -> io::Result<()> {
        let child_page_num = {
            let parent = self.pager.read_page(parent_page_num)?;
            get_child_at_index(parent, child_idx)
        };

        let child_page_type = {
            let child = self.pager.read_page(child_page_num)?;
            child[0]
        };

        match child_page_type {
            PAGE_TYPE_LEAF => self.rebalance_leaf_child(parent_page_num, child_idx),
            PAGE_TYPE_INTERIOR => self.rebalance_interior_child(parent_page_num, child_idx),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown page type for underflowed child: {}", other),
            )),
        }
    }

    fn rebalance_leaf_child(
        &mut self,
        parent_page_num: PageNum,
        child_idx: usize,
    ) -> io::Result<()> {
        let page_size = self.pager.page_size();
        let mut parent = {
            let page = self.pager.read_page(parent_page_num)?;
            read_interior_node(page)
        };

        if parent.children.len() <= 1 {
            return Ok(());
        }

        let left_idx = if child_idx > 0 { child_idx - 1 } else { 0 };
        let right_idx = left_idx + 1;
        if right_idx >= parent.children.len() {
            return Ok(());
        }

        let left_page_num = parent.children[left_idx];
        let right_page_num = parent.children[right_idx];
        let mut merged_entries = {
            let left_page = self.pager.read_page(left_page_num)?;
            read_all_leaf_entries_raw(left_page)
        };
        let right_entries = {
            let right_page = self.pager.read_page(right_page_num)?;
            read_all_leaf_entries_raw(right_page)
        };
        merged_entries.extend(right_entries);

        let right_next = {
            let right_page = self.pager.read_page(right_page_num)?;
            get_next_leaf(right_page)
        };

        if leaf_entries_fit_in_page(&merged_entries, page_size) {
            let left_page = self.pager.write_page(left_page_num)?;
            write_leaf_entries(left_page, page_size, &merged_entries, right_next);

            parent.keys.remove(left_idx);
            parent.children.remove(right_idx);
            {
                let parent_page = self.pager.write_page(parent_page_num)?;
                write_interior_node(parent_page, page_size, &parent);
            }
            self.pager.free_page(right_page_num)?;
            return Ok(());
        }

        let split_idx = choose_leaf_redistribution_split(&merged_entries, page_size)?;
        let right_side_entries = merged_entries.split_off(split_idx);
        if merged_entries.is_empty() || right_side_entries.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "leaf redistribution produced empty sibling",
            ));
        }

        {
            let left_page = self.pager.write_page(left_page_num)?;
            write_leaf_entries(left_page, page_size, &merged_entries, right_page_num);
        }
        {
            let right_page = self.pager.write_page(right_page_num)?;
            write_leaf_entries(right_page, page_size, &right_side_entries, right_next);
        }
        parent.keys[left_idx] = right_side_entries[0].key;
        let parent_page = self.pager.write_page(parent_page_num)?;
        write_interior_node(parent_page, page_size, &parent);
        Ok(())
    }

    fn rebalance_interior_child(
        &mut self,
        parent_page_num: PageNum,
        child_idx: usize,
    ) -> io::Result<()> {
        let page_size = self.pager.page_size();
        let mut parent = {
            let page = self.pager.read_page(parent_page_num)?;
            read_interior_node(page)
        };
        if parent.children.len() <= 1 {
            return Ok(());
        }

        let left_idx = if child_idx > 0 { child_idx - 1 } else { 0 };
        let right_idx = left_idx + 1;
        if right_idx >= parent.children.len() {
            return Ok(());
        }

        let left_page_num = parent.children[left_idx];
        let right_page_num = parent.children[right_idx];

        let left_node = {
            let page = self.pager.read_page(left_page_num)?;
            if page[0] != PAGE_TYPE_INTERIOR {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected left rebalance sibling to be an interior page",
                ));
            }
            read_interior_node(page)
        };
        let right_node = {
            let page = self.pager.read_page(right_page_num)?;
            if page[0] != PAGE_TYPE_INTERIOR {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected right rebalance sibling to be an interior page",
                ));
            }
            read_interior_node(page)
        };
        let separator_key = parent.keys[left_idx];

        let mut merged_keys = left_node.keys;
        merged_keys.push(separator_key);
        merged_keys.extend(right_node.keys.iter().copied());

        let mut merged_children = left_node.children;
        merged_children.extend(right_node.children.iter().copied());

        if merged_keys.len() + 1 != merged_children.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "interior merge produced inconsistent key/child counts",
            ));
        }

        if interior_entries_fit_in_page(merged_keys.len(), page_size) {
            let merged_node = InteriorNodeData {
                keys: merged_keys,
                children: merged_children,
            };
            let left_page = self.pager.write_page(left_page_num)?;
            write_interior_node(left_page, page_size, &merged_node);

            parent.keys.remove(left_idx);
            parent.children.remove(right_idx);
            {
                let parent_page = self.pager.write_page(parent_page_num)?;
                write_interior_node(parent_page, page_size, &parent);
            }
            self.pager.free_page(right_page_num)?;
            return Ok(());
        }

        let (left_key_count, promoted_key) =
            choose_interior_redistribution_split(&merged_keys, page_size)?;
        let left_keys = merged_keys[..left_key_count].to_vec();
        let right_keys = merged_keys[left_key_count + 1..].to_vec();
        let left_children = merged_children[..left_key_count + 1].to_vec();
        let right_children = merged_children[left_key_count + 1..].to_vec();

        if left_keys.is_empty() || right_keys.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "interior redistribution produced empty sibling",
            ));
        }

        let left_node = InteriorNodeData {
            keys: left_keys,
            children: left_children,
        };
        let right_node = InteriorNodeData {
            keys: right_keys,
            children: right_children,
        };

        {
            let left_page = self.pager.write_page(left_page_num)?;
            write_interior_node(left_page, page_size, &left_node);
        }
        {
            let right_page = self.pager.write_page(right_page_num)?;
            write_interior_node(right_page, page_size, &right_node);
        }
        parent.keys[left_idx] = promoted_key;
        let parent_page = self.pager.write_page(parent_page_num)?;
        write_interior_node(parent_page, page_size, &parent);
        Ok(())
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
                let entries = read_all_leaf_entries_raw(page);
                let next = get_next_leaf(page);
                (entries, next)
            };

            for entry in entries {
                let key = entry.key;
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
                let payload = self.resolve_overflow(entry)?;
                results.push(Entry { key, payload });
            }

            if next_leaf == 0 {
                break;
            }
            current = next_leaf;
        }

        Ok(results)
    }

    fn collect_tree_pages(
        pager: &mut Pager,
        page_num: PageNum,
        out: &mut Vec<PageNum>,
        visited: &mut HashSet<PageNum>,
    ) -> io::Result<()> {
        if !visited.insert(page_num) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("duplicate or cyclic B+tree page reference: {}", page_num),
            ));
        }

        let (page_type, children) = {
            let page = pager.read_page(page_num)?;
            match page[0] {
                PAGE_TYPE_LEAF => (PAGE_TYPE_LEAF, Vec::new()),
                PAGE_TYPE_INTERIOR => (PAGE_TYPE_INTERIOR, read_interior_node(page).children),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown page type: {}", other),
                    ))
                }
            }
        };

        if page_type == PAGE_TYPE_INTERIOR {
            for child in children {
                Self::collect_tree_pages(pager, child, out, visited)?;
            }
        }
        out.push(page_num);
        Ok(())
    }
}

enum InsertResult {
    Ok,
    Split { median_key: i64, new_page: PageNum },
}

struct DeleteResult {
    deleted: bool,
    underflow: bool,
}

#[derive(Debug)]
struct InteriorNodeData {
    keys: Vec<i64>,
    children: Vec<PageNum>,
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
fn insert_leaf_cell(page: &mut [u8], _page_size: usize, entry: &LeafEntry) {
    let cell_count = get_cell_count(page);
    let cell_size = LEAF_CELL_HEADER_SIZE
        + entry.local_payload.len()
        + if entry.overflow_page.is_some() {
            4
        } else {
            0
        };

    // Allocate cell space (grows downward).
    let new_content_offset = get_cell_content_offset(page) - cell_size;
    set_cell_content_offset(page, new_content_offset);

    // Write cell data.
    let mut off = new_content_offset;
    page[off..off + 8].copy_from_slice(&entry.key.to_be_bytes());
    off += 8;
    page[off..off + 4].copy_from_slice(&(entry.total_len as u32).to_be_bytes());
    off += 4;
    page[off..off + entry.local_payload.len()].copy_from_slice(&entry.local_payload);
    off += entry.local_payload.len();
    if let Some(pg) = entry.overflow_page {
        page[off..off + 4].copy_from_slice(&pg.to_be_bytes());
    }

    // Find insertion position in sorted order.
    let insert_pos = find_insert_pos_leaf(page, cell_count, entry.key);

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

fn read_leaf_entry_raw(page: &[u8], idx: usize) -> LeafEntry {
    let offset = get_cell_offset(page, idx);
    let key = i64::from_be_bytes(page[offset..offset + 8].try_into().unwrap());
    let total_len = u32::from_be_bytes(page[offset + 8..offset + 12].try_into().unwrap()) as usize;

    let max_local = max_local_payload(page.len());
    let local_len = if total_len > max_local {
        max_local
    } else {
        total_len
    };

    let payload_offset = offset + 12;
    let local_payload = page[payload_offset..payload_offset + local_len].to_vec();

    let overflow_page = if total_len > local_len {
        let overflow_offset = payload_offset + local_len;
        Some(u32::from_be_bytes(
            page[overflow_offset..overflow_offset + 4].try_into().unwrap(),
        ))
    } else {
        None
    };

    LeafEntry {
        key,
        total_len,
        local_payload,
        overflow_page,
    }
}

fn read_all_leaf_entries_raw(page: &[u8]) -> Vec<LeafEntry> {
    let cell_count = get_cell_count(page);
    let mut entries = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        entries.push(read_leaf_entry_raw(page, i));
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

fn read_interior_node(page: &[u8]) -> InteriorNodeData {
    let entries = read_all_interior_entries(page);
    let mut keys = Vec::with_capacity(entries.len());
    let mut children = Vec::with_capacity(entries.len() + 1);
    for (key, left_child) in entries {
        keys.push(key);
        children.push(left_child);
    }
    children.push(get_right_child(page));
    InteriorNodeData { keys, children }
}

fn write_interior_node(page: &mut [u8], page_size: usize, node: &InteriorNodeData) {
    debug_assert_eq!(node.keys.len() + 1, node.children.len());

    init_interior(page, page_size);
    if node.children.is_empty() {
        return;
    }
    set_right_child(page, *node.children.last().unwrap());
    for i in 0..node.keys.len() {
        insert_interior_cell(page, page_size, node.children[i], node.keys[i]);
    }
    set_right_child(page, *node.children.last().unwrap());
}

fn leaf_logical_used_bytes(page: &[u8]) -> usize {
    let cell_count = get_cell_count(page);
    let mut used = PAGE_HEADER_SIZE + cell_count * CELL_PTR_SIZE;
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let payload_size =
            u32::from_be_bytes(page[offset + 8..offset + 12].try_into().unwrap()) as usize;
        used += LEAF_CELL_HEADER_SIZE + payload_size;
    }
    used
}

fn leaf_is_underfull(page: &[u8], page_size: usize) -> bool {
    leaf_logical_used_bytes(page) * LEAF_MIN_UTILIZATION_DENOMINATOR
        < page_size * LEAF_MIN_UTILIZATION_NUMERATOR
}

fn interior_logical_used_bytes(page: &[u8]) -> usize {
    let cell_count = get_cell_count(page);
    PAGE_HEADER_SIZE + cell_count * (CELL_PTR_SIZE + INTERIOR_CELL_SIZE)
}

fn interior_is_underfull(page: &[u8], page_size: usize) -> bool {
    interior_logical_used_bytes(page) * INTERIOR_MIN_UTILIZATION_DENOMINATOR
        < page_size * INTERIOR_MIN_UTILIZATION_NUMERATOR
}

fn interior_entries_required_bytes(key_count: usize) -> usize {
    PAGE_HEADER_SIZE + key_count * (CELL_PTR_SIZE + INTERIOR_CELL_SIZE)
}

fn interior_entries_fit_in_page(key_count: usize, page_size: usize) -> bool {
    interior_entries_required_bytes(key_count) <= page_size
}

fn leaf_entries_required_bytes(entries: &[LeafEntry]) -> usize {
    PAGE_HEADER_SIZE
        + entries.len() * CELL_PTR_SIZE
        + entries
            .iter()
            .map(|entry| {
                LEAF_CELL_HEADER_SIZE
                    + entry.local_payload.len()
                    + if entry.overflow_page.is_some() { 4 } else { 0 }
            })
            .sum::<usize>()
}

fn leaf_entries_fit_in_page(entries: &[LeafEntry], page_size: usize) -> bool {
    leaf_entries_required_bytes(entries) <= page_size
}

fn choose_leaf_redistribution_split(
    entries: &[LeafEntry],
    page_size: usize,
) -> io::Result<usize> {
    if entries.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "need at least two entries for leaf redistribution",
        ));
    }

    let mut prefix_entry_bytes = Vec::with_capacity(entries.len() + 1);
    prefix_entry_bytes.push(0usize);
    for entry in entries {
        let cell_size = LEAF_CELL_HEADER_SIZE
            + entry.local_payload.len()
            + if entry.overflow_page.is_some() { 4 } else { 0 };
        let next = prefix_entry_bytes.last().copied().unwrap() + cell_size;
        prefix_entry_bytes.push(next);
    }

    let total_entry_bytes = *prefix_entry_bytes.last().unwrap();
    let mut best: Option<(usize, usize)> = None;
    for split_idx in 1..entries.len() {
        let left_entry_bytes = prefix_entry_bytes[split_idx];
        let right_entry_bytes = total_entry_bytes - left_entry_bytes;
        let left_size = PAGE_HEADER_SIZE + split_idx * CELL_PTR_SIZE + left_entry_bytes;
        let right_count = entries.len() - split_idx;
        let right_size = PAGE_HEADER_SIZE + right_count * CELL_PTR_SIZE + right_entry_bytes;

        if left_size > page_size || right_size > page_size {
            continue;
        }

        let balance_gap = left_size.abs_diff(right_size);
        match best {
            Some((_, best_gap)) if best_gap <= balance_gap => {}
            _ => best = Some((split_idx, balance_gap)),
        }
    }

    best.map(|(idx, _)| idx).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "could not find valid leaf redistribution split",
        )
    })
}

fn choose_interior_redistribution_split(
    keys: &[i64],
    page_size: usize,
) -> io::Result<(usize, i64)> {
    if keys.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "need at least three keys for interior redistribution",
        ));
    }

    let mut best: Option<(usize, usize)> = None;
    for promoted_idx in 1..keys.len() - 1 {
        let left_key_count = promoted_idx;
        let right_key_count = keys.len() - promoted_idx - 1;
        if !interior_entries_fit_in_page(left_key_count, page_size)
            || !interior_entries_fit_in_page(right_key_count, page_size)
        {
            continue;
        }

        let gap = left_key_count.abs_diff(right_key_count);
        match best {
            Some((_, best_gap)) if best_gap <= gap => {}
            _ => best = Some((promoted_idx, gap)),
        }
    }

    best.map(|(idx, _)| (idx, keys[idx])).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "could not find valid interior redistribution split",
        )
    })
}

fn write_leaf_entries(
    page: &mut [u8],
    page_size: usize,
    entries: &[LeafEntry],
    next_leaf: PageNum,
) {
    init_leaf(page, page_size);
    set_next_leaf(page, next_leaf);
    for entry in entries {
        insert_leaf_cell(page, page_size, entry);
    }
}

fn find_child_index(page: &[u8], key: i64) -> usize {
    let cell_count = get_cell_count(page);
    for i in 0..cell_count {
        let offset = get_cell_offset(page, i);
        let cell_key = i64::from_be_bytes(page[offset + 4..offset + 12].try_into().unwrap());
        if key < cell_key {
            return i;
        }
    }
    cell_count
}

fn get_child_at_index(page: &[u8], idx: usize) -> PageNum {
    let cell_count = get_cell_count(page);
    if idx < cell_count {
        let offset = get_cell_offset(page, idx);
        u32::from_be_bytes(page[offset..offset + 4].try_into().unwrap())
    } else {
        get_right_child(page)
    }
}

/// Find which child page to descend into for a given key.
fn find_child(page: &[u8], key: i64) -> PageNum {
    let idx = find_child_index(page, key);
    get_child_at_index(page, idx)
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

    fn allocate_leaf_with_range(
        pager: &mut Pager,
        start_key: i64,
        count: usize,
        payload: &[u8],
    ) -> PageNum {
        let page_size = pager.page_size();
        let page_num = pager.allocate_page().unwrap();
        let entries: Vec<LeafEntry> = (0..count)
            .map(|idx| LeafEntry {
                key: start_key + idx as i64,
                total_len: payload.len(),
                local_payload: payload.to_vec(),
                overflow_page: None,
            })
            .collect();
        {
            let page = pager.write_page(page_num).unwrap();
            write_leaf_entries(page, page_size, &entries, 0);
        }
        page_num
    }

    fn allocate_interior_with_children(
        pager: &mut Pager,
        keys: Vec<i64>,
        children: Vec<PageNum>,
    ) -> PageNum {
        let page_size = pager.page_size();
        let page_num = pager.allocate_page().unwrap();
        let node = InteriorNodeData { keys, children };
        {
            let page = pager.write_page(page_num).unwrap();
            write_interior_node(page, page_size, &node);
        }
        page_num
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
            assert_eq!(
                result,
                Some(payload.clone()),
                "key {} not found after split",
                i
            );
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
    fn delete_existing_and_missing_keys() {
        let path = temp_db_path("btree_delete.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        tree.insert(10, b"ten").unwrap();
        tree.insert(20, b"twenty").unwrap();
        tree.insert(30, b"thirty").unwrap();

        assert!(tree.delete(20).unwrap());
        assert_eq!(tree.lookup(20).unwrap(), None);
        assert!(!tree.delete(20).unwrap());

        assert_eq!(tree.lookup(10).unwrap(), Some(b"ten".to_vec()));
        assert_eq!(tree.lookup(30).unwrap(), Some(b"thirty".to_vec()));

        let keys: Vec<i64> = tree
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys, vec![10, 30]);

        cleanup(&path);
    }

    #[test]
    fn delete_after_leaf_splits() {
        let path = temp_db_path("btree_delete_split.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        let payload = vec![0xAA; 80];
        for i in 0..80 {
            tree.insert(i, &payload).unwrap();
        }

        for key in [0_i64, 1, 10, 39, 40, 79] {
            assert!(
                tree.delete(key).unwrap(),
                "expected key {} to be deleted",
                key
            );
            assert_eq!(tree.lookup(key).unwrap(), None);
        }

        for key in [2_i64, 11, 41, 78] {
            assert_eq!(tree.lookup(key).unwrap(), Some(payload.clone()));
        }

        let keys: Vec<i64> = tree
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys.len(), 74);
        assert!(!keys.contains(&0));
        assert!(!keys.contains(&1));
        assert!(!keys.contains(&10));
        assert!(!keys.contains(&39));
        assert!(!keys.contains(&40));
        assert!(!keys.contains(&79));

        cleanup(&path);
    }

    #[test]
    fn delete_compacts_root_after_leftmost_leaf_becomes_empty() {
        let path = temp_db_path("btree_delete_compact_root.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        let payload = vec![0xA5; 100];
        for i in 0..40 {
            tree.insert(i, &payload).unwrap();
        }

        let initial_root_page = tree.root_page();
        let initial_root_type = {
            let page = tree.pager.read_page(initial_root_page).unwrap();
            page[0]
        };
        assert_eq!(initial_root_type, PAGE_TYPE_INTERIOR);

        for key in 0..26 {
            assert!(
                tree.delete(key).unwrap(),
                "expected key {} to be deleted",
                key
            );
        }

        let root_type_after = {
            let page = tree.pager.read_page(initial_root_page).unwrap();
            page[0]
        };
        assert_eq!(root_type_after, PAGE_TYPE_LEAF);

        let keys: Vec<i64> = tree
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys, (26..40).map(|k| k as i64).collect::<Vec<_>>());

        cleanup(&path);
    }

    #[test]
    fn delete_compacts_multi_level_tree_to_single_leaf() {
        let path = temp_db_path("btree_delete_multi_level_compact.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        let payload = vec![0xB6; 80];
        for i in 0..300 {
            tree.insert(i, &payload).unwrap();
        }

        for key in 0..299 {
            assert!(
                tree.delete(key).unwrap(),
                "expected key {} to be deleted",
                key
            );
        }

        assert_eq!(tree.lookup(299).unwrap(), Some(payload.clone()));
        let keys: Vec<i64> = tree
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys, vec![299]);

        let root_type = {
            let page = tree.pager.read_page(tree.root_page()).unwrap();
            page[0]
        };
        assert_eq!(root_type, PAGE_TYPE_LEAF);

        cleanup(&path);
    }

    #[test]
    fn delete_compaction_reclaims_pages_to_freelist() {
        let path = temp_db_path("btree_delete_reclaim_freelist.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        {
            let mut tree = BTree::new(&mut pager, root);
            let payload = vec![0xC7; 80];
            for i in 0..300 {
                tree.insert(i, &payload).unwrap();
            }
            for key in 0..299 {
                assert!(tree.delete(key).unwrap());
            }
        }

        let reclaimed = pager.header().freelist_count;
        assert!(reclaimed > 0);
        let page_count_before = pager.page_count();
        for _ in 0..reclaimed {
            pager.allocate_page().unwrap();
        }
        assert_eq!(pager.page_count(), page_count_before);

        cleanup(&path);
    }

    #[test]
    fn delete_merges_non_empty_underfull_leaf() {
        let path = temp_db_path("btree_delete_non_empty_merge.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        let payload = vec![0xE1; 100];
        for i in 0..40 {
            tree.insert(i, &payload).unwrap();
        }

        let root_before = tree.root_page();
        assert_eq!(
            tree.pager.read_page(root_before).unwrap()[0],
            PAGE_TYPE_INTERIOR
        );

        for key in 0..6 {
            assert!(tree.delete(key).unwrap());
        }

        let root_after = tree.pager.read_page(root_before).unwrap();
        assert_eq!(root_after[0], PAGE_TYPE_LEAF);

        let keys: Vec<i64> = tree
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert_eq!(keys, (6..40).map(|k| k as i64).collect::<Vec<_>>());

        cleanup(&path);
    }

    #[test]
    fn delete_redistributes_non_empty_underfull_leaf() {
        let path = temp_db_path("btree_delete_non_empty_redistribute.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        let payload = vec![0xE2; 100];
        for i in 0..50 {
            tree.insert(i, &payload).unwrap();
        }

        let root_page_num = tree.root_page();
        assert_eq!(
            tree.pager.read_page(root_page_num).unwrap()[0],
            PAGE_TYPE_INTERIOR
        );

        for key in 0..7 {
            assert!(tree.delete(key).unwrap());
        }

        let (separator_key, left_child, right_child) = {
            let root_page = tree.pager.read_page(root_page_num).unwrap();
            assert_eq!(root_page[0], PAGE_TYPE_INTERIOR);
            assert_eq!(get_cell_count(root_page), 1);
            let separator = read_all_interior_entries(root_page)[0].0;
            (
                separator,
                get_child_at_index(root_page, 0),
                get_child_at_index(root_page, 1),
            )
        };

        let left_count = {
            let left_page = tree.pager.read_page(left_child).unwrap();
            get_cell_count(left_page)
        };
        let (right_count, right_first_key) = {
            let right_page = tree.pager.read_page(right_child).unwrap();
            let count = get_cell_count(right_page);
            let first_key = read_all_leaf_entries_raw(right_page)[0].key;
            (count, first_key)
        };

        assert!(left_count > 11, "expected redistributed left leaf");
        assert!(right_count < 32, "expected redistributed right leaf");
        assert_eq!(separator_key, right_first_key);

        let keys: Vec<i64> = tree
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert_eq!(keys, (7..50).map(|k| k as i64).collect::<Vec<_>>());

        cleanup(&path);
    }

    #[test]
    fn delete_rebalances_non_empty_underfull_interior() {
        let path = temp_db_path("btree_delete_non_empty_interior_rebalance.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();

        let payload = vec![0xD1; 100];
        let left_leaf_0 = allocate_leaf_with_range(&mut pager, 0, 24, &payload);
        let left_leaf_1 = allocate_leaf_with_range(&mut pager, 500, 24, &payload);
        let left_leaf_2 = allocate_leaf_with_range(&mut pager, 700, 24, &payload);
        let right_leaf_0 = allocate_leaf_with_range(&mut pager, 1000, 24, &payload);
        let right_leaf_1 = allocate_leaf_with_range(&mut pager, 1500, 24, &payload);
        let right_leaf_2 = allocate_leaf_with_range(&mut pager, 1700, 24, &payload);

        let left_interior = allocate_interior_with_children(
            &mut pager,
            vec![500, 700],
            vec![left_leaf_0, left_leaf_1, left_leaf_2],
        );
        let right_interior = allocate_interior_with_children(
            &mut pager,
            vec![1500, 1700],
            vec![right_leaf_0, right_leaf_1, right_leaf_2],
        );

        {
            let page_size = pager.page_size();
            let root_page = pager.write_page(root).unwrap();
            write_interior_node(
                root_page,
                page_size,
                &InteriorNodeData {
                    keys: vec![1000],
                    children: vec![left_interior, right_interior],
                },
            );
        }

        let mut tree = BTree::new(&mut pager, root);
        let root_before = tree.pager.read_page(root).unwrap();
        assert_eq!(root_before[0], PAGE_TYPE_INTERIOR);
        assert_eq!(get_cell_count(root_before), 1);

        let page_size = tree.pager.page_size();
        let (left_cell_count, left_underfull) = {
            let left_before = tree.pager.read_page(left_interior).unwrap();
            (
                get_cell_count(left_before),
                interior_is_underfull(left_before, page_size),
            )
        };
        assert!(left_cell_count > 0);
        assert!(left_underfull);

        assert!(tree.delete(10).unwrap());
        assert_eq!(tree.lookup(10).unwrap(), None);
        assert_eq!(tree.lookup(11).unwrap(), Some(payload.clone()));
        assert_eq!(tree.lookup(1005).unwrap(), Some(payload.clone()));

        let root_after = tree.pager.read_page(root).unwrap();
        assert_eq!(root_after[0], PAGE_TYPE_INTERIOR);
        assert_eq!(
            get_cell_count(root_after),
            5,
            "expected root to compact merged interior child in-place"
        );

        for key in [0_i64, 11, 23, 500, 523, 700, 723, 1000, 1023, 1500, 1523] {
            assert_eq!(
                tree.lookup(key).unwrap(),
                Some(payload.clone()),
                "expected key {} to remain readable",
                key
            );
        }

        cleanup(&path);
    }

    #[test]
    fn rebalance_interior_child_redistributes_when_merge_does_not_fit() {
        let path = temp_db_path("btree_interior_redistribute.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();

        let filler_payload = vec![0xD2; 8];
        let left_children: Vec<PageNum> = (0..81)
            .map(|idx| {
                allocate_leaf_with_range(&mut pager, 10_000 + idx as i64, 1, &filler_payload)
            })
            .collect();
        let right_children: Vec<PageNum> = (0..241)
            .map(|idx| {
                allocate_leaf_with_range(&mut pager, 20_000 + idx as i64, 1, &filler_payload)
            })
            .collect();

        let left_keys: Vec<i64> = (0..80).map(|idx| idx as i64).collect();
        let right_keys: Vec<i64> = (1001..1241).collect();
        let left_interior = allocate_interior_with_children(&mut pager, left_keys, left_children);
        let right_interior =
            allocate_interior_with_children(&mut pager, right_keys, right_children);

        {
            let page_size = pager.page_size();
            let root_page = pager.write_page(root).unwrap();
            write_interior_node(
                root_page,
                page_size,
                &InteriorNodeData {
                    keys: vec![1000],
                    children: vec![left_interior, right_interior],
                },
            );
        }

        let freelist_before = pager.header().freelist_count;
        let mut tree = BTree::new(&mut pager, root);
        tree.rebalance_interior_child(root, 0).unwrap();

        let root_page = tree.pager.read_page(root).unwrap();
        let root_node = read_interior_node(root_page);
        assert_eq!(root_node.keys.len(), 1);
        assert_eq!(root_node.children.len(), 2);
        assert_eq!(
            root_node.keys[0], 1080,
            "expected parent separator to be updated from redistribution split"
        );

        let page_size = tree.pager.page_size();
        let left_cell_count = {
            let left_page = tree.pager.read_page(left_interior).unwrap();
            assert!(!interior_is_underfull(left_page, page_size));
            get_cell_count(left_page)
        };
        let right_cell_count = {
            let right_page = tree.pager.read_page(right_interior).unwrap();
            assert!(!interior_is_underfull(right_page, page_size));
            get_cell_count(right_page)
        };
        assert_eq!(left_cell_count, 160);
        assert_eq!(right_cell_count, 160);
        assert_eq!(
            tree.pager.header().freelist_count,
            freelist_before,
            "redistribution should not reclaim either sibling page"
        );

        cleanup(&path);
    }

    #[test]
    fn reclaim_tree_returns_pages_to_freelist() {
        let path = temp_db_path("btree_reclaim_tree.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let root_page = {
            let mut tree = BTree::new(&mut pager, root);
            let payload = vec![0xD8; 80];
            for i in 0..240 {
                tree.insert(i, &payload).unwrap();
            }
            tree.root_page()
        };

        let freelist_before = pager.header().freelist_count;
        let page_count_before = pager.page_count();
        let reclaimed = BTree::reclaim_tree(&mut pager, root_page).unwrap();

        assert!(reclaimed > 0);
        assert_eq!(
            pager.header().freelist_count,
            freelist_before + reclaimed as u32
        );

        for _ in 0..reclaimed {
            pager.allocate_page().unwrap();
        }
        assert_eq!(pager.page_count(), page_count_before);

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

    #[test]
    fn overflow_payload() {
        let path = temp_db_path("btree_overflow.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        let root = BTree::create(&mut pager).unwrap();
        let mut tree = BTree::new(&mut pager, root);

        // Payload larger than page size (4096).
        let payload = vec![0xDD; 5000];
        tree.insert(1, &payload).unwrap();

        let result = tree.lookup(1).unwrap();
        assert_eq!(result, Some(payload.clone()));

        // Scan should also work.
        let entries = tree.scan_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload.len(), 5000);
        assert_eq!(entries[0].payload, payload);

        cleanup(&path);
    }
}
