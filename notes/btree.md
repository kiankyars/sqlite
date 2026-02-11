# B+tree Notes

## Page Layout

### Common header (9 bytes)
- `[0]` page_type: u8 (1=interior, 2=leaf)
- `[1..3]` cell_count: u16 (big-endian)
- `[3..5]` cell_content_offset: u16 (start of cell content, grows down)
- `[5..9]` type-specific: u32
  - Interior: right_child page number
  - Leaf: next_leaf page number (0 = none)

### Cell offset array
Starts at byte 9. Each entry is a u16 offset into the page. Entries are sorted by key.

### Interior cell (12 bytes)
- `[0..4]` left_child: u32
- `[4..12]` key: i64 (big-endian)

### Leaf cell (variable)
- `[0..8]` key: i64 (big-endian)
- `[8..12]` payload_size: u32
- `[12..]` payload bytes

## API

```rust
BTree::create(pager) -> PageNum       // Allocate and init empty leaf
BTree::new(pager, root_page)          // Open existing tree
tree.insert(key, payload)             // Insert or update
tree.lookup(key) -> Option<Vec<u8>>   // Point lookup
tree.scan_all() -> Vec<Entry>         // Full ordered scan
tree.scan_range(min, max) -> Vec<Entry>  // Range scan
tree.root_page() -> PageNum           // Current root (may change after split)
```

## Design Decisions

- Keys are i64 stored big-endian so byte ordering matches numeric ordering.
- Leaf splitting: collect all entries + new one, sort, split at midpoint. Left leaf points to right sibling via next_leaf.
- Interior splitting: similar collect-and-split approach. Median key is promoted to parent.
- When root splits, a new root is created with one key and two children.
- Update = delete old cell + insert new cell (simple, avoids in-place resize).
- No overflow pages yet — payload must fit within a single page cell.

## What's Next

- **Schema table** (task #8): Use a B+tree to store table/index metadata.
- **Overflow pages**: For large payloads exceeding ~page_size/4.
- **Delete operation**: Remove cells from leaves; merge underflowing nodes (task #18).
- **B+tree merge** (task #18): Rebalance on delete.
