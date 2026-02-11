# Join Index Probe Optimization

## Summary

Join execution now uses index-probed nested-loop joins when the ON condition
is a simple equality between a column in the right table (that has a
single-column index) and an expression over the left-side tables.  This avoids
scanning all right-table rows for every left row.

## Implementation Details

### Analysis Phase (`analyze_join_index_probe`)

For each join step, the ON condition is inspected for the pattern:

```
right_table.indexed_col = <left_expression>
```

(or the reversed form `<left_expression> = right_table.indexed_col`).

The analysis:
1. Checks if the ON condition is a simple `Expr::BinaryOp { op: Eq, .. }`
2. Identifies which side is a column reference on the right table
3. Checks that the other side only references columns from already-joined
   (left-side) tables
4. Looks up single-column indexes on the right table for the referenced column

If all conditions are met, a `JoinIndexProbe` is returned containing the
index metadata and the left-side expression to evaluate per row.

### Execution Phase (`execute_join_with_index_probe`)

For each left row:
1. Evaluate the left-side expression against the current left row to get a
   probe value
2. Use `index_eq_rowids()` to look up matching rowids in the right table's
   index
3. Fetch right-table rows by rowid
4. Combine with left row and apply residual ON condition

### Scope

- **Supported join types**: INNER JOIN, LEFT JOIN (with index probe)
- **Fallback**: RIGHT JOIN and FULL OUTER JOIN always use full-scan nested-loop
  (they need to track unmatched right rows, which requires access to all right
  rows)
- **ON condition patterns**: Only simple `col = expr` equalities are detected;
  compound AND/OR conditions fall back to full scan
- **Index types**: Only single-column indexes are used for probing

### Complexity Improvement

| Scenario | Before | After |
|----------|--------|-------|
| INNER/LEFT JOIN with indexed right col | O(L × R) | O(L × log(R) + matches) |
| RIGHT/FULL JOIN | O(L × R) | O(L × R) (unchanged) |
| Non-equality ON | O(L × R) | O(L × R) (unchanged) |

### Test Coverage

9 new integration tests added:
- `inner_join_uses_index_probe_on_right_table` — basic index probe
- `inner_join_index_probe_reversed_on_condition` — reversed ON sides
- `left_join_uses_index_probe_preserves_unmatched` — LEFT JOIN with null extension
- `right_join_falls_back_to_full_scan_with_index` — RIGHT JOIN fallback
- `join_index_probe_with_no_matching_rows` — empty result
- `join_index_probe_with_duplicate_values` — duplicate join keys
- `join_index_probe_with_text_key` — text index keys
- `join_without_index_falls_back_to_full_scan` — no-index fallback
- `join_index_probe_with_group_by_aggregate` — index probe + GROUP BY

### Future Work

- Support multi-column index probes for composite ON conditions
- Support AND conjunctions in ON (probe on one equality, filter residual)
- Join reordering with cost estimation
- Hash join for large tables without indexes
