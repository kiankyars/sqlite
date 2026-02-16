# Planner Stats Selectivity/Cost Refinement

## Scope completed

Refined planner stats behavior in `crates/planner` so `plan_where_with_stats` can make better decisions for conjunction/disjunction plans.

- `AND` path preference now uses stats-aware cost comparison when both:
  - a direct equality/prefix candidate (`IndexEq` / `IndexPrefixRange`), and
  - an `IndexAnd` intersection candidate
  are available.
- Stats-based `IndexOr` and `IndexAnd` output-row estimates now combine branch selectivities with probability-style formulas instead of raw sum/min reductions.

## Behavior changes

- For stats-driven planning, `choose_preferred_and_path(...)` now picks the cheaper of equality/prefix and intersection alternatives using `estimated_access_path_cost(..., Some(stats))`.
- `IndexOr` output rows now estimate union selectivity via:
  - `1 - Π(1 - s_i)` where `s_i` is each branch selectivity.
- `IndexAnd` output rows now estimate intersection selectivity via:
  - `Π(s_i)`.

These changes reduce cases where stats planning over-favors expensive intersections or overestimates OR/AND cardinalities from additive/min shortcuts.

## Tests added

In `crates/planner/src/lib.rs`:

- `plan_where_with_stats_prefers_selective_eq_over_costly_and_intersection`
- `combine_or_selectivity_uses_union_probability`
- `combine_and_selectivity_multiplies_probabilities`

## Validation

- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner`
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast`
