#!/usr/bin/env bash
#
# test.sh — Test harness for ralph-sqlite
#
# Usage:
#   ./test.sh          Run all tests (full mode)
#   ./test.sh --fast   Run deterministic 10% sample (fast mode)
#
# Environment:
#   AGENT_ID    Seed for deterministic sampling in --fast mode (default: "default")
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FAST_MODE=0
if [[ "${1:-}" == "--fast" ]]; then
    FAST_MODE=1
    shift
fi

AGENT_ID="${AGENT_ID:-default}"
PASS=0
FAIL=0
SKIP=0
FAILURES=()

# ─── Helpers ───────────────────────────────────────────────────────────────────

log_pass() {
    PASS=$((PASS + 1))
    echo "  PASS: $1"
}

log_fail() {
    FAIL=$((FAIL + 1))
    FAILURES+=("$1: $2")
    echo "  FAIL: $1 — $2"
}

log_skip() {
    SKIP=$((SKIP + 1))
    echo "  SKIP: $1 (sampled out)"
}

# Deterministic sampling: hash (AGENT_ID + test_name), take last 2 hex digits,
# convert to decimal, accept if < 26 (≈10.2% of 0–255 range).
should_run_test() {
    local test_name="$1"
    if [[ "$FAST_MODE" -eq 0 ]]; then
        return 0  # full mode: always run
    fi
    local hash
    hash=$(printf '%s:%s' "$AGENT_ID" "$test_name" | md5sum | cut -c1-2)
    local val=$((16#$hash))
    if [[ "$val" -lt 26 ]]; then
        return 0  # selected
    else
        return 1  # sampled out
    fi
}

# ─── Section 1: Cargo Tests ───────────────────────────────────────────────────

run_cargo_tests() {
    echo ""
    echo "══════════════════════════════════════════════"
    echo " Section 1: Cargo unit tests"
    echo "══════════════════════════════════════════════"

    local test_name="cargo-test"
    if ! should_run_test "$test_name"; then
        log_skip "$test_name"
        return
    fi

    local output
    if output=$(cargo test --workspace 2>&1); then
        log_pass "$test_name"
    else
        log_fail "$test_name" "cargo test failed"
        echo "$output" | tail -20
    fi
}

# ─── Section 2: Oracle Tests (SQL semantics) ──────────────────────────────────
#
# These tests run SQL through both sqlite3 (oracle) and ralph-sqlite, comparing
# output. Placeholder until the engine can execute queries.

run_oracle_tests() {
    echo ""
    echo "══════════════════════════════════════════════"
    echo " Section 2: Oracle comparison tests"
    echo "══════════════════════════════════════════════"

    if ! command -v sqlite3 &>/dev/null; then
        echo "  WARNING: sqlite3 not found, skipping oracle tests"
        return
    fi

    # Oracle test definitions: each is a (name, sql) pair.
    # As the engine develops, these will run against both sqlite3 and ralph-sqlite.
    local -a ORACLE_TESTS=(
        "select-literal|SELECT 1;"
        "select-arithmetic|SELECT 1 + 2;"
        "select-string|SELECT 'hello';"
    )

    for entry in "${ORACLE_TESTS[@]}"; do
        local name="${entry%%|*}"
        local sql="${entry#*|}"
        local test_name="oracle-$name"

        if ! should_run_test "$test_name"; then
            log_skip "$test_name"
            continue
        fi

        # Run through sqlite3 oracle
        local oracle_out
        if ! oracle_out=$(echo "$sql" | sqlite3 2>&1); then
            log_fail "$test_name" "sqlite3 oracle error: $oracle_out"
            continue
        fi

        # TODO: Once ralph-sqlite has a CLI or can execute queries,
        # run the same SQL and compare output to oracle_out.
        # For now, just verify the oracle produces output.
        if [[ -n "$oracle_out" ]]; then
            log_pass "$test_name (oracle-only, engine pending)"
        else
            log_fail "$test_name" "oracle produced empty output"
        fi
    done
}

# ─── Section 3: Build Check ───────────────────────────────────────────────────

run_build_check() {
    echo ""
    echo "══════════════════════════════════════════════"
    echo " Section 3: Build check"
    echo "══════════════════════════════════════════════"

    local test_name="cargo-build"
    if ! should_run_test "$test_name"; then
        log_skip "$test_name"
        return
    fi

    local output
    if output=$(cargo build --workspace 2>&1); then
        log_pass "$test_name"
    else
        log_fail "$test_name" "cargo build failed"
        echo "$output" | tail -20
    fi
}

# ─── Run All Sections ─────────────────────────────────────────────────────────

echo "ralph-sqlite test harness"
if [[ "$FAST_MODE" -eq 1 ]]; then
    echo "Mode: FAST (10% deterministic sample, seed: $AGENT_ID)"
else
    echo "Mode: FULL"
fi

run_build_check
run_cargo_tests
run_oracle_tests

# ─── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════"
echo " Summary"
echo "══════════════════════════════════════════════"
echo "  Passed:  $PASS"
echo "  Failed:  $FAIL"
echo "  Skipped: $SKIP"
echo "  Total:   $((PASS + FAIL + SKIP))"
echo ""

if [[ "$FAIL" -gt 0 ]]; then
    echo "FAILURES:"
    for f in "${FAILURES[@]}"; do
        echo "  - $f"
    done
    echo ""
    # Write failures to log
    printf '%s\n' "${FAILURES[@]}" > "$SCRIPT_DIR/test_failures.log"
    exit 1
fi

echo "All tests passed."
exit 0
