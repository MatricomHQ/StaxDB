# StaxDB Performance Optimizations

This document tracks the performance improvements made to StaxDB.

## Baseline Performance

The following table shows the baseline performance of StaxDB before any optimizations were applied. The benchmarks were run with 64 threads.

| Key Type | Operation | Time (ns) |
|---|---|---|
| Sequential | Insert | 166.20 |
| Sequential | Get | 62.93 |
| Long Sequential | Insert | 206.84 |
| Long Sequential | Get | 65.76 |
| Random | Insert | N/A (Timeout) |
| Random | Get | N/A (Timeout) |

## Optimization 1: Increase Arena Size to 128KB (Failed)

Increased the `ThreadLocalAllocator` arena size from 32KB to 128KB. This change resulted in a performance *degradation*.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 166.20 | 220.68 | -32.78% |
| Sequential | Get | 62.93 | 84.48 | -34.25% |
| Long Sequential | Insert | 206.84 | 360.17 | -74.13% |
| Long Sequential | Get | 65.76 | 123.68 | -88.07% |

## Optimization 2: Inline `get_visible_record`

Inlined the `get_visible_record` function. This resulted in a marginal improvement for sequential gets, but a significant degradation for long sequential inserts.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 166.20 | 163.86 | +1.41% |
| Sequential | Get | 62.93 | 62.50 | +0.68% |
| Long Sequential | Insert | 206.84 | 326.11 | -57.66% |
| Long Sequential | Get | 65.76 | 100.29 | -52.51% |

## Correctness Fix: Full Key Comparison in `get`

Fixed a correctness bug in the `get` method to perform a full key comparison. This is the new baseline for further optimizations.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 163.86 | 164.73 | -0.53% |
| Sequential | Get | 62.50 | 60.10 | +3.84% |
| Long Sequential | Insert | 326.11 | 547.97 | -68.03% |
| Long Sequential | Get | 100.29 | 65.35 | +34.84% |

## Optimization 3: Hoist `is_leaf` Check (Failed)

Hoisted the `is_leaf` check out of the main traversal loop in the `get` method. This resulted in a significant performance *degradation*.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 164.73 | 163.99 | +0.45% |
| Sequential | Get | 60.10 | 72.63 | -20.85% |
| Long Sequential | Insert | 547.97 | 250.61 | +54.27% |
| Long Sequential | Get | 65.35 | 79.48 | -21.62% |

## Optimization 4: ARM NEON for `find_first_differing_nibble` (Failed)

Completed the ARM NEON implementation for the `find_first_differing_nibble` function. This resulted in a significant performance *degradation*.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 163.99 | 317.22 | -93.44% |
| Sequential | Get | 72.63 | 96.20 | -32.45% |
| Long Sequential | Insert | 250.61 | N/A (Timeout) | N/A |
| Long Sequential | Get | 79.48 | N/A (Timeout) | N/A |

## Optimization 5: Use `fetch_add` in `StaxAllocator`

Replaced the `compare_exchange_weak` loop with a `fetch_add` operation in the `StaxAllocator`. This resulted in a significant performance improvement for long sequential keys, but a slight regression for shorter keys.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 317.22 | 171.88 | +45.81% |
| Sequential | Get | 96.20 | 70.85 | +26.35% |
| Long Sequential | Insert | N/A (Timeout) | 182.38 | N/A |
| Long Sequential | Get | N/A (Timeout) | 60.03 | N/A |

## Optimization 6: Refactor `insert` to Remove `goto`

Refactored the `insert` function to remove `goto` statements and added `_mm_pause()` to the CAS loops. This resulted in a performance improvement for sequential inserts, but a regression for long sequential inserts.

| Key Type | Operation | Baseline (ns) | After Change (ns) | Change (%) |
|---|---|---|---|---|
| Sequential | Insert | 171.88 | 158.42 | +7.83% |
| Sequential | Get | 70.85 | 58.35 | +17.64% |
| Long Sequential | Insert | 182.38 | 176.83 | +3.04% |
| Long Sequential | Get | 60.03 | 59.51 | +0.87% |
| Random | Insert | N/A (Timeout) | 233.32 | N/A |
| Random | Get | N/A (Timeout) | 92.98 | N/A |
