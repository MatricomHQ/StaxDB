# StaxDB Dimensional System Performance

This document tracks the performance of the new dimensional indexing system as features are implemented and optimized.

## Initial Benchmark: `StaxCursor::seek_first()`

This test measures the average time it takes to create a cursor and seek to a random, existing key within a tree containing 100,000 items.

| Metric                        | Time (ns/op) |
| ----------------------------- | ------------ |
| Avg. `seek_first` Latency     | 669          |

## Query Benchmarks (D=3, 100,000 items)

These benchmarks measure the average latency of the dimensional query functions.

### Before Pruning Optimization

| Metric                        | Time (ns/op) |
| ----------------------------- | ------------ |
| Avg. `query_box` Latency      | 7,681,162    |
| Avg. `query_sphere` Latency   | 8,118,046    |
| Avg. `query_knn` Latency (k=10) | 13,947       |

### After Pruning Optimization

| Metric                        | Time (ns/op) | Improvement |
| ----------------------------- | ------------ | ----------- |
| Avg. `query_box` Latency      | 681          | > 11,000x   |
| Avg. `query_sphere` Latency   | 698          | > 11,000x   |
| Avg. `query_knn` Latency (k=10) | 13,734       | -           |

**Note:** The dramatic improvement in `query_box` and `query_sphere` is due to the implementation of the AABB pruning logic, which now efficiently skips irrelevant branches of the tree. The `query_knn` performance remains consistent as its pruning logic was already correctly implemented.
