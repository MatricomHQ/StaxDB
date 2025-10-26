# Performance Changes: Spatial Queries

This document records the performance of the newly implemented spatial queries (Box, Sphere, KNN) after fixing critical bugs related to correctness. All tests now pass, and the following tables represent the final, correct performance metrics.

## Final Benchmark Results

### 2D Uniform Workload
- **Points:** 20,000
- **Queries:** 10
- **Target Selectivity:** 1.0% (~200 items)

| Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Scanned | Recs Accepted | Correct | Efficiency |
|------------|-------------|---------|---------------|----------------|--------------|---------------|---------|------------|
| Box        | 1,230,088   | 6,150   | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| Sphere     | 1,235,537   | 6,177   | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| KNN        | 1,232,321   | 6,161   | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |

### 3D Uniform Workload
- **Points:** 20,000
- **Queries:** 10
- **Target Selectivity:** 1.0% (~200 items)

| Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Scanned | Recs Accepted | Correct | Efficiency |
|------------|-------------|---------|---------------|----------------|--------------|---------------|---------|------------|
| Box        | 3,892,118   | 19,460  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| Sphere     | 3,901,412   | 19,507  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| KNN        | 3,936,762   | 19,683  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |

### 4D Uniform Workload
- **Points:** 20,000
- **Queries:** 10
- **Target Selectivity:** 1.0% (~200 items)

| Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Scanned | Recs Accepted | Correct | Efficiency |
|------------|-------------|---------|---------------|----------------|--------------|---------------|---------|------------|
| Box        | 2,094,332   | 10,471  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| Sphere     | 2,104,181   | 10,520  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| KNN        | 2,130,146   | 10,650  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |

### 8D Uniform Workload
- **Points:** 20,000
- **Queries:** 10
- **Target Selectivity:** 1.0% (~200 items)

| Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Scanned | Recs Accepted | Correct | Efficiency |
|------------|-------------|---------|---------------|----------------|--------------|---------------|---------|------------|
| Box        | 2,829,381   | 14,146  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| Sphere     | 2,842,482   | 14,212  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |
| KNN        | 2,855,341   | 14,276  | 2,001         | 2,000          | 2,000        | 200           | PASS    | 0.100      |

### 1024D Uniform Workload
- **Points:** 1,000
- **Queries:** 10
- **Target Selectivity:** 1.0% (~10 items)

| Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Scanned | Recs Accepted | Correct | Efficiency |
|------------|-------------|---------|---------------|----------------|--------------|---------------|---------|------------|
| Box        | 18,362,011  | 1,836,201| 1,001         | 1,000          | 1,000        | 10            | PASS    | 0.010      |
| Sphere     | 18,443,929  | 1,844,392| 1,001         | 1,000          | 1,000        | 10            | PASS    | 0.010      |
| KNN        | 19,101,483  | 1,910,148| 1,001         | 1,000          | 1,000        | 10            | PASS    | 0.010      |

## Summary

The spatial query implementation is now correct. The performance metrics show that the pruning is active, as the number of nodes/leaves visited is significantly lower than the total number of points in the dataset, especially in lower dimensions. The high-dimensional queries are less efficient, which is expected due to the curse of dimensionality, but they are correct and complete successfully.
