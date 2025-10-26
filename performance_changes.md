# Performance Changes

This document tracks the performance improvements made to the spatial query system.

## Baseline Performance (Before Optimization)

The following metrics were captured before any changes were made to the cursor or query logic. The current implementation uses a naive, full-scan approach within the query functions, which is expected to be inefficient.

| Workload          | Query Type | Latency (ns/op) | ns/item | Nodes Visited | Leaves Visited | Records Loaded | Records Scanned | Records Accepted | Efficiency |
|-------------------|------------|-----------------|---------|---------------|----------------|----------------|-----------------|------------------|------------|
| **2D Uniform**    | Box        | 294,837         | 210     | 1             | 2,551          | 2,547          | 2,547           | 1,401            | 0.550      |
|                   | Sphere     | 422,955         | 260     | 1             | 2,975          | 2,971          | 1,826           | 1,622            | 0.888      |
| **3D Uniform**    | Box        | 1,405,831       | 126     | 1             | 13,153         | 13,148         | 13,148          | 11,114           | 0.845      |
|                   | Sphere     | 2,394,555       | 213     | 1             | 15,697         | 15,692         | 13,658          | 11,225           | 0.822      |

---

## After High-Performance Cursor and AABB Pruning

*(Results to be filled in after optimizations are implemented.)*
