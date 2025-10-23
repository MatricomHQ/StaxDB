# Performance Changes

This document tracks the performance changes resulting from the refactoring of the `INSERT` operation.

## Baseline Performance

The following metrics were captured before any changes were made to the codebase.

| Benchmark                                    | StaxTree (ns/op) |
| -------------------------------------------- | ---------------- |
| Lexicographical 8-byte Insert (Random)       | 330              |
| Lexicographical 8-byte Get (Random)          | 107              |
| Avg. Insert Latency (Short Sequential Keys)  | 286              |
| Avg. Get (Hit) Latency (Short Sequential Keys) | 94               |

## Performance After Refactor

The following metrics were captured after refactoring the `InternalNode` structure and `insert` method.

| Benchmark                                    | Before (ns/op) | After (ns/op) | Improvement |
| -------------------------------------------- | -------------- | ------------- | ----------- |
| Lexicographical 8-byte Insert (Random)       | 330            | 287           | **13.03%**  |
| Lexicographical 8-byte Get (Random)          | 107            | 92            | **14.02%**  |
| Avg. Insert Latency (Short Sequential Keys)  | 286            | 271           | **5.24%**   |
| Avg. Get (Hit) Latency (Short Sequential Keys) | 94             | 95            | **-1.06%**  |
