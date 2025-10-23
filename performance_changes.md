# Performance Changes

This document tracks the performance changes related to the unification of the `InternalNode` struct.

## InternalNode Unification

| Benchmark                                    | Before | After | Change |
|----------------------------------------------|--------|-------|--------|
| Lexicographical 3-byte Insert (Random)       | -      | 83    | -      |
| Lexicographical 3-byte Get (Random)          | -      | 3     | -      |
| Lexicographical 4-byte Insert (Random)       | -      | 74    | -      |
| Lexicographical 4-byte Get (Random)          | -      | 2     | -      |
| Lexicographical 7-byte Insert (Random)       | -      | 120   | -      |
| Lexicographical 7-byte Get (Random)          | -      | 10    | -      |
| Lexicographical 8-byte Insert (Random)       | -      | 240   | -      |
| Lexicographical 8-byte Get (Random)          | -      | 53    | -      |
| Lexicographical 16-byte Insert (Random)      | -      | 251   | -      |
| Lexicographical 16-byte Get (Random)         | -      | 53    | -      |
| Lexicographical 17-byte Insert (Random)      | -      | 265   | -      |
| Lexicographical 17-byte Get (Random)         | -      | 60    | -      |
| Lexicographical 32-byte Insert (Random)      | -      | 287   | -      |
| Lexicographical 32-byte Get (Random)         | -      | 56    | -      |
| Lexicographical 35-byte Insert (Random)      | -      | 279   | -      |
| Lexicographical 35-byte Get (Random)         | -      | 58    | -      |
| Small Scans (10 items)                       | -      | 1573  | -      |
| Medium Scans (100 items)                     | -      | 6811  | -      |
| Large Scans (1000 items)                     | -      | 58793 | -      |
| Avg. Insert Latency (Short Keys)             | -      | 270   | -      |
| Avg. Get (Hit) Latency (Short Keys)          | -      | 103   | -      |
| Avg. Get (Miss) Latency (Short Keys)         | -      | 20    | -      |
| Avg. Insert Latency (Long Keys)              | -      | 1407  | -      |
| Avg. Get (Hit) Latency (Long Keys)           | -      | 541   | -      |
| Avg. Get (Miss) Latency (Long Keys)          | -      | 56    | -      |
| Avg. Insert Latency (Hash-like Keys)         | -      | 2049  | -      |
| Avg. Get (Hit) Latency (Hash-like Keys)      | -      | 334   | -      |
| Avg. Get (Miss) Latency (Hash-like Keys)     | -      | 23    | -      |
