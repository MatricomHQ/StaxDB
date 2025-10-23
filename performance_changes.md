# Performance Changes

This document tracks the performance changes resulting from the refactoring of the `INSERT` function.

## Baseline

| Benchmark                                    | StaxTree (ns/op) |
| -------------------------------------------- | ---------------- |
| Lexicographical 3-byte Insert (Random)       | 95               |
| Lexicographical 4-byte Insert (Random)       | 84               |
| Lexicographical 7-byte Insert (Random)       | 146              |
| Lexicographical 8-byte Insert (Random)       | 347              |
| Lexicographical 16-byte Insert (Random)      | 348              |
| Lexicographical 17-byte Insert (Random)      | 349              |
| Lexicographical 32-byte Insert (Random)      | 405              |
| Lexicographical 35-byte Insert (Random)      | 405              |
| Short Sequential Keys (8 bytes) Insert       | 317              |
| Long Sequential Keys (128 bytes) Insert      | 3457             |
| Hash-like Keys (64 bytes) Insert             | 2620             |

## Refactor 1: Inline Key in Node

| Benchmark                                    | StaxTree (ns/op) | % Change |
| -------------------------------------------- | ---------------- | -------- |
| Lexicographical 3-byte Insert (Random)       | 85               | +10.5%   |
| Lexicographical 4-byte Insert (Random)       | 84               | 0%       |
| Lexicographical 7-byte Insert (Random)       | 139              | +4.8%    |
| Lexicographical 8-byte Insert (Random)       | 331              | +4.6%    |
| Lexicographical 16-byte Insert (Random)      | 353              | -1.4%    |
| Lexicographical 17-byte Insert (Random)      | 359              | -2.8%    |
| Lexicographical 32-byte Insert (Random)      | 386              | +4.7%    |
| Lexicographical 35-byte Insert (Random)      | 392              | +3.2%    |
| Short Sequential Keys (8 bytes) Insert       | 313              | +1.3%    |
| Long Sequential Keys (128 bytes) Insert      | 2557             | +26.0%   |
| Hash-like Keys (64 bytes) Insert             | 2536             | +3.2%    |
