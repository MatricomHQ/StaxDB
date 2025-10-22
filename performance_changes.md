# StaxDB Performance Optimizations

This document tracks the performance improvements made to StaxDB.

## Baseline Performance

The following table shows the baseline performance of StaxDB before any optimizations were applied.

| Metric                 | Avg Insert (ns) | Avg Get-Hit (ns) |
| ---------------------- | --------------- | ---------------- |
| **StaxDB (1 thread)**  | 727.803         | 82.358           |
| **StaxDB (64 threads)**| 166.876         | 74.694           |

## Optimization 1: Remove malloc/memcpy from stax_get

| Metric                 | Avg Insert (ns) | Avg Get-Hit (ns) | Improvement      |
| ---------------------- | --------------- | ---------------- | ---------------- |
| **StaxDB (1 thread)**  | 1138.485        | 81.506           | -56.4% (Insert), 1.03% (Get) |
| **StaxDB (64 threads)**| 164.179         | 59.780           | 1.6% (Insert), 20.0% (Get) |

## Optimization 2: Optimize insert function

| Metric                 | Avg Insert (ns) | Avg Get-Hit (ns) | Improvement      |
| ---------------------- | --------------- | ---------------- | ---------------- |
| **StaxDB (1 thread)**  | 933.570         | 91.485           | -28.3% (Insert), -11.1% (Get) |
| **StaxDB (64 threads)**| 162.229         | 58.088           | 2.8% (Insert), 22.2% (Get) |
