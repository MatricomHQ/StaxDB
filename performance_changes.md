# Performance Changes

This document tracks the performance changes resulting from the benchmark refactoring.

## Baseline Performance (Before)

| Benchmark                                    | StaxTree (ns/op) | std::map (ns/op) | std::unordered_map (ns/op) |
| -------------------------------------------- | ---------------- | ---------------- | -------------------------- |
| **Short Sequential Keys (8 bytes)**          |                  |                  |                            |
| Avg. Insert Latency                          | 230              | 1159             | 669                        |
| Avg. Get (Hit) Latency                       | 85               | 1172             | 169                        |
| Avg. Get (Miss) Latency                      | 20               | 129              | 222                        |
| **Long Sequential Keys (128 bytes)**         |                  |                  |                            |
| Avg. Insert Latency                          | 1297             | 2269             | 1363                       |
| Avg. Get (Hit) Latency                       | 299              | 1677             | 415                        |
| Avg. Get (Miss) Latency                      | 39               | 116              | 400                        |
| **Hash-like Keys (64 bytes)**                |                  |                  |                            |
| Avg. Insert Latency                          | 1709             | 1670             | 1291                       |
| Avg. Get (Hit) Latency                       | 286              | 1448             | 371                        |
| Avg. Get (Miss) Latency                      | 23               | 121              | 413                        |

## Optimized Performance (After)

| Benchmark                                    | StaxTree (ns/op) | std::map (ns/op) | std::unordered_map (ns/op) |
| -------------------------------------------- | ---------------- | ---------------- | -------------------------- |
| **Short Sequential Keys (8 bytes)**          |                  |                  |                            |
| Avg. Insert Latency                          | 296              | 1246             | 750                        |
| Avg. Get (Hit) Latency                       | 125              | 1213             | 209                        |
| Avg. Get (Miss) Latency                      | 18               | 125              | 222                        |
| **Long Sequential Keys (128 bytes)**         |                  |                  |                            |
| Avg. Insert Latency                          | 781              | 1807             | 772                        |
| Avg. Get (Hit) Latency                       | 250              | 1716             | 371                        |
| Avg. Get (Miss) Latency                      | 30               | 102              | 377                        |
| **Hash-like Keys (64 bytes)**                |                  |                  |                            |
| Avg. Insert Latency                          | 591              | 1504             | 1167                       |
| Avg. Get (Hit) Latency                       | 263              | 1480             | 324                        |
| Avg. Get (Miss) Latency                      | 19               | 103              | 345                        |

## Performance Comparison (%)

| Benchmark                                    | StaxTree Change | std::map Change | std::unordered_map Change |
| -------------------------------------------- | --------------- | --------------- | ------------------------- |
| **Short Sequential Keys (8 bytes)**          |                 |                 |                           |
| Avg. Insert Latency                          | -28.7%          | -7.5%           | -12.1%                    |
| Avg. Get (Hit) Latency                       | -47.1%          | -3.3%           | -23.7%                    |
| Avg. Get (Miss) Latency                      | +10.0%          | +3.1%           | 0.0%                      |
| **Long Sequential Keys (128 bytes)**         |                 |                 |                           |
| Avg. Insert Latency                          | +39.8%          | +20.4%          | +43.4%                    |
| Avg. Get (Hit) Latency                       | +16.4%          | -2.3%           | +10.6%                    |
| Avg. Get (Miss) Latency                      | +23.1%          | +12.1%          | +5.8%                     |
| **Hash-like Keys (64 bytes)**                |                 |                 |                           |
| Avg. Insert Latency                          | +65.4%          | +9.9%           | +9.6%                     |
| Avg. Get (Hit) Latency                       | +8.0%           | -2.2%           | +12.7%                    |
| Avg. Get (Miss) Latency                      | +17.4%          | +14.9%          | +16.5%                    |
