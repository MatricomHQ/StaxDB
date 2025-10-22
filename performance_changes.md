# Performance Changes

## Baseline

| Key Type | Operation | Time (ns/op) |
|---|---|---|
| Short Sequential (8 bytes) | Insert | 747 |
| Short Sequential (8 bytes) | Get (Hit) | 105 |
| Long Sequential (128 bytes) | Insert | 2274 |
| Long Sequential (128 bytes) | Get (Hit) | 435 |
| Hash-like (64 bytes) | Insert | 1891 |
| Hash-like (64 bytes) | Get (Hit) | 287 |

## After `get` Optimization (No-Copy `get`)

| Key Type | Operation | Baseline (ns/op) | New (ns/op) | Improvement |
|---|---|---|---|---|
| Short Sequential (8 bytes) | Insert | 747 | 499 | +33.2% |
| Short Sequential (8 bytes) | Get (Hit) | 105 | 115 | -9.5% |
| Long Sequential (128 bytes) | Insert | 2274 | 1183 | +48.0% |
| Long Sequential (128 bytes) | Get (Hit) | 435 | 287 | +34.0% |
| Hash-like (64 bytes) | Insert | 1891 | 780 | +58.7% |
| Hash-like (64 bytes) | Get (Hit) | 287 | 278 | +3.1% |

## After `insert` Optimization (Failed)

| Key Type | Operation | Previous (ns/op) | New (ns/op) | Improvement |
|---|---|---|---|---|
| Short Sequential (8 bytes) | Insert | 499 | 782 | -56.7% |
| Short Sequential (8 bytes) | Get (Hit) | 115 | 112 | +2.6% |
| Long Sequential (128 bytes) | Insert | 1183 | 2337 | -97.5% |
| Long Sequential (128 bytes) | Get (Hit) | 287 | 287 | 0% |
| Hash-like (64 bytes) | Insert | 780 | 1815 | -132.7% |
| Hash-like (64 bytes) | Get (Hit) | 278 | 271 | +2.5% |
