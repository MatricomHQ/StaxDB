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

## After Arena Size Increase (64KB)

| Key Type | Operation | Previous (ns/op) | New (ns/op) | Improvement |
|---|---|---|---|---|
| Short Sequential (8 bytes) | Insert | 782 | 750 | +4.1% |
| Short Sequential (8 bytes) | Get (Hit) | 112 | 94 | +16.1% |
| Long Sequential (128 bytes) | Insert | 2337 | 2536 | -8.5% |
| Long Sequential (128 bytes) | Get (Hit) | 287 | 250 | +12.9% |
| Hash-like (64 bytes) | Insert | 1815 | 1902 | -4.8% |
| Hash-like (64 bytes) | Get (Hit) | 271 | 288 | -6.3% |

## After SIMD `memcpy` Optimization

| Key Type | Operation | Previous (ns/op) | New (ns/op) | Improvement |
|---|---|---|---|---|
| Short Sequential (8 bytes) | Insert | 750 | 287 | +61.7% |
| Short Sequential (8 bytes) | Get (Hit) | 94 | 88 | +6.4% |
| Long Sequential (128 bytes) | Insert | 2536 | 872 | +65.6% |
| Long Sequential (128 bytes) | Get (Hit) | 250 | 250 | 0% |
| Hash-like (64 bytes) | Insert | 1902 | 724 | +62.0% |
| Hash-like (64 bytes) | Get (Hit) | 288 | 272 | +5.6% |

## After `find_first_differing_nibble` Optimization (Failed)

| Key Type | Operation | Previous (ns/op) | New (ns/op) | Improvement |
|---|---|---|---|---|
| Short Sequential (8 bytes) | Insert | 287 | 809 | -181.9% |
| Short Sequential (8 bytes) | Get (Hit) | 88 | 106 | -20.5% |
| Long Sequential (128 bytes) | Insert | 872 | 2923 | -235.2% |
| Long Sequential (128 bytes) | Get (Hit) | 250 | 350 | -40.0% |
| Hash-like (64 bytes) | Insert | 724 | 1930 | -166.6% |
| Hash-like (64 bytes) | Get (Hit) | 272 | 300 | -10.3% |
