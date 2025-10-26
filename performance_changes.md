# Performance Changes

## Fourth Attempt: `get` Method Optimization

### Short Sequential Keys (8 bytes)

| Metric | Before | After | Improvement |
|---|---|---|---|
| Avg. Insert Latency | 337 ns/op | 268 ns/op | 20.5% |
| Avg. Get (Hit) Latency | 94 ns/op | 82 ns/op | 12.8% |
| Avg. Get (Miss) Latency | 25 ns/op | 25 ns/op | 0% |

### Hash-like Keys (64 bytes)

| Metric | Before | After | Improvement |
|---|---|---|---|
| Avg. Insert Latency | 1651 ns/op | 887 ns/op | 46.3% |
| Avg. Get (Hit) Latency | 263 ns/op | 255 ns/op | 3.0% |
| Avg. Get (Miss) Latency | 29 ns/op | 30 ns/op | -3.4% |
