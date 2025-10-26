# Performance Changes

## Spatial Query Benchmarks

### Before

| Workload | Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Efficiency |
|---|---|---|---|---|---|---|---|---|---|
| 2D Uniform | Box | 1315058 | 938 | 1 | 21224 | 2547 | 2547 | 1401 | 0.550 |
| 2D Uniform | Sphere | 1419010 | 874 | 1 | 21224 | 2971 | 1826 | 1622 | 0.888 |
| 3D Uniform | Box | 1762015 | 158 | 1 | 21200 | 13148 | 13148 | 11114 | 0.845 |
| 3D Uniform | Sphere | 2612052 | 232 | 1 | 21200 | 15692 | 13658 | 11225 | 0.822 |
| 4D Uniform | Box | 1727291 | 179 | 1 | 21273 | 10636 | 10636 | 9644 | 0.907 |
| 4D Uniform | Sphere | 3228804 | 234 | 1 | 21273 | 18082 | 17090 | 13754 | 0.805 |

### After

| Workload | Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Efficiency | Correctness |
|---|---|---|---|---|---|---|---|---|---|---|
| 2D Uniform | Box | 1292500 | 64 | 1 | 20000 | 20000 | 20000 | 20000 | 1.000 | 100/100 |
| 2D Uniform | Sphere | 1495280 | 74 | 1 | 20000 | 20000 | 20000 | 20000 | 1.000 | 100/100 |
| 3D Uniform | Box | 664179 | 62 | 1 | 10635 | 10635 | 10635 | 10635 | 1.000 | 100/100 |
| 3D Uniform | Sphere | 759886 | 71 | 1 | 10635 | 10635 | 10635 | 10635 | 1.000 | 100/100 |
| 4D Uniform | Box | 664606 | 62 | 1 | 10593 | 10593 | 10593 | 10593 | 1.000 | 100/100 |
| 4D Uniform | Sphere | 763493 | 72 | 1 | 10593 | 10593 | 10593 | 10593 | 1.000 | 100/100 |
