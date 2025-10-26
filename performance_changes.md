# Performance Changes

This document tracks the performance changes made to the StaxDB dimensional indexing system.

## Baseline Performance

| Benchmark | Latency (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Efficiency |
|---|---|---|---|---|---|---|---|---|
| Box Query (2D, 1.00% selectivity) | 20438725 | | | | | | | |
| Sphere Query (2D, 1.00% selectivity) | 21386991 | | | | | | | |
| Box Query (3D, 1.00% selectivity) | | | | | | | | |
| Sphere Query (3D, 1.00% selectivity) | | | | | | | | |
| Box Query (4D, 1.00% selectivity) | | | | | | | | |
| Sphere Query (4D, 1.00% selectivity) | | | | | | | | |
| Box Query (8D, 1.00% selectivity) | | | | | | | | |
| Sphere Query (8D, 1.00% selectivity) | | | | | | | | |

## Performance After Changes

| Benchmark | Latency (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Efficiency |
|---|---|---|---|---|---|---|---|---|
| Box Query (2D, 1.000% selectivity) | 1140685 | 721 | 3467 | 2600 | 2600 | 2600 | 1580 | 0.608 |
| Sphere Query (2D, 1.000% selectivity) | 1051368 | 706 | 3467 | 2600 | 2600 | 1580 | 1488 | 0.942 |
| Box Query (3D, 1.000% selectivity) | 4015059 | 532 | 12301 | 9200 | 9200 | 9200 | 7544 | 0.820 |
| Sphere Query (3D, 1.000% selectivity) | 4021527 | 644 | 12301 | 9200 | 9200 | 7544 | 6235 | 0.826 |
| Box Query (4D, 1.000% selectivity) | 4474455 | 822 | 8563 | 6400 | 6400 | 6400 | 5442 | 0.850 |
| Sphere Query (4D, 1.000% selectivity) | 4802156 | 1205 | 8563 | 6400 | 6400 | 5442 | 3984 | 0.732 |
| Box Query (8D, 1.000% selectivity) | 910986 | 1159 | 1068 | 800 | 800 | 800 | 786 | 0.983 |
| Sphere Query (8D, 1.000% selectivity) | 1014960 | 5126 | 1068 | 800 | 800 | 786 | 198 | 0.252 |

## Percentage Change

| Benchmark | Latency |
|---|---|
| Box Query (2D, 1.00% selectivity) | -94.42% |
| Sphere Query (2D, 1.00% selectivity) | -95.08% |
