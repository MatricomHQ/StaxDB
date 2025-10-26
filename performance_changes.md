# Performance Changes

## Baseline Performance

The following table shows the initial performance metrics before any changes were made to the codebase.

| Workload          | Query Type | Lat (ns/op) | ns/item | Nodes Visited | Leaves Visited | Recs Loaded | Recs Scanned | Recs Accepted | Efficiency |
|-------------------|------------|-------------|---------|---------------|----------------|-------------|--------------|---------------|------------|
| 2D Uniform        | Box        | 269542      | 192     | 1             | 2551           | 2547        | 2547         | 1401          | 0.550      |
| 2D Uniform        | Sphere     | 414903      | 255     | 1             | 2975           | 2971        | 1826         | 1622          | 0.888      |
| 2D Uniform        | KNN        | 99998       | 499     | 369           | 502            | 500         | 500          | 200           | 0.400      |
| 3D Uniform        | Box        | 1375349     | 123     | 1             | 13153          | 13148       | 13148        | 11114         | 0.845      |
| 3D Uniform        | Sphere     | 2354562     | 209     | 1             | 15697          | 15692       | 13658        | 11225         | 0.822      |
| 3D Uniform        | KNN        | 92151       | 460     | 295           | 416            | 414         | 414          | 200           | 0.483      |
| 4D Uniform        | Box        | 1143843     | 118     | 1             | 10638          | 10636       | 10636        | 9644          | 0.907      |
| 4D Uniform        | Sphere     | 3060190     | 222     | 1             | 18083          | 18082       | 17090        | 13754         | 0.805      |
| 4D Uniform        | KNN        | 87334       | 436     | 254           | 365            | 363         | 363          | 200           | 0.551      |
| 8D Uniform        | Box        | 38443       | 202     | 1             | 215            | 212         | 212          | 190           | 0.896      |
| 8D Uniform        | Sphere     | 5046140     | 282     | 1             | 21297          | 21297       | 21274        | 17856         | 0.839      |
| 8D Uniform        | KNN        | 88104       | 440     | 165           | 273            | 271         | 271          | 200           | 0.738      |
| 1024D Uniform     | Box        | 3515        | 0       | 1             | 2              | 0           | 0            | 0             | 0.000      |
| 1024D Uniform     | Sphere     | 338705539   | 1782660 | 1             | 21298          | 21298       | 21298        | 190           | 0.009      |
| 1024D Uniform     | KNN        | 334396654   | 1671983 | 20614         | 21965          | 21965       | 21965        | 200           | 0.009      |
