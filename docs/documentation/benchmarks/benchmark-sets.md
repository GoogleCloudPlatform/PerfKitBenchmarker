# Benchmark Sets

## Standard Benchmark Set
---

### Running the Standard Benchmark Set
Run without the `--benchmarks` parameter and every benchmark in the standard set
will run serially which can take a couple of hours (alternatively, run with
`--benchmarks="standard_set"`).  Additionally, if you don't specify
`--cloud=...`, all benchmarks will run on the Google Cloud Platform.

## Named Benchmark Sets
---
Named sets are are groupings of one or more benchmarks in the benchmarking
directory. This feature allows parallel innovation of what is important to
measure in the Cloud, and is defined by the set owner. For example the GoogleSet
is maintained by Google, whereas the StanfordSet is managed by Stanford.

Once a quarter a meeting is held to review all the sets to determine what
benchmarks should be promoted to the `standard_set`. The Standard Set is also
reviewed to see if anything should be removed.

### Running a Named Benchmark Set
To run all benchmarks in a named set, specify the set name in the benchmarks
parameter (e.g., `--benchmarks="standard_set"`). Sets can be combined with
individual benchmarks or other named sets.
