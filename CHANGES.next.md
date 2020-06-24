### Breaking changes:

### New features:

-   Add FairSeq Roberta Masked Multilingual LM benchmark for linux.
-   Add bulk delete API operation to object storage API scripts.
-   Catch preemptible instance's interrupt event.

### Enhancements:

-   Enable support for AWS's c6g and r6g families.
-   Integrate edw_benchmark with new Client Interface and results aggregation
    framework.

### Bug fixes and maintenance updates:

-   Parallelize entity deletion processing for a kind.
-   Move coremark installation into a package.
-   Fixed one object per stream run for object storage service benchmark.
-   Benchmarking of AWS Athena now records client observed run time rather that
    Athena's reported run time.
