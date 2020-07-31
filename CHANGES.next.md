### Breaking changes:

### New features:

-   Add prefix/directory support for object storage service runs.
-   Add MaskRCNN and ReXtNet-101 to the horovod benchmark.

### Enhancements:

-   Added delay_time support for delete operations in object storage service.
-   Added horovod_synthetic option for synthetic input data in ResNet/ReXtNet models.
-   Added support for A100 GPUs.
-   Added cuda_tookit 11.0 support.
-   Updated `retry_on_rate_limited` to false on all cluster_boot runs.

### Bug fixes and maintenance updates:

-   Tuned entity cleanup parameters.
-   Fix wrong unit in MLPerf benchmark.
-   Disabled TF_CUDNN_USE_AUTOTUNE for horovod benchmarks.
-   Updated default NCCL version.
-   Use a deletion task to cleanup leftover entities to avoid hitting batch
    limit.
-   Updated object storage service deleteObjects API to also return back
    object_sizes of deleted objects.
-   Fixed a bug in leftover entity deletion logic.
-   Fixed string encoding bug in netperf_pps benchmark.
-   Fix installing RedHat EPEL on GCP (rhel8,centos8) and AWS (rhel7,rhel8,
    centos8, amazonlinux2)
-   Correctly install pip rhel8,centos8 all os types.
-   Fixed a bug in leftover entity deletion logic.
-   Use absl parameterized test case.

