### Breaking changes:

-   The core_os os_type was removed from the Azure provider as
    [the image was deleted](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/endorsed-distros#supported-distributions-and-versions),
    -   It will be replaced by
        [Fedora Core OS](https://docs.fedoraproject.org/en-US/fedora-coreos/provisioning-azure/)
        if a public image is made avaiable.
-   The `dpb_sparksql_benchmark` now requires passing the requested queries with
    `--dpb_sparksql_query_order`

### New features:

-   Add prefix/directory support for object storage service runs.
-   Add MaskRCNN and ReXtNet-101 to the horovod benchmark.
-   Add Bigtable Benchmarking tutorial.
-   Add ycsb_skip_run_stage argument.
-   Move PKB static website files from gh-pages branch to master branch.
-   Add UDP benchmarking to iPerf.
-   Add ability to use Intel package repos.
-   Add Ubuntu 20.04 to AWS, Azure, and GCP providers.

### Enhancements:

-   Added delay_time support for delete operations in object storage service.
-   Added horovod_synthetic option for synthetic input data in ResNet/ReXtNet
    models.
-   Added support for A100 GPUs.
-   Added cuda_tookit 11.0 support.
-   Updated `retry_on_rate_limited` to false on all cluster_boot runs.
-   Add ability to apply HPC optimized script to GCE VMs with --gce_hpc_tools
-   Update the nginx benchmark.
-   Added `--before_run_pause` flag for debugging during benchmark development.
-   Report CPU vulnerabilities as a sample via the --record_cpu_vuln flag.
-   Add retry when calling MountDisk.
-   Measure time to running status of a VM in large_scale_boot benchmark.
-   SpecCPU2017 runs on Redhat, Centos7
-   Aerospike YCSB can have multiple server nodes and different client/server
    machine types.
-   HPL's configuration file customizable with flags.

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
-   Error out if AWS EFA install fails (centos8,rhel8)
-   Added `cascadelake` as a `--gcp_min_cpu_platform` option.
-   Move CoreOS from EOL Container Linux CoreOS to Fedora CoreOS on AWS and GCP.
-   Update files to use absl/flags.
-   Tries alternate command to get boot time, fixing continuous checking for new
    time on rebooted system.
-   Correctly handle ARM processors in netperf.
-   Added --always_teardown_on_exception to allow pkb to perform teardown when
    there is exception at the provision|prepare|run|cleanup stage.
-   Updates crcmod, boto, and awscli installation to pip3.

