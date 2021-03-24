### Breaking changes:

-   The core_os os_type was removed from the Azure provider as
    [the image was deleted](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/endorsed-distros#supported-distributions-and-versions),
    -   It will be replaced by
        [Fedora Core OS](https://docs.fedoraproject.org/en-US/fedora-coreos/provisioning-azure/)
        if a public image is made avaiable.
-   The `dpb_sparksql_benchmark` now requires passing the requested queries with
    `--dpb_sparksql_query_order`
-   AwsVirtualMachine.IMAGE_OWNER has been changed from a string to a list of
    strings to support images which have multiple owners e.g. AmazonLinux2 in
    opt-in regions.
-   Remove Ubuntu1710 from `--os_types`.
-   Remove Amazon Linux 1 from `--os_types`.

### New features:

-   Add ibmcloud as a new provider.
-   Add prefix/directory support for object storage service runs.
-   Add MaskRCNN and ReXtNet-101 to the horovod benchmark.
-   Add Bigtable Benchmarking tutorial.
-   Add ycsb_skip_run_stage argument.
-   Move PKB static website files from gh-pages branch to master branch.
-   Add UDP benchmarking to iPerf.
-   Add ability to use Intel package repos.
-   Add Ubuntu 20.04 to AWS, Azure, and GCP providers.
-   Add support for running DPB Apache Spark benchmarks on PKB provisioned VMs
-   Add support for AWS gp3 disks.
-   Add ability to use Intel compiled HPCC binaries with
    --hpcc_use_intel_compiled_hpl
-   Add OSU MPI micro-benchmarks benchmark
-   Add support for setting virtual NIC type for GCP VMs.
-   Add ability to collect /proc/meminfo data with --collect_meminfo
-   Add support for setting egress bandwidth tier for GCP VMs.
-   Add MLPerf multiworker benchmark based on NVIDIA's v0.6 submission

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
-   Added Intel MPI to linux_packages.
-   Added ability to use MKL 2018.2 from Intel repos via --mkl_install_from_repo
-   Added ability to install gfortran 9 with --fortran_version=9
-   Added MSS support to netperf with `--netperf_mss`.
-   Added nfs_service.NfsExport(vm, path) to easily NFS export a directory.
-   AWS EFA works for Ubuntu1604.
-   Added support for MySQL 8.0 on VMs and minimal innodb tuning.
-   Add ability to specify version of Intel MKL with --mkl_version
-   Added intelmpi.NfsExportIntelDirectory to NFS export /opt/intel
-   Modify cloud_datastore_ycsb benchmark to execute YCSB on the same db entries
    each run instead of emptying and preloading the db every time. Can set flag
    google_datastore_repopulate=True to empty & repopulate.
-   Enhance the cpu metric collection feature for cloud bigtable benchmark: as
    long as there is a real workload (table loading is not counted), the cpu
    metrics will be collected.
-   Support wiring properties into DPB clusters with `--dpb_clusters_properties`
    in addition to `--dpb_job_properties`.
-   Add support for GCS and S3 I/O in PKB managed Spark and Hadoop clusters.

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
-   Consolidates adding Ubuntu toolchain repo.
-   Moved stress_ng installation to a package.
-   Limit pip to v20.2.2 for ubuntu1604 and for
    object_storage_service_benchmark.
-   Switch to using Google Cloud Build for continuous integration.
-   Fix PrettyPrintStreamPublisher to make "cpu_utilization_per_minute" show up
    in the PKB results summary for cloud bigtable benchmark.
