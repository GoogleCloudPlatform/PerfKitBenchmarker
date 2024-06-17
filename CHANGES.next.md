### Breaking changes:

-   Added --accept_licenses flag. User have to turn this flag on to acknowledge
    that PKB may install software thereby accepting license agreements on the
    user's behalf.
-   Renamed Database-related flags from managed_db* to db* Added alias for
    backwards compatibility, might not be supported in the future release.
-   Require Python 3.9+
-   The core_os os_type was removed from the Azure provider as
    [the image was deleted](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/endorsed-distros#supported-distributions-and-versions),
    -   It will be replaced by
        [Fedora Core OS](https://docs.fedoraproject.org/en-US/fedora-coreos/provisioning-azure/)
        if a public image is made available.
-   The `dpb_sparksql_benchmark` now requires passing the requested queries with
    `--dpb_sparksql_query_order`
-   AwsVirtualMachine.IMAGE_OWNER has been changed from a string to a list of
    strings to support images which have multiple owners e.g. AmazonLinux2 in
    opt-in regions.
-   Remove Ubuntu1710 from `--os_types`.
-   Remove Amazon Linux 1 from `--os_types`.
-   Changed redis_memtier_benchmark to use redis version 6 and above. Redis
    versions less than 6 are no longer supported.
-   Compressed redis_memtier samples from `--memtier_time_series` into a few
    time-to-value dictionaries, greatly reducing the number of samples produced
-   Make Ubuntu 18 the default os_type.
-   Deprecate Ubuntu 16 as it is EOL on 2021-05-01.
-   Switch to Azure CLI to MSAL. This requires updating the CLI to >= 2.30.0.
    -   See https://docs.microsoft.com/en-us/cli/azure/msal-based-azure-cli
-   Remove deprecated `--eks_zones` flags. Use `--zones` instead.
-   Deprecate CentOS Linux 8 as it is EOL on 2021-12-31.
-   `--zones` and `--extra_zones` deprecated in favor of `--zone`.
-   Deprecate Aerospike_YCSB benchmark.
-   Remove pkb's --placement_group_style cloud-agnostic values 'cluster'/
    'cluster_if_supported'/'spread'/'spread_if_supported'.
-   Replace flag --ibm_azone with --ibm_region.
-   Changed the default benchmark to `cluster_boot` instead of the standard set.
    This makes the default behavior for PKB much faster and the standard set of
    benchmarks was defined many years ago. It's not a reasonable introduction to
    PKB or something that most people should run by default.
-   --dpb_export_job_stats is now False by default.
-   Validate arguments to IssueCommand & RobustRemoteCommand. Replaced
    force_info_log & suppress_warning parameters with vm_command_log_mode flag,
    added should_pre_log parameter. Passed stacklevel variable to logging to
    better distinguish between RemoteCommand call sites. See stacklevel docs:
    https://docs.python.org/3/library/logging.html#logging.Logger.debug
-   Remove Dataflow parameter --maxNumWorkers by default and add
    dataflow_max_worker_count in spec to allow users to set this parameter on
    their own.
-   Remove flag fio_write_against_multiple_clients from FIO.
-   Remove flag benchmark_compatibility_checking.
-   Drop windows coremark benchmark.
-   Remove cudnn linux package.
-   Make Ubuntu 20 the default os_type.
-   Default `--ip_addresses` to `INTERNAL`.
-   Add multichase_benchmark flag defaults.
-   For hammerdbcli_benchmark, set default num_warehouses=25*num_cpus and
    num_vu=2*num_cpus.
-   Remove old `spark_benchmark` and `hadoop_terasort_benchmark` and related
    services. Prefer to use the newer `dpb_generic_benchmark` and
    `dpb_terasort_benchmark`.
-   gcp/aws/azure_provisioned_iops/throughput flags are unified to
    provisioned_iops/throughput flags.
-   aws_dynamodb_ycsb benchmark now requires an explicit
    `--aws_dynamodb_ycsb_cli_profile` flag to select the credentials to talk to
    YCSB.
-   Benchmarks that require EC2 client VMs now require
    `--aws_ec2_instance_profile` to configure VM permissions instead of
    installing local AWS CLI credentials via aws_credentials.py. This must be
    set up by the user beforehand. See
    [AWS documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html)
    - Alternatively for EKS benchmarking, clusters require
    --aws_eks_pod_identity_role for Kubernetes VMs inside the cluster calling
    APIs. The role must be set up by the user beforehand. See
    [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-association.html)
-   Split `--azure_preprovisioned_data_bucket` into
    `--azure_preprovisioned_data_account` and
    `--azure_preprovisioned_data_subscription`, which allows cross-subscription
    access.
-   Remove Rocky Linux on Azure.
-   Changed supported Python version to 3.11.

### New features:

-   Add support for systems running fedora36 and fedora37
-   Add support for AlloyDB on GCP
-   Add support for static systems running debian11
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
-   Add support for Azure Ultra Disk
-   Add `cloudharmony_network` benchmark
-   Add GPU PingPong benchmark.
-   Added lmod linux package.
-   Support IntelMPI 2021.x on RedHat.
-   Add TensorFlow BigQuery Connector benchmark.
-   Add gcsfuse benchmark.
-   Add MLPerf multiworker benchmark based on NVIDIA's v0.6 submission
-   Add retries to PKB.
-   Added GCE networking MTU support with --mtu.
-   Add pd extreme support to PKB.
-   Add '--delete_samples' to measure VM deletion during benchmark teardown
    phase
-   Add cloudharmony iperf benchmark to pkb.
-   Add specjbb2015 benchmark to PKB.
-   Add VM stop start benchmark.
-   Add suspend_resume benchmark.
-   Add AWS support for VM stop start benchmark.
-   Add Azure support for VM stop start benchmark.
-   Add `--os_type=debian11` support for GCP, AWS and Azure Providers.
-   Add cURL benchmark for object storage.
-   Add vbench video encoding benchmark to PKB.
-   Add Kubernetes based DPB Service for Spark
-   Add support for creating Dataproc cluster on GKE
-   Add support for TPC-DS/H benchmarks on Dataproc Serverless.
-   Add support for TPC-DS/H benchmarks on AWS Glue Job.
-   Add messaging service latency benchmark (for GCP PubSub, AWS SQS & Azure
    Service Bus).
-   Add Intel perfspect as a new trace
-   Add Ubuntu 22.04 support for GCP, AWS, and Azure Providers.
-   Add support for Rocky Linux 8 and 9 on GCP, AWS, and Azure Providers.
-   Add support for CentOS Stream 8, CentOS Stream 9 on GCP and AWS Providers.
-   Add support for chbench using s64da.
-   Add sysbench_memory benchmark.
-   Add support for RHEL 9 on AWS, Azure, and GCP.
-   Add GCP optimized Rocky Linux 8 and 9 OSes.
-   Add mtu to os_metadata in linux_virtual_machine.
-   Add support for TPC-DS/H benchmarks on AWS EMR Serverless.
-   Add Intel MPI benchmark.
-   Add support for Azure ARM VMs.
-   Add an HTTP endpoint polling utility & incorporate it into app_service.
-   Added support for dynamic provisioning of Bigquery flat rate slots at
    benchmark runtime
-   Create a new subdirectory of linux_benchmarks called provisioning_benchmarks
    for benchmarking lifecycle management timings of cloud resources. Including:
    -   Kubernetes Clusters
    -   KMS cryptographic keys
    -   Object storage buckets
-   Add support for using the hbase2 binding in the Cloud Bigtable YCSB
    benchmark.
-   Add iPerf interval reporting.
-   Add support for DynamoDB on demand instances.
-   Add support for Debian 10 & 11 with backported kernels on AWS.
-   Add fio_netperf benchmark, which executes the run stages of fio and netperf
    benchmarks in parallel using the first 2 VM's in benchmark_spec.
-   Add Google Kubernetes Engine based DPB service to run flink benchmarks.
-   Add support for Amazon Linux 2023.
-   Add support for multi-network creation/attachment. PKB currently does not
    handle subnet creation on an existing network.
-   Add support for GCE Confidential VM's.
-   Add cos-dev, cost109, cos105, and cos101 OS support for GCP.
-   Add --object_ttl_days flag for lifecycle management of created buckets.
-   Add support for multi-NIC netperf throughput on AWS.
-   Added AWS/GCP support for Data Plane Development Kit (DPDK) on Linux VM's to
    improve networking performance, as well as a DPDK benchmark for testpmd.
-   Add --dpb_hardware_hourly_cost and --dpb_service_premium_hourly_cost to
-   Add support for Spanner Postgres benchmarking with TPCC. estimate cost of
    DPB service in benchmark runs.
-   Add --dpb_dynamic_allocation flag to disable dynamic allocation in Spark
    benchmarks.
-   Add support for benchmarking VMs with Local SSD on Windows on GCE/AWS/Azure.
-   Add support for Debian 12.
-   Add netperf_hammerdbcli benchmark, which executes netperf and hammerdbcli
    benchmarks in parallel.
-   Add default_benchmark_config.yaml and merge it with user_config. This is
    done before config overrides. Within default_benchmark_config.yaml, add
    configs for netperf_tcp_rr, netperf_tcp_stream, and hammerdbcli_mysql.
-   Add support for Ubuntu 23.10.
-   HammerDB enables "Use All Warehouses" by default for increased I/O.
-   Add Ubuntu 24.04 support for GCP, AWS, and Azure Providers.
-   Add keydb_memtier_benchmark (KeyDB is a fork of Redis).

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
-   Update FIO workload to support extraction of common benchmark parameters
    from the scenario string.
-   Added Intel oneAPI BaseKit to packages.
-   Upgrade default CUDA version to 11.0.
-   Add support for AWS IO2 EBS instances.
-   Add support for Postgres 13 on VMs.
-   Add support to compile GCC versions on non Debian systems.
-   Add additional customization options to SPEC CPU 2017.
-   Add average utilization to the metadata of cpu metrics for Cloud Bigtable
    benchmark.
-   Add support for NFS `nconnect` mount option.
-   Add support for custom compilation of OpenJDK.
-   Add support for configuring the HBase binding to use with `--hbase_binding`.
-   Enhance the cpu utilization metrics for cloud bigtable ycsb benchmark:
    collect the cpu data for each workload (differentiated by workload_index);
    the time window estimation is also more accurate.
-   Support default subnets in --aws_subnet with `default` as value passed.
-   Add Unsupported config failure substatus for runs that are not supported by
    the cloud.
-   Add support for nodepools to `container_cluster` benchmark spec.
-   Expose GCS FUSE disk type to allow using GCS buckets as a data_disk.
-   Add support for 5th gen Azure VMs.
-   Support multiple Redis instances on the same VM and multiple client VMs.
-   Support creating autoscaled Bigtable instances.
-   Added support to allow deleting a static table in Cloud Bigtable benchmarks
    via --google_bigtable_delete_static_table.
-   Support downloading data twice in object_storage_service_benchmark.
-   Add memtier reported percentile latencies to Memtier samples metadata.
-   Add support for building multiarch Docker images.
-   Add latency capped throughput measurement mode to memtier.
-   Add Unsupported config failure substatus for Azure runs.
-   Add support for Windows 2022 and Sql server 2019 on Windows 2022
-   Add support for Redis Enterprise clustered database.
-   Support regional GKE clusters.
-   Support zonal node placement in regional Kubernetes clusters.
-   Uses the regular version of gcloud for Bigtable commands instead of beta.
-   Support logging the start timestamp of each stage.
-   Support for building GCC on Debian
-   Support for Postgres 13 on Debian
-   Add OSProvisioningTimedOut as a recognized failure mode in Azure.
-   Add support for providing initialization actions into DPB clusters with
    `--dpb_initialization_actions`
-   Add sandbox configuration fields for GKE nodepools.
-   Fetch Redis benchmark live migration times from GCP metadata server.
-   Retry table deletions in Cloud Bigtable benchmarks against a user managed
    instance and fail if the benchmark eventually fails to delete a table.
-   Add support for Snowflake on Azure External Tables
-   Fetch memtier benchmark runtime information
-   Support installing the Google Cloud Bigtable client by a given version via
    --google_bigtable_client_version and simplify dependency management.
-   Support setting --dpb_dataflow_additional_args and --dpb_dataflow_timeout
    for dpb_dataflow_provider.
-   Add support for T2A (ARM) VMs on GCE.
-   Add `--dpb_job_poll_interval_secs` flag to control job polling frequency in
    DPB benchmarks.
-   Add support for more readings in nvidia_power tracking.
-   Report benchmark run costs for dpb_sparksql_benchmark runs on Dataproc
    Serverless, AWS EMR Serverless & AWS Glue.
-   Create a list of resources in benchmark_spec to extract common lifecycle
    timing samples from regardless of benchmark. The set is initially small, but
    can be expanded to any resource.
-   Add per-VM resource metadata for id, name, and IP address.
-   Add Key Management Service (KMS) resource for cloud cryptographic keys.
-   Add support for using java veneer client with google bigtable
    `google_bigtable_use_java_veneer_client`.
-   Allow configuring the number of channels used per VM for the Cloud Bigtable
    YCSB benchmark with `--google_bigtable_channel_count`.
-   Add `--pkb_log_bucket` flag, allowing users to route PKB logs to a GCS
    bucket and clean up space on their machines.
-   Add support for rls routing with direct path with new flag
    `google_bigtable_enable_rls_routing`.
-   Set default YAML config vm_spec.GCP_network_name to null, and added the
    corresponding attribute to GCEVMSpec, GCENetworkSpec and GCEVirtualMachine.
    vm_spec overrides FLAGS.gce_network_name.
-   Add `--dpb_sparksql_queries_url` flag to provide custom object store path
    (i.e. GCS/S3) where the queries will be used for `dpb_sparksql_benchmark`.
-   Add `--gke_node_system_config` flag to the GKE provider for passing kubelet
    and linux parameters.
-   Add 'Time to Create' and 'Time to Running' samples on cluster_boot for
    GCEVirtualMachine and AWSVirtualMachine instances that are provisioned with
    asynchronous 'create' invocations.
-   Add `--dpb_sparksql_streams` to run TPC-DS/H throughput runs.
-   Add `--gce_create_log_http` to pass `--log-http` to `gcloud compute instance
    create` and `gcloud compute operations describe`.
-   Update AWS/Azure/GCP data disks to use cheap ssds rather than hdds.
-   Support Azure ZRS disks and hyperdisk balanced.
-   Add --create_container_cluster_time_resize option to time adding node to
    provisioned Kubernetes clusters.
-   Removed --container_cluster_cloud & --cloud=Kubernetes. Now to run VM
    benchmarks on Kubernetes, just set --cloud=GCP (or whichever) and
    --vm_platform=Kubernetes.
-   Added function under perfkitbenchmarker.publisher to de-serialize labels
    from a string.
-   Add support for S3 Express One Zone buckets with --object_storage_zone.
-   Add `--always_call_cleanup` flag for runs that need to run Cleanup, but may
    fail in Provision.
-   Add support for setting an app profile to use on an existing instance via
    `--google_bigtable_app_profile_id` for Cloud Bigtable YCSB benchmarks.
-   Add retryable failure sub-statuses for runs that fail on a `vm_util.Retry()`
    command timing out or exceeding its retry limit.
-   Local disks not included in striping are now available as scratch disks.
-   Add supportability of running Hadoop DFSIO on unmanaged Hadoop Yarn cluster.

### Bug fixes and maintenance updates:

-   Add 'runcpu --update' and 'runcpu --version' commands to install phase.
-   Set the command to download preprovisioned data to be robust and have a five
    minute timeout.
-   Make Speccpu17 fail if there are compilation errors that will cause missing
    results.
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
-   Switch to using Google Cloud Build for continuous integration.
-   Fix PrettyPrintStreamPublisher to make "cpu_utilization_per_minute" show up
    in the PKB results summary for cloud bigtable benchmark.
-   Added an option to install GCP NCCL plugins.
-   Updated hbase binding (from hbase10 to hbase12) for cloud bigtable ycsb
    benchmark and hbase ycsb benchmark.
-   Added retries around spurious InvalidPlacementGroup.InUse on AWS VM create.
-   Support Redis version 6 on managed Redis datastores.
-   Updated Aerospike server release version to 4.9.0.31.
-   Updated Aerospike client release version to 4.6.21.
-   Install zlib1g-dev dependency for Aerospike client.
-   Use OMB version 4.7.1 and parse new min/max columns.
-   Added --application_default_credential_file as an alternative way to
    authenticate with Bigquery.
-   Only install pip and pip3 when they are not already installed with Python.
-   Install pip and pip3 from get-pip.py to avoid issues with old packages.
-   Fix parsing Dstat 0.7.3
-   Update hadoop version to 3.3.1
-   Updated required numpy and six versions.
-   Added `--hadoop_bin_url` flag to allow overrides for Hadoop downloads.
-   Make RunBenchmark handle KeyboardInterrupt so that benchmark specific
    resources can be cleaned up on cancellation. Expose these errors via status.
-   Added --ycsb_fail_on_incomplete_loading flag to allow the test to fail fast
    in the case of table loading failures. --ycsb_insert_error_metric can be
    used to determine which metric indicates that loading failed (defaults to
    'insert Return=ERROR').
-   Enable the aggregation for "Return=NOT_FOUND" errors.
-   Added no_proxy flag for proxy settings
-   Stop attempting to delete PKB resources that failed to create.
-   Added a new user guide for bigtable walkthrough.
-   Sanitize the shell code in bigtable walkthrough doc: removing dollar sign
    and using variable expansion.
-   Added `--google_monitoring_endpoint` flag for querying a different endpoint
    than monitoring.googleapis.com. Used by `cloud_bigtable_ycsb`.
-   Update Go language binary to version 1.17.2
-   Broadens Azure quota detection parsing
-   AWS disk attaches now wait for attach, supporting io2 block express
-   Update the performance results of Bigtable testing which used a more proper
    client setup.
-   Update the runner's AWS CLI to 1.19.75.
-   Upgrade from AWS ecr get-login to ecr get-login-password.
-   Minor fix of the Bigtable benchmarking user guide.
-   Enable icelake and milan as --gcp_min_cpu_platform options.
-   Update the bigtable tutorial readme with the content of batch_testing.md.
    Unneeded files are removed.
-   Fix fio_write_against_multiple_clients additional samples and metadata.
-   Use real URLs as the links in Bigtable walkthrough doc.
-   Add option to publish to a subfolder in cloud storage publisher.
-   Parse resulting output matrix by indexing from the bottom up instead of top
    down.
-   Double build time for all cloud's docker images, for a more complex build
    script.
-   Add required dataflow option --gcpTempLocation and --region to
    gcp_dpb_dataflow provider.
-   Support taking FLAGS.dpb_jar_file and FLAGS.dpb_wordcount_additional_args
    when running wordcount benchmark.
-   Add some required types to BaseAppServiceSpec.
-   Uses nic type of GVNIC by default (instead of VIRTIO_NET) on GCE
-   Rename pkb's --placement_group_style values to reflect their cloud-specific
    CLI arguments (GCP - 'COLLOCATED'/'AVAILABILITY-DOMAIN'; AWS -
    'cluster'/'spread'/'partition'; Azure -
    'proximity-placement-group'/'availability-set'). Cloud-agnostic value
    'closest_supported' will choose the most tightly-coupled placement policy
    supported.
-   Fix how the CBT client is installed for the cloud_bigtable_ycsb_benchmark
    (when --google_bigtable_client_version is set) and use the `cbt` CLI instead
    of the hbase shell to create and delete tables.
-   Update Bigtable benchmarking configs along with new docker image release.
    Important dates are added to the user guide.
-   Add `--assign_external_ip` flag to allow benchmarking VMs without creating
    external (public) IPs for better security and reduced costs on AWS, Azure,
    and GCP. The `--connect_via_internal_ip` flag should also be used in this
    case.
-   Add `--boot_completion_ip_subset` flag to determine how to measure Boot
    Completion
-   Add `--azure_subnet_id` flag to use an existing subnet instead of creating a
    new one.
-   Remove `--google_bigtable_enable_table_object_sharing`. Use
    `--ycsb_tar_url=https://storage.googleapis.com/cbt_ycsb_client_jar/ycsb-0.14.0.tar.gz`
    to retain the previous behavior.
-   Remove `--google_bigtable_hbase_jar_url`. Rely on
    `--google_bigtable_client_version` instead.
-   Fix how environment variable is set for direct path
-   Fix incorrect string concatenation causing Snowflake Throughput runs to
    fail.
-   Remove reboot after changing sysctl and load via sysctl -p instead.
-   Clean up settings related to the Cloud Bigtable Veneer client.
-   Don't install HBase dependencies when using the Cloud Bigtable Veneer
    client.
-   Require monitoring.write scope for client side metrics when using the Cloud
    Bigtable veneer client.
-   Add flag dpb_job_type and support running native flink pipeline on
    dataproc_flink.
-   Cleanup Coremark compiling flags.
-   Remove cygwin codepath.
-   Moved flags from `pkb.py` to `flags.py` to help avoid circular dependencies.
-   Updated tracer dstat to use pcp dstat.
-   Removed Windows 2012 after loss of support on all clouds.
-   Formatted entire directory with https://github.com/google/pyink.
-   Added a new flag `--azure_attach_disk_with_create` (default=True) to
    enable/disable attach of disks to VM as a part of Disk creation for Azure.
-   Using the flag `--gcp_create_disks_with_vm=false` in provision_disk
    benchmark to separate disk creation from VM creation in GCP and get the disk
    create and attach time.
-   Reduce duplicate code in MaintenanceEventTrigger.AppendSamples().
-   Attach "run_number" label to "LM Total Time" sample.
-   Refactored `azure_virtual_machine.py` to use `azure_disk_strategies.py` and
    Enabled disk provision benchmark for Azure.
-   Refactored `aws_virtual_machine.py` to use `aws_disk_strategies.py` and
    Enabled disk provision benchmark for AWS by using
    `--aws_create_disks_with_vm`.
-   Enabled parallel/bulk create and attach of GCE, AWS and Azure remote disks.
-   Set `--always_call_cleanup=True` flag as the default for `cluster_boot`.
    This prevents leaking `tcpdump` processes from runs that fail in the
    Provision phase.
-   Test change.
