# v1.14.0

Breaking changes:
- Replaced gflags with absl-py. (GH-1518)
- Renamed GPU-related flags like so (old_flag_value -> new_flag_value):
  - gce_gpu_count -> gpu_count
  - gce_gpu_type -> gpu_type
- Replaced object_storage_objects_written_file* flags with prefix versions.
- Renamed tf_version flag to tf_pip_package flag to allow the user to specify
  the full tensorflow pip package name to be installed, instead of just being
  able to control the version.
- Changed default Tensorflow package when using CPUs to an Intel CPU-optimized
  build.
- Renamed cuda_toolkit_8 to cuda_toolkit
- Migrated cluster boot benchmark default machines to 'default_dual_core'.
- Changed metric name in mnist and inception3.
- Renamed the `tf_batch_size` flag in tensorflow_benchmark to `tf_batch_sizes`.
- Updated GCP sole tenancy support.  Removed `gcp_host_type` added
  `gcp_node_type`.
- Fixed missing installation directory in gpu_pcie_bandwidth benchmark.
- Removed duplicated metrics from the HPCC benchmark. Metrics now use the name
  from the summary section of the benchmark-produced output.
- Benchmarks are expected to not modify FLAGS in any way. If FLAGS are modified
  and multiple run configurations are run in a single PKB invocation, benchmark
  configurations may be incorrect.
- Changed TF Serving benchmark to use ResNet instead of Inception.
- Renamed prepare_sleep_time flag to after_prepare_sleep_time.
- multichase_taskset_options flag changed to multichase_numactl_options.
- Deprecated CUDA version 8.
- Remove support for supplying CUDNN package path via the 'cudnn' flag.
- Raise IssueCommandError by default when vm_util.IssueCommand return code is
  non-zero.  Previous behavior can be emulated by setting
  `raise_on_failure=False`.
- Fix AliCloud support by using the newer aliyun (instead of aliyuncli) cmd
  line utility and removing Paramiko dependency
- Changed support for reusing buckets for Azure blob service benchmarking. The
  storage account and resource group are now named based on the bucket name so
  that subsequent runs can use the same bucket.
- Changed Windows Server versions for GCP and Azure. Before this AWS used
  Windows Server Core (non-graphical), while GCP and Azure used Windows Server
  Base (graphical). All existing windows os_types now refer to the Core
  version and there is a new 'windows*_base' os_types for users who want to RDP
  in and use a full GUI.
- Changed `--aws_user_name` default from `ubuntu` to `ec2-user`. Individual
  OS types including Ubuntu should still override it appropriately.

New features:
- Windows benchmarks can now be run from linux controllers.
- MXNet benchmarks can now be run from linux controllers.
- Added initial support for preprovisioning benchmark binaries in the cloud,
  if binaries are not located in local /data directory.
- YCSB benchmark for Cloud Redis in GCP, Elasticache Redis in AWS, and
  Redis Cache in Azure.
- YCSB benchmark for DynamoDB in AWS.
- Added a flag, `run_stage_iterations`, which causes a benchmark's run stage to be
  called a specified number of times
- Added cuda_toolkit_version flag
- Added support for CUDA Toolkit 9.0
- Added MXNet support for CUDA Toolkit 9.0
- Added new version of HPCG, with CUDA 9 support
- Added latest gen 'default_dual_core' machine type configuration for AWS, Azure and GCP.
- Added memory as disk type for Linux only.
- Added support for publishing individual dstat samples with
  `--dstat_publish_regex`.
- Added Inception v3 benchmark that supports CPU, GPU and TPU. This benchmark is
  the same as TensorFlow with inception3 for CPU and GPU.
- Added container image building.
- Added netperf container benchmark.
- Added AWS container registry (ECR).
- Added Azure container registry (ACR).
- Added Google container registry (GCR) and added GKE autoscaling.
- Added `create_time` to VM metadata.
- Added new ycsb workload where each payload is 1mb versus the default 1kb.
- Added Tensorflow Serving benchmark which tests the throughput and latency of the
  standard model-server using a pre-trained inception3 model.
- Added AWS Fargate support.
- Added the ability to pass arbitrary parameters to tf_cnn_benchmarks.py in the
  Tensorflow benchmark, through the `benchmark_args` flag.
- Added hdrhistogram support to ycsb package.
- Added support for custom machine types on GKE.
- Added `container_cluster_version` flag to container_service.py.
- AWS EFS support via "disk_type: nfs"
- Added --disk_fill_size and --after_prepare_sleep_time flags
- Add timeout_minutes flag to assist with cleaning up stale resources
- All AWS resources and Azure resource groups are now tagged. (including timeout_minutes value).
- Added windows udp test using iperf3.
- Added timeout-decorator python package.
- Added timeout support for windows vm remote command call.
- Added IOR benchmark.
- Added mdtest to IOR benchmark.
- Added Elastic Container Service (EKS) as a container cluster type.
- Added ResNet benchmark.
- Added support for ACI (Azure Container Instances).
- Added spec cpu 2017 and feedback optimization for peak runs.
- Added glibc benchmark.
- Added lmbench benchmark.
- Added support to `--disable_interrupt_moderation` for Windows VMs on AWS.
- Added support to `--disable_rss` for Windows VMs on GCP.
- Added act benchmark.
- Added `--gce_tags` flag to add --tags when launching VMs on GCP.
- Added PKB support to publish samples immediately.
- Adding benchmarking of Memcached on cloud VMs using memtier benchmark.
- Adding support for different client/server machine types on memcached-memtier.
- Adding functionality in memtier benchmark to run a variable number of threads
  counts in each run.
- Adding support for AWS ARM machine types.
- Adding support for AWS ARM machines on SPECCPU.
- Added Horovod distributed Tensorflow traning benchmark.
- Added support for capacity reservations for VMs and added AWS implementation.
- Added support for adding additional flags to mount and /etc/fstab.
- Added support for Windows2012, 2016, and 2019 for AWS, Azure, and GCP.
- Terasort implementation on dpb backend.
- Added cluster boot benchmark implementation on dpb backend.
- Support multiple redis versions in cloud redis.
- Azure Files support via "disk_type: smb"
- Added MLPerf benchmark.
- Added stress-ng benchmark.
- Added flag --after_run_sleep_time which instructs PKB to sleep for the number
  of seconds specified after the run stage has completed.
- Added P5 cache type for Azure managed redis.
- Added AWS elasticache Memcached provider.
- Added additional options to the stress-ng benchmark.
- Added support for launching GCE VMs into the standard network tier.
- Added a demo under tools/.
- Added Nginx benchmark.
- Added support for measuring VM reboot time to the cluster boot benchmark.
- Added Cygwin support to WindowsVirtualMachine and enabled a Windows+Cygwin
  version of Coremark.
- Added sar trace utility to measure cpu steal time.
- Added ssh_via_internal_ip flag to use internal IP addresses when ssh'ing to
  runner VMs, will use external IP addrs if not set.
- Added support for launching GCP VM with shielded VM secure boot
- Added Amazon Linux 2 os type. `--os_type=amazonlinux2`
- Redis enterprise benchmark.
- Added flag `--append_kernel_command_line`, which allows appending to the
  kernel command line when using Linux os types.
- Added Spark SQL benchmark.
- Added aws_dynamodb_connectMax flag to vary the maximum number of connections
  to dynamodb.
- Added helpmatchmd flag to dump markdown formatted help strings
- Enable specifying source ip when creating a firewall rule.
- Added support for T4 GPUs on GCE.
- Added mpstat utility to measure cpu processor stats.
- Added a benchmark to run spark application on a cluster that computes an approximation to pi.
- Added a benchmark to run spark io workload on a cluster that provisions a database and queries it.
- Added flag --record_lscpu=True to record lscpu data as its own metric "lscpu"
- Added support for the Clear Linux OS type for AWS and static VMs (e.g. --os-type=clear)
- Added flag --placement_group_style to allow or disable different AWS placement groups options for ec2 VM benchmarks.
  Flag supports cluster, spread, and none placement group options.
- Added support for availability zones for Azure. Format for availability zone support is "location-availability_zone".
  Example: eastus2-1 specifies Azure location eastus2 with availability zone 1.
  A PKB zone can be either a Azure location or an Azure location with an availability zone.
- Added resource class and mapping for AWS Athena Service.
- Added support for Azure Dedicated Hosting. Simply add flag `--dedicated_hosts=True` to use.
- Records two new samples for /proc/cpuinfo data.
- Added support for specifying a number of vms per host for Azure & AWS Dedicated Hosting.
  Simply add flag `num_vms_per_host=<#>` to use. Benchmark fails if the # of vms per host specified exceeds the memory capacity of the host.
- Added support for skipping the creation of Azure availability sets with the --azure_availability_set=False flag (fixes #1863)
- Added support for AWS EFA networking with --aws_efa=True flag.
- Added NCCL benchmark for GPU networking.
- Added AWS DAX provider.
- Added Google Cloud Firestore ycsb benchmarks.
- Added support for un-managed data processing yarn cluster benchmarking.
- Added support for UDP_STREAM tests to netperf_benchmark
- Added flags to select stream size to netperf_benchmark
- Added placement group flag support for Azure. "cluster" will create proximity placement group.
  "spread" will create an availability set.
- Added a tcpdump collector with --tcpdump flag.
- Added ability to load datasets into BigQuery from csv files on GCS.
- Updated AzureVirtualNetwork to support location level virtual nets, which allows support for cross zone benchmark runs.

Enhancements:
- Support for ProfitBricks API v4:
  - Add `profitbricks_image_alias` flag and support for image aliases
  - Add new location, `us/ewr`
- Add `aws_image_name_filter` flag to ease specifying images.
- Add c5/m5 support for NVME disks.
- Add MNIST benchmark support for TPU, CPU and GPU
- Created KubernetesPodSpec which allows the user to specify kubernetes resource
  requests and limits, including fractional CPUs.
- Add `skip_pending_runs_file` flag and extension hooks to workaround SIGINT issues.
- Add support for `specsfs2014_load` parameter as an integer list.
- Publishers can be extended through external modules.
- Add `run_processes_delay` flag to stagger parallel runs.
- Add support for SPECspeed.
- Add new `os_types` Centos7, Debian9, Ubuntu1404, Ubuntu1604, and Ubuntu1710.
- Make it easier to RDP to PKB VMs
- Add `os_type` support to KubernetesVirtualMachine.
- Avoid setting up thread pool etc when run_processes is not set (to simplify
  debugging).
- Added a sample benchmark for descriptive purposes.
- Added GPU peer to peer topology information to metadata.
- Added a flag, `mpirun_allow_run_as_root` which allows OpenMPI to run in the
  case that the user is root.
- Modified KubernetesVirtualMachine to ensure that ssh is installed on the
  container.
- Added `container_cluster_num_vms` flag.
- Added the cluster's worker shape and worker count into the dpb service metadata.
- Added `azure_accelerated_networking` flag for Azure SR-IOV support.
- Added support for V100 GPUs on GCP.
- Add support for distributed TensorFlow benchmark.
- Added Azure Relational DB support for Postgres 9.6.
- Added `copy_benchmark_single_file_mb` flag for single file support.
- Record guest system information.
- Support for static edw clusters.
- Add more granularity to FAILED benchmarks with FailedSubstatus due to
  insufficient quota, cloud capacity, or known intermittent failures.
- Update sysbench benchmark to version 1.0. (deprecate 0.4 and 0.5 versions)
- Change GCP TPU command from alpha to beta.
- Update configurable parameters for ycsb benchmarks.
- Added tags to AWS kops instances and increased loadbalancer timeout.
- Allow cassandra_ycsb to run on a single vm.
- Adding support for using preprovisioned data for SPEC CPU 2006 benchmark.
- Support negative numbers when parsing an integerlist.
- Added float16 support for TensorFlow and MXNet Benchmarks.
- Added flag --mx_key_value_store in MNXnet benchmark.
- Added `time_commands` flag to enable diagnostic timing of commands
- Added image processing speed in mnist.
- Updated cloud TPU model link.
- Updated AWS spot instance creation.
- Added support for extending the failure sample with metadata if AWS spot VMs
  or GCP preemptible VMs are interrupted.
- Added flags `ycsb_version` and `ycsb_measurement_type` to support
  user-specified ycsb versions and measurement types.
- Added support to tensorflow_benchmark for running multiple batch sizes per run.
- Added resnet152 in TensorFlow benchmark default models.
- Added 50kb per payload ycsb workload.
- Added `num_cpus` to virtual_machine published metadata.
- Added a timeout to RobustRemoteCommand.
- Added support for the `gcp_min_cpu_platform` flag on GKE clusters.
- Preliminary support for NFS file systems
- Added BigQuery provider implementation to extend the edw benchmarking in Perfkitbenchmarker.
- Added support for authorizing gcloud to access Cloud Platform with a Google service account.
- Added support for specification of resource specific ready timeouts.
- Adding runner utilities to execute multiple sql scripts on edw clusters.
- TPU checkpoints and summaries are now stored in GCS.
- Updated cloud TPU models version.
- Support for cloud NFS services (no implementation).
- Added support for default batch sizes given a certain GPU type and model in
  the Tensorflow benchmark.
- Added method to get the NfsService from the linux_virtual_machine.
- Added support for fio job files in the data directory.
- Added InvalidConfigurationError.
- Added owner tag in metadata.
- Added support for NVIDIA P4 GPUs.
- Added YCSB timeseries parsing.
- Added Intel MKL to HPCC benchmark.
- Added flag support for enabling or disabling transparent hugepages on Linux
  VMs.
- Add AWS MySql Aurora support
- Abandon beta in TPU commands.
- Update ycsb hdr histograms to output {bucket:count} data set for latencies, removing percentile:latency_value data set.
- Added GPU test for ResNet benchmark.
- Update ResNet testcases.
- Remove unused lib in ResNet benchmark.
- Added way to run several specsfs benchmarks in a single PKB run.
- Add `--preemptible` in TPU creation command.
- Add `--skip_host_call` in ResNet benchmark.
- Update ResNet benchmark test for TensorFlow 1.9.
- Removed the flag resnet_num_cores from the resnet benchmark and added the flag tpu_num_shards to MNIST, resnet, and inception3.
- Update MNIST benchmark source path.
- Replace old flags `--tpu_name` and `--master` with a new flag, `--tpu`.
- Added support for EFS provisioned throughput.
- Added support for `--os_type` ubuntu1804. Removed ubuntu1710 for GCP.
- Added flag `--iperf3_set_tcp_window_size` to let the user avoid setting the
  TCP Window size in Iperf3 Windows performance tests.
- Added flags `--hbase_version` and `--hbase_use_stable` to set the HBase
  version.  Also upped hadoop to version 2.8.4.
- Updated cuDNN installation methods.
- Added support to schedule multiple TPUs.
- Added GroupZonesIntoRegions support for providers.aws.util.
- Added function to get the number of TPU cores.
- Update spec17 copies per run to respect system available memory.
- Add tensor2tensor benchmark.
- Change train_steps to train_epochs in TPU test.
- Add default tags like timeout_utc to GCE.
- Add validation to all the TPU tests.
- Add number of Train/Eval TPU shards in metadata.
- The spark service resource supports a new flag (spark_service_log_level), to control the log level and generated output.
- Updated openjdk 8 installation to support newer ycsb versions (after 0.13.0);
  Improved hdrhistogram result reporting in ycsb.py.
- Added flag `--ntttcp_udp` to allow the user to run UDP tests with ntttcp.
- Added flag `--ntttcp_packet_size` to allow user to set the packet size in
  ntttcp tests.
- Extract more data from the ntttcp results file and put into the metadata of
  the samples.
- Updated the default of Cloud Bigtable client version to 1.4.0. Added
  `--hbase_bin_url` to allow bypassing GetHBaseURL().
- Remove flag tf_benchmarks_commit_hash, and add tf_cnn_benchmarks_branch.
  Branch cnn_tf_vX.Y_compatible is compatible with TensorFlow version X.Y.
- Added flags `--ntttcp_sender_rb`, `--ntttcp_sender_sb`,
  `--ntttcp_receiver_rb`, `--ntttcp_receiver_sb` to control the socket buffer
  sizes in ntttcp tests.
- Move the ycsb_operation_count default from being set in the flag to being set in the workload file.
- Introducing a new flag gcp_dataproc_subnet which allows a user to specify the subnet that a dataproc cluster will be part of.
- Added support for running a subset of HPCC benchmarks.
- Introducing a new multi string flag 'gcp_dataproc_property' which allows a user to modify many common configuration files when creating a Dataproc cluster.
- Added configuration support for specifying type of the boot disk for a Dataproc cluster.
- Added AddTag method to AzureResourceGroup.
- Added some support for IPv6 (on static machines).
- Added retransmit count to netperf metadata.
- Added a common base class to PKB, PkbCommonTestCase,
  which uses absl.testing.flagsaver to save and restore
  flag values on test setUp / tearDown.
- Remove all usage of mock_flags and replace with PkbCommonTestCase.
- Upgraded memtier benchmark to version 1.2.15.
- Add precision flag in Inception3 benchmark.
- Support Hadoop 3.x.x in hadoop_terasort
- Add z1 support for NVME disks.
- Add default tags to dataproc.
- Adding default run configurations for spec17 when none
  of os_type, runspec_config or is_partial_results flags is set.
- Add a flag to control the number of worker threads used by the
  memcached server
- Add default tags to GKE.
- Add support for using real data from GCS and add support to download GCS data
  to vm in the Tensorflow benchmark.
- Added flag `nttcp_config_list` to allow the user to supply a list of test
  configurations, all to be run in a single run phase.
- Add support for `--nouse_pkb_logging` to use standard ABSL logging instead.
- Improved support for booting more than 200 VMs with the cluster_boot benchmark.
- Adding version support to redis server and setting permissions for newer redis versions.
- Introduced app service metadata to indicate backend concurrency.
- Added `--ssh_reuse_connections` to reuse SSH connections which speeds up benchmarks with lots of short commands.
- Add abstract PreDelete method to the base resource class.
- Add ability for linux vms to print the dmesg command before termination
  using the flag '--log_dmesg'.
- TPU and its GCS bucket should be in the same region.
- Adding field count and field length flag overrides to ycsb load.
- Added `--ssh_server_alive_interval` and `ssh_server_alive_count_max` to adjust SSH server alive options.
- Support for cloud SMB services (no implementation).
- Added rwmixread option to fio scenarios. This option is ignored unless a split
  read/write workload is specified.
- Added gce_local_ssd_count and gce_local_ssd_interface metadata to
  GceVirtualMachine.
- Added option to use real training data to the Horovod benchmark.
- Added support to record individual latency samples in the object_storage_service
  benchmark using the flag --record_individual_latency_samples.
- Added `--ssh_retries` to adjust the number of times we retry for errors with a
  255 error code.
- Added `--num_benchmark_copies` which controls the number of copies of each
  configuration to run.
- Added `--zone multistring` flag as an additional way to specify zones. It can be
  used in conjunction with the `--zones` flag.
- Added `--before_cleanup_pause` to ease debugging.
- Added support for CUDA 10.1.
- Added `--skip_firewall_rules` flag to help manage firewall rule economy.
- Added Azure Premium Files support.
- Added 'os_type' metadata to virtual machines for use in performance samples.
- Storage specification related dpb terasort benchmark improvements.
- Added support to run fio against multiple clients.
- Added an optional flag to set a delay between boot tasks.
- Added precision flag to Horovod.
- Added a flag to Horovod that indicates that the VM is using a deep learning
  image.
- Add support to set load upper bound for specsfs2014.
- Add support for different workload sizes for stress-ng.
- Implemented RobustRemoteCommand for Windows.
- Extract GceVirtualMachine's GetNetwork into a method.
- Updated redis memtier benchmark to pre-populate the redis db before testing.
- Add Clear Linux support for AWS stripe disks.
- Load memcached server before running benchmark.
- Add support for xfs disk formatting.
- Add gluster_fio benchmark.
- Added support for static AWS VPCs with --aws_vpc and --aws_subnet flags.
- Added support to append a region name to a bucket name for object storage
  benchmarking. For Azure, the region is appended to storage accounts and
  resource groups as well.
- Added sysbench benchmarking for MySQL in a VM.
- Added check that cuda_toolkit is installed when installing cudnn.
- Added ability to set provider-specific MySQL flags for relational databases.
- Add support for running t3 burstable VMs on AWS without unlimited mode.
- Add support to run fio with a timeout on fio commands.
- Added support for reporting bigtable cluster cpu utilization, gated by flag
  --get_bigtable_cluster_cpu_utilization.
- Move vm_groups to inside the relational_db spec when there is one, to allow it
  to control what vms are used. Rename the old vm_spec and disk_spec under the
  relational_db to db_spec and db_disk_spec respectively, for added clarity.
- Added ability in stress_ng benchmark to test --cpu-method's.
- Updated cloud_spanner_ycsb_benchmark to add the support for GCP Go client
  library.
- Make Kubernetes virtual machines not rebootable by default, to avoid changing
  things that require rebooting, since doing so is not ideal for containers.
- Add Amd Blis support for HPCC.
- Add Stress-ng support to run on different versions, 0.05.23 and 0.09.25, which
  are the defaults in Ubuntu1604 and Ubuntu1804.
- Improved the usability of cloud bigtable ycsb benchmark by introducing flags
  --google_bigtable_static_table_name,
  --google_bigtable_enable_table_object_sharing, and --ycsb_skip_load_stage.
- Added `--cluster_boot_test_port_listening` for cluster_boot benchmark. When
  set to True it will report the time until the remote command port (SSH
  port for Linux and WinRM port for Windows) in addition to the time until
  a remote command is successfully run.
- Added flag `--cluster_boot_test_rdp_port_listening` to measure time until RDP
  port accepts a connection in Windows. It is True by default.
- Add support from reading from BigQuery Storage API in the
  dpb_sparksql_benchmark. It uses
  https://github.com/GoogleCloudPlatform/spark-bigquery-connector. Support is
  currently limited to GCP Dataproc provider, but it could run anywhere
  provided some auth is plumbed through.
- Add support for generating CLI download commands for object_storage_service.
- Added flag '--azure_low_priority_vms' to create Low Priority VMs in Azure.

Bug fixes and maintenance updates:
- Moved GPU-related specs from GceVmSpec to BaseVmSpec
- Fix ProfitBricks issue with extra `/` in the API url
- Fix ProfitBricks volume availability zone issue
- Bulk AllowPort restored.
- Moved CustomMachineTypeSpec and related decoders to their own module
- Updated GKE engine version to 1.9.6-gke.0 when using a GPU-accelerated cluster
- Update python and pip package names used when installed with Yum.
- Don't try to publish samples if there aren't any
- Disallow overwriting of benchmarks using the same benchmark name.
- Log exception thrown in RunBenchmark
- Explicitly state python package names for RedHat on GCP
- Always set password for 'postgres' user when creating PostgreSQL CloudSQL
  instances.
- Fixed EPEL package URL for Redhat 7.
- Updated to latest version of NVIDIA CUDA Toolkit 8.
- Updated Ubuntu image versions in Tensorflow benchmark.
- Don't log wget output when installing CUDA Toolkit 8.
- Install CUDA Toolkit 8 patch from NVIDIA.
- Updated Ubuntu image versions in MNIST and MXNet benchmarks.
- Allow SMB3 protocol for file transfer which is required for windows 2012 with encrypted transport
- Updated Ubuntu image versions in HPCG, MNIST and MXNet benchmarks.
- Fixed a bug getting the correct number of layers in MXNet.
- Gcloud alpha version no longer required for GPU and minimum CPU platform.
- Backfill image on GCE if empty.
- Remove logging of empty environment variables in vm_util.IssueCommand.
- Explicitly state python package names for RedHat and Centos in AWS and Azure.
- Remove flag "tf_use_nccl" because the latest TensorFlow benchmark removed this
  argument.
- "nightly" now is not a valid argument in GCP TPU commands.
- Make aerospike_ycsb runnable on Amazon AMI.
- Fixed assumption that HOME was set.
- Fixed issue with default K8s image setup.
- Install the correct TensorFlow pip package based on whether VM has GPUs or
  not.
- Change to root for command "lspci".
- Install package pciutils when install cuda_toolkit.
- Update TensorFlow benchmark arguments.
- Isolate Darwin OS pkb runs to fix Issue # 1563
- Fixed YCSB so it pushes workload once to each VM even if they are repeated.
- Simplified resource and spec registration.
- Fixed a bug in which the PKB recorded reboot time could be low for AWS because
  we would restart the creation timer for VMs that were reported as pending.
- Increased timeout of GKE Cluster create command from 10 to 15 minutes.
- Set ImageNet image shape as 3,224,224.
- Updated MountDisk commands to use named parameters.
- Calling repr(IntegerList) returns a readable string.
- Support installing azure-cli on RedHat systems.
- Fixed default behavior of using /usr/bin/time --quiet on all commands
- Fixed ycsb failure when the same workload is ran more than once.
- Fixed yum proxy config bug (GH-#1598 from @kopecpiotr)
- Create destination directory for Azure blob storage downloads.
- Made the pip package more robust by adding a symbolic link in /usr/bin if pip
  is installed in /usr/local/bin.
- Make ~/.ssh/config only readable by the owner.
- Increased timeout for Azure `az vm create` commands.
- Increased timeout for GCP `gcloud compute instances create` commands.
- Replace all underscores in the benchmark name with dashes when downloading
  preprovisioned benchmark data from Azure. This is because Azure blob storage
  container names cannot contain underscores.
- Fixed the bug of running distributed TensorFlow on multiple batch sizes.
- Updated `gcloud compute networks create` to use `subnet-mode`.
- Changed default CUDA version from 8.0 to 9.0.
- Updated default Tensorflow version to 1.7 when using GPUs.
- Updated default Tensorflow tf_cnn_benchmarks version to a commit dated April
  2, 2018.
- Fixed a bug in which boto could not be uninstalled on newer GCE images.
- Update each ycsb workload to have its own recordcount configuration if
  ycsb_record_count is not set.
- Fixed treatment of the boot time metric so that it is more like any other run
  stage metric.
- Fixed bug of modifying the providers/aws/util.AWS_PREFIX value.
- Made failures to create VMs on AWS, Azure, and GCP fail PKB quickly.
- Fix Kubernetes StorageClass deletion
- Added `git` installation to `tensorflow_serving` package.
- MountDisk throws exception if mounting the disk fails.
- Added support for preprovisioned benchmark data on KubernetesVirtualMachines.
- Refactored speccpu2006 benchmark to use common elements for both
  speccpu2006 and speccpu2017.
- Use flags['key'].parse(...) to set mocked flags in linux_virtual_machine_test
- Cleanup some redundant logging and duplicate decoder statements.
- Fixed build_tools package re-installation for speccpu2006.
- Fixed fio job parsing, section parameters should always overrides global
  job parameters.
- Refactored StaticVirtualMachines to use GetResourceClass() pattern.
- Fixing the Redshift provider implementation to extend the edw benchmarking in pkb.
- Support using --gcp_min_cpu_platform=none to clear --min-cpu-platform. This
  lets the flag override a benchmark spec.
- Fix windows security protocol casting.
- Added '--iteration' to MNIST benchmark. The default value of 50 is too small
  and can result in excessive communication overhead which negatively impacts
  performance.
- Added support for more AWS regions in the object storage benchmark.
- Add ability to skip known failing scripts when running edw profiles.
- Set the control port and data port for nuttcp benchmark.
- Fix overwriting of bandwidth variable in nuttcp benchmark.
- Fixed fio histogram parsing.
- Refactored AwsKeyFileManager out of AwsVirtualMachine.
- Added delay between server and client initiation to windows benchmarks.
- Defaulted static machines to linux based.
- Add time limit to windows fio benchmark.
- Adding -w buffer parameter to windows iperf3 benchmark.
- Fixed a bug in how we select images on AWS by introducing an additional
  regular expression to match against image names.
- Terminate long running servers on windows benchmarks with timeouts.
- Updated azure_cli package to match installation instructions.
- Fix helpmatch for intergerlist.
- Fix PSPing benchmark so that it runs on AWS and Azure.
- Upgrade CPU pip package version in the Tensorflow benchmark to version 1.6.
- Moved from ACS to AKS for Azure Kubernetes clusters.
- Cleanup and fix Beam benchmark.
- Sysbench failover tests added for GCP and AWS Aurora
- Sysbench qps metric added
- Fixed a bug of checking TPU exist.
- Add GetMasterGrpcAddress method to CloudTpu.
- Fix a bug of getting the number of TPU cores.
- Set number of images in ResNet benchmark command so it can support other datasets.
- Sysbench supports benchmarking MySQL 5.6
- Update memcached server to install from a pre-built package provided by the operating system.
- TensorFlow Serving benchmark now runs off master branch with optimized binaries
- Updated HPCC benchmark to version 1.5.0.
- Psping benchmark no longer report histogram for every sample metadata.
- Specifies the number of threads to use in MXNet CPU test.
- Fixed the way several unittests are using flags and mock_flags.
- Fixed linter issues in several unittests.
- Add python_pip_package_version as a class attribute of
  BaseVirtualMachine. Children classes that have the problem of
  https://github.com/pypa/pip/issues/5247 can use an older pip package.
- Update AWS Aurora Postgres default version from 9.6.2 to 9.6.8
- Update speccpu17 to write profiles in different files for each benchmark.
- Avoid installing unnecessary MySQL server for sysbench when client tools suffice.
- Added support for running the object service benchmark with a GCP service
  account.
- Add missing if __name__ == '__main__' stanza to some unittests, and fix those
  that were broken.
- Consolidate FlagDictSubstitution and FlagsDecoder into a simpler context manager,
  OverrideFlags, which does not abuse the internals of FlagValues.
- Changed GkeCluster so that it adds PKB metadata to all VMs that it creates.
- Upgraded OpenMPI from 1.6.5 to 3.1.2.
- Upgraded OpenBLAS from 0.2.15 to 0.3.3.
- Add flag to control database machine type for managed relational databases.
- Ensure randomly generated windows passwords start with a character.
- Upgrade Tensorflow version to 1.12.
- Install NCCL when installing Tensorflow with GPU support.
- Update AzureBlobStorageService to persist for max of `--timeout_minutes` and `--persistent_timeout_minutes`.
- Update S3Service to persist for max of `--timeout_minutes` and `--persistent_timeout_minutes`.
- Add --project flag to GoogleCloudStorageService MakeBucket command.
- Fix virtual_machine.GetResourceMetadata() so that it does not try to gather
  metadata from a VM that has not been provisioned yet.
- TPU Pod doesn't support eval. Stop Evaluation in train phrase in MNIST benchmark.
- Global steps was set incorrectly in Inception3 benchmark.
- Add AWS T3 instances to list of non-placement group capable machine types.
- Refactor managed redis resource base class into managed memory store resource base class
- Remove flag tpu_zone because VM and TPU should be in the same zone.
- Refactoring managed_memorystore to add GetIp, GetPort and GetPassword class methods for managed_memorystore base class.
- Run individual stressors in stress-ng one by one.
- Upgrade gcp cloud redis to use prod gcloud.
- Upgrade glibc_benchmark's version of glibc to 2.29.
- Add number of disks for AWS i3.metal instance to configuration
- Fix bug in Azure disk letter assignment when attaching more than 24 disks.
- Incremental fixes to support Python3.
- Fix bug in Azure Windows VM names for long run URIs where the name could be
  longer than 15 characters.
- Update aws dynamodb_ycsb_benchmark to use aws credentials from the runner vm.
- Reboot immediately during call to ApplySysCtlPersient.
- Set lower tcp keepalive thresholds on netperf vms.
- Introduced NumCpusForBenchmark() which should be used instead of num_cpus for benchmark configuration.
- Use stat -c %z /proc/ instead of uptime -s to find a boot timestamp since it
  is more universal.
- Added support for regex_util.ExtractInt to extract an integer from a string.
- Updated Coremark to v1.01 and improved its error handling and output parsing.
- Added flag support to Coremark for choosing a parallelism method. Defaults to
  the existing mechanism of pthreads.
- Make a temporary copy on VM's disk for scp on SMB.
- Change the GCE Windows VM boot disk type to PD-STANDARD by default to better
  match the default for GCE Linux VMs.
- Fixed azure credentials package to avoid pushing the entire .azure directory.
- Update bigtable benchmark to make configurations configurable, and report them.
- RobustRemoteCommand now retries on ssh connections refused.
- Fixed a bug that was causing AWS capacity reservation creation to fail.
- GceNetworkSpec's zone is equal to the VM's zone
- Creates the bucket from PKB rather than from within the VM that PKB launches
  in TPU test.
- Sets TPU zone MLPerf benchmark.
- Compile Stress-ng version 0.05.23 rather than installing its package.
- Added `--gce_firewall_rules_clean_all` option to network resource: clean up
  all the firewall rules that depend on the PKB-created network.
- Fix AWS stripe disks assumption that the NVME index for the boot drive is always 0.
- Increased default timeout for booting Windows VMs to 40 minutes.
- Expose filesystem type and block size in vm metadata.
- Fixed problem with AWS network creation in aws_dpb_emr, aws_nfs_service, and
  elastic_container_service.
- Create the client_vm of the relational_db attribute of BenchmarkSpec from the
  default vm_group if there's no clients group. Fixes pgbench benchmark failure.
- Fixed use of numpy.piecewise to be numpy > 1.13 compliant
- Fixed the cluster boot benchmark config to use SSD boot disk types for GCP and
  Azure, adding support for specifying a boot_disk_type in Azure.
- Fixed the default path of the spark example jar on an EMR cluster.
- Check if an AWS default route exists before creation.
- Fixed the broken reference to ycsb.YCSB_TAR_URL in the
  cloud_spanner_ycsb_benchmark. Without this, the benchmark is not runnable.
- Fixed bug of query failing to find AWS internet gateway after Delete called.
- Abort and log when container resources are exhausted.
- Add a retry on gcloud commands when rate limited.
- Check if an AWS firewall rule exists before creating one by querying AWS.
- Add flag overrides for server and client vm groups inside relational_db.
- Update requirements.txt for Python 3
- Upgrade hadoop to version 3.2.1.
- Install the netcat-openbsd and zlib.h debian package for aerospike_ycsb.
- Updated linux_virtual_machine.py to use the single source of truth of
  perfkitbenchmark_keyfile path for non-static VM, expanding the use case of
  stage-wise test, e.g., do prepare on machine A, do run stage on machine B.
- Added --scp_connect_timeout and --ssh_connect_timeout flag to replace the
  hard-coded values at the call sites of vm_util.GetSshOptions.
- Added support for Container Optimized OS in the GCP Provider. Support is
  currently limited to the cluster_boot benchmark.
- Unifying metadata creation on cluster boot.
- Added support for CoreOS Container Linux in GCP, AWS, and Azure Providers.
  Support is currently limited to the cluster_boot benchmark.
- PkbCommonTestCase extends absltest.TestCase for py3 test backports.
- Update tox python version.
- Remove the remote file before pushing a local file to the remote place. This
  can avoid permission issue as the pushed file only allows user to read or
  execute.
- Add support for unmanaged NFS.
- Fixed Cassandra branch name (GH-2025 from @marcomicera)
- Add AWS T3 cpu credits argument to sample metadata.
- Add OpenFOAM benchmark.
- Removed RDP setup from Windows Startup script, because it was not needed by
  any tested provider.
- Updated ycsb.py to handle the cases of read only and update only, where there
  is no result for certain hdr histogram group.
- Add `-m PEM` to `ssh-keygen` to fix AWS.
- Updated AWS Virtual Machine to handle shutting_down case when creating an instance.
- Default Docker image for cluster_boot on Kubernetes (GH-2061 from @marcomicera)
- Fixes to MySQL server and client settings to ensure enough data and temp
  directory space, and to prevent connection timeouts.

# v1.13.0

Breaking changes:
- Removed gpus_per_node metadata from stencil2d and hpcg benchmarks (GH-1455)
- Removed legacy disk metadata (use data_disk\* not scratch_disk\* and removed
  num_striped_disks + aws_provisioned_iops metadata) (GH-1506)

New features:
- Added YCSB benchmark for Cloud Spanner (GH-1387 from @haih-g)
- Added CUDA-enabled HPCG benchmark (GH-1395)
- Added ManagedRelationalDatabase base classes and tests (GH-1405)
- Added gpus to GceVmSpec. This is now the only way to create VMs with gpus on GCE due to a gcloud API change (GH-1406)
- Added flag_zip_defs which functions like flag_matrix_defs, but performs a zip
  operation on the axes instead of a cross-product (GH-1414)
- Added TensorFlow Benchmarks. (GH-1420)
- Added --network_enable_BBR flag (GH-1515)
- Added pgbench benchmark for managed PostgreSQL (GH-1505)

Enhancements:
- Added basic auth support for Mesos provider. (GH-1390)
- Added --failed_run_samples_error_length flag to limit failed run error length (GH-1391)
- Added `__eq__` and `__ne__` to IntegerList (GH-1395)
- Added total_free_memory_kb to VirtualMachine class and implemented it for
  Linux vms (GH-1397)
- Created hpc_util for a place to share common HPC functions
- Added --runspec_estimate_spec flag to calculate an estimated spec score (GH-1401)
- Added all HPCC metrics as samples when running HPCC (GH-1448)
- Added support for P100 gpus on Google Cloud Platform (GH-1450)
- Added gpu type to cuda_toolkit_8 metadata (GH-1453)
- Added num_gpus to cuda_toolkit_8 metadata (GH-1455)
- Added range mode to gpu_pcie_bandwidth which calculates average bandwidth over
  a provided range of transfer sizes (GH-1482)
- Added size support to fio_benchmark scenarios (GH-1489)
- Added RemoteCommandWithReturnValue (GH-1516)

Bug fixes and maintenance updates:
- Fixed provision phase of memcached_ycsb benchmark for non-managed memcached instances (GH-1384)
- Fixed GPU benchmarks on GCE to work with new GPU API (GH-1407)
- Changed cuda_toolkit_8 to use the gpu-specific base clock speed as the default (GH-1453)
- Changed default AWS P2 region for gpu benchmarks (GH-1454)
- Fix multi-threading issue in fio.job filename (GH-1474)
- Decreased speccpu2006 default disk size to 50gb (GH-1484)
- Updated azure library version 1.0.3 for blob storage (GH-1491)
- Added azure host_caching to metadata (GH-1500)
- Fixed fio histograms when generated with numjobs>1 (GH-1501)

# v1.12.0

Breaking changes:
- Azure CLI updated to 2.0 (GH-1359)
- Update gflags to version 3.1.1 (GH-1322)

New features:
- Azure container service (GH-1361)
- Add Reboot() method to BaseOsMixin (GH-1356)
- Add container cluster classes (GH-1355)
- Optional `--completion_status_file` flag to write the completion status of every config to a json file (GH-1349)
- Add `--gcp_min_cpu_platform` flag (GH-1336)
- Add GCP sole tenancy support (GH-1333)
- Add SHOC package (GH-1332)
- Add Stencil2D benchmark (GH-1332)
- Managed memcache service abstraction and a backend for AWS ElastiCache (GH-1235)
- Add Apache Beam benchmark (GH-1324)
- Add `image_project` to GCP YAML config options (GH-1319)
- Add support for AWS i3 (GH-1308)
- Optionally create failed run samples with --create_failed_run_samples (GH-1363)
- Add --storage_size flag for mysql_service benchmark (GH-1360 from @sspano)

Enhancements:
- Add optional step parameter to IntegerList (GH-1330)
- Upgrade to NVIDIA's latest version of CUDA Toolkit 8 (GH-1304)
- Add ID attribute to GCE VMs (GH-1318)
- Add ability to install netperf from local tar file (GH-1364)

Bug fixes and maintenance updates:
- Fix up README.md - incorrect command line example and added note on Amazon ES publisher (GH-1350 from @Lirt)
- Fix up README.md - remove outdated comments about GCP requirements (GH-1354 from @sspano)
- Fix creating instance with local SSD using NVMe interface (GH-1287)
- Make flag values consistent with gcloud CLI "SCSI" and "NVME" (GH-1287)
- Store os types in list instead of tuple so that it can be extended by third parties (GH-1348)
- Don't create host during VM Create if `num_vms_per_host` is specified (GH-1352)
- Fix up kubernetes provider (GH-1351)
- Add `--region` to describe/cancel spot-instance-request commands (GH-1347)
- Install sysbench05plus package in a separate path from sysbench (GH-1338)
- Change `--openstack_additional_flags` to be a list flag (GH-1340)
- Move gflags installation from netperf benchmark to a package (GH-1341)
- Move package installation of netperf from benchmark to a package (GH-1341)
- Use snake_case gflags APIs (GH-1328)
- Check if apt-update has been called in InstallPackages, and call it if it has not been (GH-1334)
- Fix broken regular expression in blazemark benchmark (GH-1311)
- Fix CloudSuite license page link (GH-1323)
- Update hadoop version (GH-1327)
- Use regional storage class for GCS buckets instead of DRA (GH-1326)
- Fix gflags FLAGS object initialization in tests (GH-1313)
- Fix bug with exception name in `azure_network.py` (GH-1314)
- Always set GPU clock speed in cuda toolkit installation (GH-1321)
- Specify `image_project` in `gpu_pcie_bandwidth` benchmark config (GH-1321)
- Move gpu clock speed logic to `cuda_toolkit_8` module (GH-1279)
- Add timeout to netperf remote script (GH-1306)
- Don't assume that gcloud is in PATH in providers.gcp.util.GetDefaultProject (GH-1366)
- Make AWS S3 bucket deletion retryable (GH-1343)
- Empty bucket at the end of the Run stage of the object_storage_service benchmark (GH-1343)

# v1.11.0

External contributions:
- Add ability to publish to InfluxDB (thanks @besanradwan GH-1278)
- Always set internal IP-Address on OpenStack VMs (thanks @yremmet GH-1228)
- Add CloudSuite data-analytics benchmark (thanks @ivonindza GH-1285)
- Update README.md (thanks @ianmaddox GH-1291)
- Fix a small typo in the anti-affinity FLAG lookup (thanks @joelio GH-1284)
- Deprecate flag openstack_volume_size (thanks @vhe182 GH-1220)
- Add ProfitBricks cloud api v3 support (thanks @aajdinov GH-1264)
- Enable OpenStack Cinder Volume Type Option (thanks @rossmartyn04 GH-1197)

New features:
- Add dpb_wordcount_benchmark (GH-1253)

Enhancements:
- Add helpmatch usage instructions (GH-1268)
- Add openStack_volume_type option decoder (GH-1267)
- Upgrade OpenStack CLI dependency to 3.6.0 (GH-1265)
- Add object storage service cold reads support, various improvements (GH-1290, GH-1274)
- Add support for AWS Spot Instances (GH-1288)
- Bump fio version to v2.17 and enable histogram (GH-1300)
- Let publisher.py republish already-collected samples (GH 1299)
- Allow specifying temp dir (GH-1277)
- Add --json_write_mode (GH-1276)
- Allow override hpcc binaries (GH-1269)

Bug fixes and maintenance updates:
- Lock samples json file before writing to it (GH-1265)
- Fixed exception caused by publishers being None (GH-1302)
- Add error and warning log level options to reduce log size (GH-1301)
- Use force for DigitalOcean deletes (GH-1281)
- Print helpmatch usage instructions (GH-1280)
- Fix fio parser regex (GH-1275)
- Specify AWS region when calling describe-images (GH 1273)
- Make CheckPrerequisites more comprehensive (GH-1272)
- Use image_project flag in disk creation if set (GH-1271)
- Minor bug fixes and configuration improvements (GH-1262)


# v1.10.0

External contributions:
- Re-write flag_util_test.py so it's easier to read (thanks @yilei GH-1244)
- Fix bug that prevents running bonnie++ on Azure (thanks @rgodha GH-1218)
- Add kernel compile benchmark (thanks @maggiejiao GH-1215, GH-1255)
- Change the documentation to reflect that work should be on master (thanks
  @besanradwan GH-1209)
- Add flags to capture latency, bandwidth and IOPS logs in Fio benchmark (thanks @meteorfox GH-1205)
- Re-add 'vm_util' object to fix broken master branch (thanks @vhe182 GH-1196)
- Add EPEL Repo to Silo Installation (thanks @akrzos GH-1153)

New features:
- Add GPU PCIe bandwidth test (GH-1234, GH-1250, GH-1252, GH-1254)
- Add blazemark (GH-1145)
- Add simple memcached ycsb benchmark based on aerospike ycsb benchmark (GH-1199)
- Add scimark2 installation package (GH-1185)

Enhancements:
- Use FLAGS.aws_user_name if set for Rhel (GH-1249)
- Fio enhancements (GH-1246)
- Use aes128-ctr if aes128-cbc isn't available (GH-1242)
- Make aeropsike able to run with raw devices on static vms (GH-1243)
- Add some metadata to netperf samples (GH-1236)
- Remove unneeded Azure code and add availability sets (GH-1232)
- Add cassandra read concurrency flag (GH-1233)
- Add a metadata attribute 'ssd_count' to spark_service (GH-1229)
- Add a metadata attribute 'spark_svc_cloud' to spark_service (GH-1226)
- Add ability to specify boot_disk_size in AWS vm spec (GH-1231)
- Add a metadata attribute 'spark_svc_cloud' (GH-1236)
- Set a default zone for the spark test to allow for subnet creation (GH-1222)
- Update the location of the spark examples jar on the emr cluster (GH-1222)
- Persist package installation across reboots (GH-1185)
- Allow mongodb_ycsb to specify readahead settings (GH-1256)

Bug fixes and maintenance updates:
- Close Aersospike connection in 1 second (GH-1241)
- Fix bug with having multiple Azure data disks (GH-1223)
- Fix aws spark service (GH-1222)
- Add some git ignore entries (GH-1222)
- Fix gcs credential conflict (GH-1221)
- Fix bug with run_processes (GH-1212)
- Remove collector from RunBenchmarkTask args (GH-1210)
- Add the minimum tox version to the "tox is not installed" message (GH-1201)
- Move sysbench05plus path prefix logic into package (GH-1185)
- Fix bug with specifying Azure image (GH-1204)
- Fix two files that were failing boilerplate check (GH-1203)
- Update README.md (GH-1200)

# 1.9.0

New features:
- Add config file imports and allow for circular imports and add documentation (GH-1163)
- Add a way to run benchmarks in parallel (GH-1192)

Enhancements:
- ContainerizedDebianMixin: Ping to a fixed docker image and remove the sudo hack (GH-1171)
- Auto detect  openjdk, libsnappy package versions (GH-1181)
- Add flags to govern subnet creation and Optionally create only 1 subnet (GH-1182)
- Install docker images as packages (GH-1184)
- Call FLAGS.get_help if it's available (GH-1187)
- Publish boot time samples for all tests (GH-1156)

Bugfixes and maintenance updates:
- Update requirements.txt to set version of contextlib2 (GH-1164)
- Fix bug with config flags (GH-1165)
- Updated the cloudsuite web-serving benchmark (thanks @nooshin-mirzadeh, GH-1166)
- Fix load command parsing (GH-1168)
- openstack: Open all TCP and UDP ports for the internal network (GH-1169)
- Fix aerospike metadata (GH-1170)
- Adjust aerospike default replication factor (GH-1172)
- Try replacing FlagValues._flags instead of FlagValues.FlagDict first (GH-1175)
- Fix ycsb aggregator (GH-1176)
- Fix bug during cleanup phase of object_storage_service benchmark (GH-1178)
- Netperf thinktime fixes and support think time in nanoseconds instead of microseconds (GH-1179, GH-1186, GH-1188)
- Fix sysbench05plus installation on ubuntu16.04 (GH-1183)
- Fix race condition in Aerospike benchmark (GH-1190)
- Clean up some pickling, unpickling issues (GH-1191)
- Calculate free ram with more robust /proc/meminfo instead of `free`(GH-1194)
- Fix multithread netperf (GH-1149)
- Multiple fixes and refactoring of the linux package management on vms (GH-1152)
- Fix dstat metadata in Analyze (GH-1154)
- Obey flag overrides for static vm specs (GH-1155)
- Fix links in README file for OpenStack and Cloudstack setup steps (thanks @shakhat, GH-1157)
- Fix issue with GetConfig using flags (GH-1160)
- Remove scratch disk from netperf and object_storage_service benchmark (GH-1147, GH-1148)
- Netperf changes to add num_streams metadata to samples (GH-1177)

# 1.8.1

Bugfixes and maintenance updates:
- Fixed default machine type for Azure (GH-1161)

# 1.8.0

New features:
- Multithreaded netperf benchmark (GH-1141, GH-1144)
- Terasort benchmark using spark service infrastructure (GH-1104)
- Support 10 or more disks on Azure (GH-1102)
- Add sysctl and set_files flags (GH-1096)
- Support DigitalOcean block storage (GH-1092)
- Add cloudsuite_data_serving_rec_count and cloudsuite_data_serving_op_count parameters to cloudsuite_data_serving benchmark (GH-1091)
- Add support for setting GCE boot disk size and type (GH-1106)
- Add flag to skip reload on ycsb benchmark (GH-1108)
- Support for AWS dedicated hosts (GH-1109)
- Object storage multistream write-only scenario (GH-1123)
- Allow multiple configs for the same benchmark in the same file (GH-1126)
- Add flat that allows run retries (GH-1129)
- Add fio_parameters flag (GH-1131)
- Add config matrices (GH-1134)
- Add events at the beginning and end of benchmarks (GH-1135)
- Add extra_zones flag to pkb (GH-1137)
- Add flag matrix filters (GH-1138)
- Support allowing port ranges on Firewall.AllowPort (GH-1139)
- Add static vm tags (GH-1142)

Breaking changes:
- Because of GH-1112, region names have changed.

Enhancements:
- Allow speccpu to run individual test (GH-1113)
- Use Azure resource management mode (GH-1112)
- GCP networks use subnets (GH-1087)
- Add top level flags key to configs (GH-1125)
- Precise resource usage for YCSB benchmarks (GH-1127)

Bugfixes and maintenance updates:
- Fix bugs with Terasort (GH-1136)
- Don't run container commands as sudo with ContainerizedDebianMixin (GH-1130)
- Automatically use amazon linux if os_type is rhel (GH-1124)
- Removed Panda as a dependency (GH-1118, GH-1119)
- Allow flag_util.ParseKeyValuePairs values to have colons in them (GH-1117)
- Add ycsb to data directory (GH-1116)
- Fix bug in mongodb_ycsb_benchmark cleanup (GH-1115)
- Fix bug where static VM disk specs were overridden (GH-1105)
- Use string formatting instead of addition to concatenate (GH-1103)
- Fixed regression with fio flag (GH-1097)
- Fix flags in static vm files (GH-1093)
- Fix wait_for_command.py: wait for status file to avoid race condition (GH-1090)

# 1.7.0

New features:
* Add ProfitBricks provider (thanks @tjb1019, GH-1005)
* Add YCSB JDBC benchmark (thanks @k1xme, GH-1049)
* Add support for st1 and sc1 disks on EBS (GH-1067)

Breaking changes:
* Change default gce_migrate_on_maintenance to True (GH-1069)

Enhancements:
* Update OpenStack provider to use OpenStacks CLI instead of python library
  (thanks @meteorfox, GH-942)
* Improve side-by-side tool (GH-1034)
* Specify flags to metadata (GH-1047)
* Allow Java version for Java benchmarks to be selected via command line (GH-1051)
* Add numjobs runtime option to fio_benchmark (GH-1066)
* Add netperf histogram support (GH-1072)
* Add multi-vm support for object_storage_service_benchmark (GH-1074)
* Add support for custom s3 endpoints for object_storage_service_benchmark
  (GH-1078)
* Add user-defined metadata for vms (GH-1080)

Bugfixes and maintenance updates:
* Refactor object_storage_service_benchmark (GH-1023)
* Support overriding ycsb_client_vms in benchmark spec (thanks @k1xme, GH-1042)
* Update YCSB to 0.9.0 and support latest logging format (thanks @k1xme, GH-1043)
* Fix zones flag (GH-1044)
* Update Bigtable to v2 API (thanks @tswast, GH-1045)
* Update maven version to 3.3.9 (GH-1046)
* Start background workload in prepare stage and stop in cleanup stage (GH-1048)
* Minor fixes in flag description (GH-1052)
* Only upload files needed for API testing script for
  object_storage_service_benchmark (GH-1053)
* Fix redis_ycsb benchmark to use multiple processes (GH-1062, GH-1068, GH-1070)
* Fix bug with custom machine types configs (GH-1077)
* Fix in documentation of ParseTimeCommandResult (thanks @skorgu, GH-1079)
* Replace Pandas dependency with Numpy (GH-1081)
* Turn off OpenBlas threading, improving HPCC on large vms (GH-1084)
* Cleanup object_storage_service benchmark (GH-1085)
* Cleanup aerospike_ycsb benchmarks (GH-1086)

# 1.6.0

New features:
* Apache Spark benchmark (GH-1014,GH-1011)
* Cloudsuite dataserving benchmark (thanks @ustiugov, GH-930)

Enhancements:
* Optionally publish VM hostnames in metadata (GH-1020)
* Control which netperf benchmarks are run via flag (GH-1029)
* Netperf benchmark reports min and max latency (GH-1013)
* Add multichase to the google benchmark set (GH-1018)
* Update Cloudsuite web serving benchmark (thanks @ivonindza, @GH-998)
* Object storage updates (GH-987)
* Repeat Run stage (GH-1032)

Bugfixes and maintenance updates:
* Fix path to cassandra-cli (thanks @adamisrael, GH-1006)
* Update ycsb version; affects cloud_bigtable_ycsb_benchmark, hbase_ycsb
  (GH-1021, GH-1025, GH-1031)
* Use latest gsutil version (GH-1012)
* Remove duplicate requirements files (GH-975)
* Update openblas version (GH-1003)
* Background workload refactor (GH-1033)

# 1.5.0

New features:
* Add cloudsuite graph analytics benchmark (thanks @mdrumond; GH-986)
* Add initial implementation of multichase benchmark (GH-977)
* Add cloudsuite media streaming benchmark (thanks @ivonindza; GH-993)
* Add analysis functions for processing multistream object storage benchmark data (GH-905)
* Add CloudSuite Data-caching benchmark (thanks @neo-apz; GH-970)
* Add ability to bundle PKB into a python-executable zip file (GH-971)
* Add os_type for Juju Mixin (thanks @AdamIsrael; GH-764)
* Add CloudSuite in-memory analytics benchmark (thanks @ivonindza; GH-934)

Breaking changes:
* Replace the old CloudSuite Web Search benchmark with the docker version (thanks @javpicorel; GH-931)
* Update Aerospike version (GH-978)

Enhancements:
* Add --fio_blocksize option (GH-943)
* Update speccpu2006 benchmark to support iso file (GH-944)
* Allow --run_stage to specify multiple stages (GH-935)
* Add flag to control iperf timeout (GH-994)

Bugfixes and maintenance updates:
* Fix cloud bigtable and hbase (GH-1001)
* Install openssl for the object storage benchmark (GH-1000)
* Fix bug in MongoDB YCSB benchmark (GH-999)
* Change import_util.LoadModulesForPath to also load packages (GH-983)
* Add 'ap-northeast-2' to the S3 regions table (GH-990)
* Change check-lint.sh to invoke flake8 via tox (GH-979)
* Change GetLastRunUri to handle the new temp directory structure (GH-985)
* Support S3 Signature Version 4 (GH-984)
* Add back support for python-gflags version 2. (GH-973)
* Define units.Unit.\_\_ne\_\_ (GH-976)
* Reorder some PKB initialization steps (GH-965)
* Move to gflags version 3.0.4 (GH-969)
* Modify semantics of UnitsParser's convertible_to parameter (GH-968)
* Add percent (%) as a recognized pint unit (GH-964)
* Improve Azure CLI version check (GH-967, GH-958)
* Don't request SSH verbose output when --log_level=debug (GH-962)
* Move per-provider package requirement files (GH-961)
* Move pint unit registry code into a separate module (GH-960)
* Only check python package requirements if requirements.txt exists (GH-959)
* Changed LoadProvider to accept un-lowered cloud provider name (GH-957)
* Change bg tests to patch and verify call counts for each VM separately (GH-954)
* Make BenchmarkSpec.vms order consistent across runs (GH-953)
* Fix os type related issues and bugs (GH-950, GH-955, GH-952, GH-951, GH-949)
* Call CheckPrerequisites earlier (GH-947)
* Add percentiles option to PercentilesCalculator (GH-910)
* Fix publisher bug when disk_size is None (GH-946)
* Handle unset PYTHONPATH in tox.ini (GH-941)
* Add a helper function to BenchmarkConfigSpec to redirect flags (GH-936)
* Add support for interrupting child threads created by RunThreaded (GH-926)
* Fix bugs and improve the Cloud Bigtable benchmark (GH-937, GH-933, GH-932)
* Add a helpful error message if a UnitsParser parses a unitless value (GH-963)

# 1.4.0

New features:

* openstack: Add support for optional floating ip pool (thanks @meteorfox, GH-861)
* openstack: Use Keystone session for handling authentication (thanks @meteorfox, GH-870)
* Support object storage classes in object storage service benchmarks (GH-895)
* Add Object Size Distributions in object storage service benchmarks (GH-888)
* Add MultiStreamThroughput benchmark to object storage benchmarks (GH-840)
* Adds a SPEC SFS 2014 benchmark which runs against GlusterFS (GH-876)
* gce_virtual_machine: Support user provided instance metadata (GH-859)

Improvements and fixes:

* openstack: Wait until VM deletion has been completed (thanks @meteorfox, GH-904)
* openstack: Fix floating IP allocation and deallocation (thanks @meteorfox, GH-862)
* rackspace: Fix missing flags bug (thanks @meteorfox, GH-903)
* Allow user to specify a flag when they run object storage benchmark on GCP, and default that flag to the latest known working version. (GH-925)
* Update mechanism used to get iperf server process id. (GH-921)
* Rename variables and improve documentation of BaseOsMixin.PullFile. (GH-923)
* Fix WindowsMixin._GetNumCpus when more than one Win32_processor exists. (GH-920)
* Add exception types to except clauses. (GH-893)
* Add Flag for List Consistency Iterations (GH-889)
* Add unit tests for scripts (GH-882)
* Add disk type examples to README.md (GH-871)
* Copy-edit the README (GH-877)
* Turn off selinux for mongodb. (thanks @akrzos, GH-867)
* Use temp files for Popen stdout and stderr in IssueCommand. (GH-878)

# 1.3.0

External contributions:
* Add RHEL based virtual machines to OpenStack provider. (thanks @akrzos;
  GH-858)
* Change rackspace provider information to use official CLI (thanks @meteorfox; GH-844)
* Add rackspace requirements (thanks @meteorfox; GH-805)

New features:
* Support flags in YAML format. (GH-857)
* Check version of required packages at runtime (GH-834)
* User-specified multiregion for GCS benchmarking (GH-845)
* Support metadata for gcp instances (GH-859)

Bugfixes and maintenance updates:
* Change rackspace's AllowPort to enable UDP (thanks @meteorfox; GH-805)
* Allow most recent verison gcs-oauth2-boto-plugin (GH-849)
* Require Pint >= 0.7 (GH-850)
* Update PIP (GH-842)
* Fix windows log message (GH-832)
* Properly Pickle Pint Quantities (GH-830)
* os_type added to boot benchmark metadata (GH-826)
* Better handle Azure timeouts (GH-825)
* Better handling of AWS integration tests. (GH-869, GH-868)

# 1.2.0

New features:
* Introduced Object Sizes in flags (GH-808).
* Add ListDecoder for verifying a config option that expects list values
  (GH-807).

Enhancements:
* Bump HBase to 1.0.3 in hbase_ycsb benchmark (GH-822).
* Change MockFlags to be more like the real FlagValues (GH-812).
* Rename test_flag_util.py and add FlagDictSubstitution tests (GH-811).
* Create BenchmarkConfigSpec to aggregate benchmark input checking (GH-810).
* Remove flag proxy objects (GH-802).

Bugfixes and maintenance updates:
* Fix sample iperf config (GH-801).
* FixIntDecoder and FloatDecoder behavior when min=0 or max=0 (GH-800).

# 1.1.0

External contributions:
* Add additional percentile samples to mysql_service benchmark and others
  (thanks @zbjornson; GH-729).
* Add Mesos-Marathon as a provider (thanks @mateusz-blaszkowski; GH-679,
  GH-731).
* Use only available floating-ips for OpenStack (thanks @meteorfox; GH-733).
* Add OpenStack CLI-based support for object_storage_service benchmark (thanks
  @meteorfox; GH-738).
* Make printed benchmark results more readable (thanks @wonderfly; GH-747).
* Clean up formatting, grammar, and markdown in README.md (thanks @mbrukman;
  GH-748).
* Add links to projects and licenses in README.md (thanks @mbrukman; GH-763).
* Fix a bug that caused --helpxml to not work (thanks @jldiaz; GH-782).
* Make OpenStack VM username configurable with a new --openstack_image_username
  flag (thanks @lwatta; GH-788).

New features:
* Allow users to specify a storage region for the object_storage_service
  benchmark with a new --object_storage_region flag (GH-609).
* Add support for running a background CPU workload while executing a benchmark
  (GH-715, GH-762).
* Allow creating GCE custom VMs easily from command-line flags (GH-727).
* Add the --gce_network_name to allow GCE users to use pre-existing networks
  (GH-746).
* Publish a PTRANS sample from the hpcc benchmark (GH-785).
* Add support for running a background network workload while executing a
  benchmark (GH-786).
* Add a CSV sample output format (GH-791).

Enhancements:
* Trim blocks while formatting and mounting disks (GH-692).
* Raise an error when no requests succeed in redis_benchmark (GH-702).
* Update the Google benchmark set (GH-705).
* Add input-checking to disk specs (GH-675, GH-779).
* Let disks on Windows machines return Windows DeviceIds (GH-743).
* Speed up Cassandra cluster boot by setting "auto_bootstrap" to false (GH-751).
* Defer package update for Ubuntu (GH-752).
* Add provider-specific requirements.txt files (GH-753).
* Increase preload thread count for YCSB benchmarks (GH-760). This change
  affects some of the samples generated by the aerospike_ycsb, cassandra_ycsb,
  and mongodb_ycsb benchmarks, including the overall Runtime and overall
  Throughput.
* Guarantee order of VM and disk spec config option decoding (GH-792).

Bugfixes and maintenance updates:
* Create one AWS VPC/IGW/PlacementGroup per region (GH-687, GH-798).
* Update README.md instructions for setting up PKB for GCE (GH-689).
* Update comments in pkb.py to reflect new provision and teardown stages
  (GH-691).
* Fix bug that prevented object_storage_service benchmark from running in stages
  (GH-694).
* Add test helpers that make it easier to mock command-line flags (GH-695,
  GH-730).
* Delete set-interrupts.sh (GH-703).
* Make process exit status non-zero upon benchmark failure (GH-713).
* Move "To run again" message from teardown to cleanup stage (GH-714).
* Fix creation of AWS VMs using Placement Groups (GH-721).
* Create AWS/GCE default access rules before creating VMs (GH-726, GH-756).
  This change affects the Boot Time measured by the cluster_boot benchmark.
* Fix a GCE boot disk creation bug (GH-736).
* ntttcp benchmark fixes (GH-737).
* Force mongodb_ycsb benchmark to use only one VM (GH-755).
* Unmount /mnt if it exists prior to creating a scratch filesystem (GH-757).
* Move provider constants to a separate module (GH-758).
* Fix a bug when creating an AWS VPC outside the user's default region (GH-774).
* Add tests for generating help strings (GH-782).
* Kill servers started by the iperf benchmark as part of its cleanup (GH-784).
* Fix some style inconsistencies (GH-789).
* Move operating system constants to a separate module (GH-790).
* Fix some broken tests (GH-795, GH-796).

# 1.0.1

* Fix for benchmark_compatibility_checking flag. (thanks @mateusz-blaszkowski;
  GH-707)

# 1.0.0

New features:
* Added decoders to configs which allow for additional config validation
  (GH-672)
* Add support for GCE custom VMs (GH-664)
* Providers can now selectively support benchmarks (GH-690)

Breaking changes:
* Enhanced config usage by multi-node benchmarks - this changes some of their
  default metadata. (GH-669)
* YCSB histogram results are now not included by default (GH-656)

Enhancements:
* Ping benchmark is now bi-directional (GH-685)
* Metadata flag can be specified multiple times (GH-684)
* Allow AWS regions as well as zones (GH-658)
* Added 'vm_count' metadata to all benchmarks (GH-659)
* GCP now creates networks (GH-648)
* Added provision and teardown run_stages (GH-652)
* Update tomcat_wrk to report more samples (GH-650)
* Add a flag to control the log file log level (GH-651)

Bugfixes and maintenance updates:
* Added helper for mocking FLAGS (GH-678)
* Fix for silo benchmark which fails when running behind proxy (thanks
  @mateusz-blaszkowski; GH-680)
* Fix for not-installed curl package (thanks @mateusz-blaszkowski; GH-681)
* Invoke YCSB from its installation directory (GH-677)
* Fix bug with detecting run_uris (GH-673)
* Fix the image_project flag so it works as intended (thanks @wonderfly; GH-666)
* Add explicit --boot-disk-auto-delete to GCE VM creation (GH-670)
* Improved the README layout and fixed errors (GH-661, GH-660)
* Broaden exception caught during PKB run (GH-667)
* Fixed bug with run stages (GH-665)
* Fixed formatting of docstring (thanks @wonderfly; GH-662)
* Fixed bug with GCP networks (GH-663)
* Add GetMachineTypeDict() to replace machine_type in Sample metadata (GH-653)
* Add GcloudCommand helper (GH-649)
* Fixed Tomcat download URL (GH-700)
* Retry RemoteCopy operations (GH-699)
* Various fixes to data disks for OpenStack (thanks @meteorfox; GH-688)

# 0.24.0

New features:
* CS2 rachel web serving benchmark (thanks @rmend016; GH-451)
* Add [AliCloud](http://intl.aliyun.com/) provider (thanks @hicrazyboy; GH-611)
* Add tomcat/wrk benchmark (GH-598)

Breaking changes:
* Remove `--parallelism` flag (GH-633)
* Rename `fio` flags (GH-518, GH-581)
* Added new disk type options to clarify use. (Breaking change) we will remove
  the current options in a coming release (GH-599)

Enhancements:
* Docker support in DigitalOcean and StaticVirtualMachine (thanks @maxking; GH-528)
* Always use latest Azure package (GH-585)
* Support `read/counter_read/mixed` in cassandra_stress benchmark (GH-607,
  GH-592)
* Add pretty-printed benchmark run summary table (GH-620)
* Support new configs in the
  [Cloudsuite web serving](http://parsa.epfl.ch/cloudsuite/web.html) benchmark
  (GH-605)

Bugfixes and maintenance updates:
* Move providers to their own directory (GH-617)
* Automatically register VM classes and specs (GH-600)
* Only load needed provider modules (GH-635, GH-636)
* Prefix benchmark and packages directories with `linux_` (GH-640)
* Update to OpenStack Nova (thanks @meteorfox; GH-613)
* Add `AUTHORS` file; update copyright header (GH-618)
* Continue executing benchmarks after one fails (GH-614)
* Disk integration tests for GCE, AWS, Azure (GH-571, GH-595)
* Update Windows `cluster_boot` benchmark to match Linux one (GH-647)
* Update `README` for lazy-loading providers (GH-645)
* Only call `SetupPackageManager` if `install_packages` is True (GH-644)
* Generate per-VM-group SSH aliases (GH-639)
* Move AWS and Azure `_GetDefaultImage` calls to `_CreateDependencies` (GH-641)
* Get Azure default image name from `azure vm image list` (GH-637)
* Add `boto` lib version to object storage benchmark metadata (GH-632)
* Improve the guide on using Docker (thanks @mateusz-blaszkowski; GH-628)
* Improvements to VM pool handling (GH-626, GH-608)
* Make benchmark metadata specify benchmark name (GH-615)
* Support overriding Azure client lib version (GH-587)
* Update Cassandra version to 2.1.10 (GH-577)


# v0.23.0

Known Issues:

* Resources fail to be cleaned up when PerfKitBenchmarker receives SIGINT while
  running with `--parallel` (GH-529).

New Features:

* Added [CloudStack](http://cloudstack.apache.org) provider support (thanks @syed; GH-558).
* **❢ BREAKING CHANGE:** Added a more flexible benchmark configuration system
  (GH-510, GH-546). This supports running the same benchmark multiple times,
  and running a benchmark using heterogeneous machine types and clouds. See [the
  wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/PerfkitBenchmarker-Configurations)
  for more information. This replaces the configuration system under
  `perfkitbenchmarker/deployment`.
* New benchmark: `redis_ycsb`, which runs YCSB against a single YCSB node (GH-511).

Enhancements:

* Added flag: `--archive_bucket`, to archive the results of a run to Google Cloud
  Storage or S3 (GH-489).
* Added `--prefill_device` option to fio benchmark. Note that the `fio` benchmark
  now defaults to *not* pre-filling the device when testing against a filesystem (GH-515, FH-516).
* Updated `cloud_bigtable_ycsb` benchmark to pre-split table, following HBase recommendions (GH-524).
* Added additional logging to `LinuxVirtualMachine.RobustRemoteCommand` (GH-534).
* Added support for local disks with EC2 `d2` types.
* Kubernetes: Support `emptyDisk` disk types (thanks @mateusz-blaszkowski GH-565).
* `cluster_boot`: Add `num_vms` metadata (GH-575).
* Add published `run_uri` to VM metadata (GH-579).
* Add a flag to specify the Azure Python library version in object storage benchmark,
  default to the latest version (GH-585, GH-587).

Bugfixes and maintenance updates:

* Kubernetes: Adapt provider to work with different versions of `kubectl`
  (Thanks @mateusz-blaszkowski; GH-574).
* Eliminated a spurious resource warning (GH-522).
* Fixed a string comparison bug in Object storage benchmark (GH-526).
* Rackspace; Removed an extra parameter from firewall implementation (thanks @meteorfox, GH-531).
* Rackspace: Fixed an SSH key upload bug (thanks @meteorfox, GH-539).
* Fix an issue with loggin errors in `vm_util.RunThreaded` (GH-542; thanks @mateusz-blaszkowski for reporting).
* `mongodb_ycsb`: Update default write concern to "Acknowledged" (equivalent to the previous deprecated option, safe) (GH-543).
* Specify SSH port when using SCP (thanks @mateusz-blaszkowski GH-548).
* Fix string conversion (thanks @mateusz-blaszkowski GH-556).
* `hbase_ycsb`: Make load records use a BufferedMutator (GH-566).

# v0.22.1

* Update the default image used in Azure tests to Ubuntu 14.04.3 (runs failed
  with the current image.) (cherry-pick of #520)

# v0.22.0
* New Features:
  * Add Kubernetes as a provider where benchmarks can be run (thanks @mateusz-blaszkowski) (GH-475)
  * Add EPFL CloudSuite WebSearch Workload (thanks @Vukasin92) (GH-422, GH-479)
  * Aerospike_yscb_benchmark (GH-486)

* Enhancements:
  * OpenStack checks/changes for attaching volumes, default volume size, config improvements, … (thanks @kivio) (GH-454, GH-459, GH-464)
  * Fio test improvements and clean-up - template files, logging, ...  (GH-421, GH-501, GH-502)
  * Add improvement to use compiled crcmod when benchmarking Google Cloud Storage (GH-461)
  * Collect Fio data over time via “--run_for_minutes” flag (GH-477)
  * Execute command improvements to add better logging and parallelism (GH-488, GH-470)

* Bugfixes and maintenance updates:
  * Update to allow PKB to work with the latest Azure CLI 0.9.9 (GH-485)
  * Updated fio version to 2.2.10 (GH-509)
  * fio iodepth flag fix “--io_depths”  (GH-495)
  * Fixes a race condition on AWS/Azure where networks in use were deleted (GH-506)
  * Added a note on how to fix dependencies when tox tests fail (GH-505)
  * Fixed  broken Apache project links (GH-492, GH-467)
  * Netperf timeout to work around occasional hangs (GH-483)
  * Fixed lock pickling issues (GH-471)

# v0.21.0

* New features:
  * Add support for OpenStack affinity and anti-affinity groups. (GH-440, thanks to @kivio)
  * Add large object scenario for CLI tests in object_storage_service. (GH-445)
  * Add support for pre-emptible GCE VMs. (GH-415)

* Enhancements:
  * Parallelize cassandra_ycsb and hbase_ycsb benchmark setups. (GH-435)
  * Add more config options for aerospike benchmark. (GH-450)

* Bugfixes and maintenance updates:
  * Refactor OpenStack network IP management. (GH-438, thanks to @kivio)
  * Fix thread lock pickling bug. (GH-425, thanks to @kivio)
  * Update jars used in cloud_bigtable_ycsb benchmark. (GH-444)
  * Update YCSB to v0.3.0. (GH-428)
  * Fix object_storage_service bug introduced by Azure breaking change. (GH-446)
  * Fix object storage CLI output format. (GH-449)

# v0.20.0

* Enhancements:
  * Specify project to gcloud alpha bigtable clusters list (GH-433)
  * More Samples in FIO (GH-416)

* Bugfixes and maintenance updates:
  * pkb label problem in OpenStack driver fixes (GH-410)
  * On exception, only cleanup if run stage is all/cleanup (GH-412)
  * Fix issue with using a flag before flags are parsed (GH-413)
  * Umount disk when running fio against raw device. (GH-417)
  * Clarify warnings from `ycsb._CombineResults`. (GH-432)

# v0.19.0

* New features:
  * New mysql_service benchmark. This benchmarks a cloud's managed MySQL
    offering using sysbench. (GH-387)

* Enhancements:
  * Added option to disable iptables if your image requires it (GH-361)
  * mongodb_ycsb now installs client and server dependencies in parallel,
    speeding up the end to end run time for the benchmark. (GH-402)
  * The netperf and iperf benchmarks now only add firewall rules if they are
    running over external ips. (GH-382)

* Bugfixes and maintenance updates:
  * The iperf package will now check the 'redhat-release' version and install
    directly from an RPM (this enables iperf to be run on Scientific Linux 6.x).
    (GH-392, thanks to @Vukasin92)
  * Fix bug where VM temporary directory wasn't created before use on RHEL based
    static VMs. (GH-389, thanks to @Vukasin92)
  * netperf package url changed since version 2.6.0 is now in archive/ (GH-390)
  * Fixed DigitalOcean package installation error (GH-396)
  * The object_storage_service benchmark no longer copies gcloud logs as part of copying
    the gcloud configuration to the VM. (GH-383)
  * Correctly cleanup network resources when run stages are used. (GH-386)
  * Added timeout to apt-get update command because it will occasionally hang.
    (GH-391)
  * Update copy_throughput benchmark so it works with
    ContainerizedVirtualMachines. (GH-408)
  * Install python inside ContainerizedVirtualMachines so that
    RobustRemoteCommand works on them. (GH-404)

* Benchmark-specific changes:
  * speccpu2006 will no longer report results if the run was incomplete. This
    behavior can be modified with a flag. (GH-397)
  * The mongodb benchmark has been completely removed since mongodb_ycsb
    replaced it with greater functionality. (GH-403)
  * The fio benchmark now has more latency percentiles included in sample
    metadata. (GH-399)
  * Cassandra version bumped up to 2.0.16 since 2.0.0 has known issues (GH-393)


# v0.18.0

(See also https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues/369
which includes this change log with clickable GH-* links.)

* New features:
  * Support OpenStack as cloud provider (GH-305, GH-353, thanks @kivio and
    @mateusz-blaszkowski)
  * Support Rackspace as cloud provider (GH-336, thanks @meteorfox and @jrperritt)
  * Add support for ContainerizedVM using docker exec (GH-333, thanks @gablg1)
  * Windows guest VM support on Static VM (GH-350), Azure (GH-349, GH-374), AWS
    (GH-347), and GCE (GH-338)
  * Add NTttcp Windows networking benchmark (GH-348)

* Enhancements:
  * Support using proxies in VMs (GH-339, GH-337, thanks @kivio)
  * Enable optional migration on GCE (GH-343)
  * Execute long running commands via a remote agent (GH-310)
  * Add resource creation/deletion times to logs (GH-316)

* Bugfixes and maintenance updates:
  * Update PKB to work with Azure version 0.9.3 (GH-312)
  * Fix AWS CLI usage on Windows host (GH-313)
  * Auto-fetch AMI IDs for AWS images (GH-364)
  * Fix publisher missing info for default image and machine type (GH-357)
  * Fix 'no attribute pkb_thread_log_context' error for sub-thread logs (GH-322)

* Benchmark-specific changes:
  * aerospike: config/flag handling bugfixes (GH-367, GH-360, GH-354)
  * cassandra_ycsb: move num_vms prerequisite check
  * fio: add latency percentiles for results (GH-344)
  * hadoop_terasort: Fix bad SSH option (GH-328)
  * iperf: add lower bounds to arguments (GH-314)
  * iperf: add timeout to parallel benchmark runs to handle iperf hangs (GH-375)
  * netperf: Support confidence intervals, increase test length, report stddev
    (GH-317, GH-306)
  * ycsb: Drop unaggregatable results from samples (GH-324)

* Development and testing:
  * **Breaking Change** Automated testing now uses `tox` (GH-330)
  * Refactored hook scripts, including new opt-in pre-push hook (GH-363)
  * Use travis for CI testing (GH-340)
  * Speed up tests using timeouts (GH-299)

* Internals:
  * Move defaults from benchmark_spec to VM classes, move network instantiation
    out of benchmark spec (GH-342)
  * Add event hook support (GH-315)
  * Refactor VM classes (GH-321)

# v0.17.0

* Add initial support for DigitalOcean as a cloud provider (GH-291).
* Add SciMark2 Benchmark (GH-271, thanks @zlim!).
* New NoSQL benchmarks: `cloud_bigtable_ycsb` (GH-283), `mongodb_ycsb`
  (GH-279), `cassandra_ycsb` (GH-278).
* Allow PerfKitBenchmarker to run on a Windows controller (GH-274).
* Add a `--dstat` flag, to collect performance metrics during benchmark runs
  (GH-282).
* Add support for parallel iperf clients to the iperf benchmark (GH-262).
* Add scratch disk information to sample metadata (GH-277).
* Add a 5 minute timeout to local commands (GH-289, GH-293).
* Do not use FLAG values in generating benchmark documentation (GH-280).
* Bump HBase to v1.0.1.1 (GH-281).
* Fix an issue preventing resources from being cleaned up (GH-276).

# v0.16.0

* **Breaking Change** Added a new scratch disk type: "local" (ephemeral
  storage bundled with the instance). As a result, the type "ssd" was
  changed to "remote_ssd". (GH-253)
* You can now omit "--run_uri" under certain circumstances. (GH-255)
* Support for striping multiple scratch disks together has been added. (GH-259)
* Add hbase_ycsb benchmark. (GH-263)
* Use the boto API differently when downloading a one byte object to avoid
  the unnecessary HEAD request (and issue GET request only). (GH-264)
* Move VM name to BaseVirtualMachine. (GH-265)
* Write an SSH config file to the run temp directory. (GH-266)
* Fix a typo in package_managers.py. (GH-267)
* Add a Cassandra package. (GH-268)
* GCE: do not restart on host failure. (GH-273)
* Fix Hash Sum mismatch errors. (GH-275)

# v0.15.0

* Add ability to inject environment variables to IssueCommand. (GH-231)
* More SPEC CPU 2006 execution flexibility. (GH-230)
* Enable different disk types for cassandra_stress test. (GH-252)
* Add the option to skip installing packages. (GH-223)
* Expose VM count and scratch disk requirements in help. (GH-240, GH-241)
* Add iops and against device flag. (GH-236)
* Netperf: Parse CSV output; add p50, p90, p99. (GH-222)
* Logging fixes and enhancements. (GH-68)
* Fixes some apt-get update flakiness. (GH-250)
* Updates AWS to use `_Exists` for resources. (GH-228)
* Fixes small bug wth GCP metadata. (GH-242)
* Call benchmark specific cleanup() when there is exception. (GH-239)
* Fix the command that creates a storage account for Azure. (GH-238)
* Defines BENCHMARK_INFO for all benchmarks. (GH-232)
* Fixes issue with retry. (GH-233)
* Make resource deletion continue even when exceptions are thrown. (GH-224)
* Fix a DivideByZeroError in side-by-side. (GH-226)

# v0.14.0

* Added the Silo filesystem benchmark (GH-170)
* Added Azure Blobs support to the object_storage_service_benchmark (GH-209)
* Logging now prints in color to make it easier to read (GH-212)
* Improvement to allow SSH port selection is static machine configs (GH-201)
* Bug fix for the filesystem workloads when using PIOPS (GH-190)
* Bug fixes for the side-by-side tool to use the right workload name. (GH-220)
* Bug fix for a race condition in an exception path that could leave resources behind (GH-214)

# v0.13.0

* Show metadata in StreamPublisher. (GH-178)
* **Breaking Change**: Use parser to extract fio benchmark results, and
  change metric name from job_name:bandwidth/latency to
  job_name:read/write:bandwidth/latency. (GH-192)
* Add unit test to check proper handling of scratch disk property. (GH-191)
* Fix bug where exceptions raised in a runthreaded thread may not have
  been raised after the threads had been joined. (GH-194)
* Stop mutating globals in GetInfo. (GH-198)
* **New Benchmark**: oldisim. (GH-200)
* Add oldisim benchmark to google_set and stanford_set.
* Add max_throughput_for_completion_latency_under_1ms metric to redis and
  aerospike benchmarks. (GH-203)

# v0.12.1

* Fix a tagging issue from prior release (v.0.11.0) where a prior commit made
  directly to master was not properly included.

# v0.12.0

* Add per-phase timestamp samples (GH171)
* Regex improvement (GH182)
* Fix a bug in mesh network benchmarks (GH163)
* Fix for running on AWS when user has a non-JSON output format
  configured(GH167)
* Clarify how to provide scratch disk for static VMs (GH166)
* Add benchmark name, run URI to log messages (GH157)
* Fix fio_benchmark to support static VMs (GH162)
* Optional patch to the unix bench for 16+ core VMs (GH 135)

# v0.11.1

* Fixed CHANGES.md

# v0.11.0

* Create Intel benchmark set (integrate from main v0.10.1)
* Support adding custom resource tags to AWS and GCE runs
* Limited hadoop terasort resource usage to 90% of the VM available memory
* Download and install package epel-release if not present
* Fixed dead link in README.md

# v0.10.0

* Various improvements to object_storage_service benchmarks. This includes
  reporting results in percentiles, adding a list consistency benchmark,
  and more. (GH-100 through GH110).
* Rename test_sample to sample_test. (GH-111)
* Updated the EPEL repo on RHEL and CentOS to use `epel-release` available
  through yum rather than a downloaded rpm. (GH-99)
* Added more benchmark sets

# v0.9.0

* **Breaking change**: removed `--json_output` flag. JSON samples are always
  written (GH-41).
* **Breaking change**: `object_storage_service` requires a `.boto` file
  configured for AWS and GCS to function.
* **Breaking change:** updated benchmark names to be more consistent (GH-72).
  Specific changes:

    + `cassandra` → `cassandra_stress`
    + `copy_benchmark` → `copy_throughput`
    + `fio_benchmark` → `fio`
    + `hadoop_benchmark` → `hadoop_terasort`
    + `mesh_benchmark` → `mesh_network`
    + `netperf_simple` → `netperf`
    + `object_storage_benchmark` → `object_storage_service`
    + `synthetic_storage_workloads_benchmark` → `block_storage_workload`
    + `sysbench_oltp_benchmark` → `sysbench_oltp`
    + `UnixBench_benchmark` → `unixbench`

* Added support for C4 instance types on EC2 (GH-63).
* Added support for specifying `--product_name` on the command line (GH-55).
* Added side-by-side comparison tool (GH-39, GH-61, GH-62).
* Factored out package management to support RHEL, CentOS (GH-54).
* Improved accuracy of cluster boot time (GH-69, GH-73).
* Introduced a class to represent performance samples (GH-71)
* Updated Hadoop benchmark to calculate per-core terasort throughput (GH-75).
* Added a results parser for bonnie++ benchmark (GH-70).
* Added a results parser for fio benchmark (GH-32).
* Added prerequisite checking to benchmarks (GH-49).
* Switch to Apache distribution of Cassandra (GH-92).
* Improved default behavior for machine types with no local storage (GH-88).
* Updated `object_storage_service` benchmark to test both command line tool
  performance and direct API calls (GH-59, GH-90).
* Added benchmark sets: predefined collections of benchmarks to run (GH-80).
* Modified HPCC benchmark to use 80% of available memory rather than 80% of
  total. Prevents crashes on low-memory systems (GH-81).
* Updated the default Azure image (GH-84).
* Improved the Cassandra stress benchmark to incorporate a user-specified
  number of rows, with defaults that run on all cloud platforms with default
  quotas (GH-31).
* Improved the Cassandra stress benchmark to incorporate a user-specified
  number of cassandra-stress threads on client node, with defaults of 50
  (originally default was 300 which caused the benchmark to crash on small
  instance types). As a result, on large instance types, the throughput
  reported by cassandra-stress tool is lower than previous version (GH-31).

# v0.8.0

* Documentation cleanup (GH-19, GH-34).
* Fix incorrect assignment of `ip_type` metadata in `netperf_simple` benchmark (GH-26).
* Added `--gcloud_scopes` flag, to support providing permissions to created instances on GCP.
* Changed GCP default image from `debian-7-backports` to `ubuntu-14-04`. All cloud providers now run Ubuntu 14.04 by default (GH-43).
* Added results parser for MongoDB (GH-36) and UnixBench++ (GH-45).
* Improved unit test coverage (GH-21).

# v0.7.1

* GCE VM SSH keys are now provided via a temporary file rather than the command
  line, which fixes a compatibility issue between versions of `gcloud` (GH-19).

# v0.7.0

* New benchmark: `aerospike` (GH-13).
* `iperf`: Run benchmark in both directions (VM A -> VM B and B -> A) (GH-7).
* `hadoop_benchmark`: Bump Hadoop to version 2.5.2 (GH-5).
* `synthetic_storage_workloads_benchmark`: Fix IO sizes passed to `fio`.
* Add a verbose log to `/tmp/perfkitbenchmarker/run_<run_uri>/perfkitbenchmarker.log`
  (exact file name announced to stderr at start of run) (GH-3).
* Merge `perfkitbenchmarker_lib` into `vm_util` (GH-9)
* Refactor result publishing and metadata collection (GH-10).
* Add a Google Cloud Storage publisher (GH-14)
* Change the default Azure machine type to "Small".
* Added unit tests.
* Style fixes.

# v0.6.0

Initial release under Apache 2.0 license.

## Unreleased

# v0.5.1

* Fix for HPCC result parser.
* Fix MySQL configuration in sysbench OLTP benchmark.

# v0.5

v0.5 contains primarily bugfixes and internal improvements.

New dependency:
`jinja2`. Install with `pip install jinja2`.

New Benchmarks:
* `synthetic_storage_workloads_benchmark`: new `fio` benchmarks to simulate
  logging, database and streaming workloads.

Fixes and usability improvements:

* Bugfixes in Cassandra benchmark: variable redefinition, invalid method name.
* Add default config file for Cassandra benchmark.
* Add an `--ip_addresses` flag for networking benchmarks
* Replace sed with jinja2 templates for Hadoop configuration.
* Make the default image Debian backports for GCE.
* Add scratch disks for static VMs.
* No longer specifying absolute path to azure.
* Fixe a bug preventing AWS t2 types from working.
* Add local drives.
* Run scp copy benchmark on internal IP if accessible.
* Give immediate feedback on an exception during an Artemis run.
* Add placement groups for AWS.
* Add a method to burn cpu and dirty cache.
* Standardize SCP zone metadata to match iperf, netperf

# v0.4

New Benchmarks:
* Add a Cassandra benchmark.

New features of note:
* Static VM files are now JSON format, support the optional zone specification.

Fixes:

* Fix coremark compilation on Ubuntu 14.04.
* Do not include network creation time in VM creation time.
* Set GCE VMs to terminate on host maintenance rather than live migrating.
* Enable DNS hostnames on AWS.
* Fixed configuration error in Hadoop benchmark.
* Fixed an error in dd benchmark.

# v0.3

New Benchmarks:

New features of note:
Resource tagging -
* Add a "user" tag defaulting to the logged in user name to GCE and EC2 VMs.

Fixes:
* VMs, Networks, and disks now inherit from resource.BaseResource for uniform resource lifecycle management.
* Update object_storage_benchmark match latest CLI from all providers; retrieve credentials file from default location for each cloud provider.
* Add __setstate__ and __getstate__ methods so that pickling of GCE and AWS firewalls works again
* Exit with an error when invalid benchmark names are specified.
* Add support for running hadoop_benchmark on EC2.
* Upgrade from Ubuntu 12.04 LTS (Precise) to 14.04 LTS (Trusty) on AWS and Azure.
* Fix AzureDisk to be compatible with the latest version of the CLI

# v0.2
* Added end to end run time metric.
* Renamed `storage_benchmark` to `object_storage_benchmark`.
* Embedded PerfKitBenchmarker version in published metadata.
* Improved help message in PerfKitBenchmarker.
* Allow PerfKitBenchmarker to use multiple types of VMs and multiple disks with different sizes in a single benchmark.
* Fixed iperf parsing regex results
* Cleaned up versioning - Checked out specific versions from git in the MongoDB, OpenBLAS, Redis, and storage benchmarks.
* Fixed corner cases in SPEC 2006 causing it to fail on large instances.

# v0.1
* Support static vms (i.e. machine not provisioned via Cloud APIs. We call all machines VMs). All static VMs provided will be used before any non-static VMs are provisioned.
* See static_virtual_machine.py for detailed description.
* Added copy benchmark.
* Added storage benchmark.
* Added ping benchmark.
* Added SpecCPU2006 benchmark.

# v0.0

Initial release.
