### Breaking changes:
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
- Changed TF Serving benchmark to use ResNet instead of Inception

### New features:
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
- Added disk_fill_size and prepare_sleep_time flags
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

### Enhancements:
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
- Add more granularity to FAILED benchmarks with FailedSubstatus (GCP and AWS).
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
- Add support for --nouse_pkb_logging to use standard ABSL logging instead.

### Bug fixes and maintenance updates:
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
- Made failures of 'aws ec2 run-instances' fail PKB quickly.
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
- Add --project flag to GoogleCloudStorageService MakeBucket command.
- Fix virtual_machine.GetResourceMetadata() so that it does not try to gather
  metadata from a VM that has not been provisioned yet.
