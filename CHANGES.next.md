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

### New features:
- Windows benchmarks can now be run from linux controllers.
- MXNet benchmarks can now be run from linux controllers.
- Added initial support for preprovisioning benchmark binaries in the cloud,
  if binaries are not located in local /data directory.
- YCSB benchmark for Cloud Redis in GCP, Elasticache Redis in AWS, and
  Redis Cache in Azure.
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
- Avoid setting up thread pool etc when run_processes is set
  to 1 and using --run_with_pdb flag to simplify debugging.
- Added a sample benchmark for descriptive purposes.
- Added GPU peer to peer topology information to metadata.
- Added a flag, `hpcg_run_as_root` which allows OpenMPI to run HPCG in the case
  that the user is root.
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
- Updated AWS spot instance creation and added
  spot instance failure metadata support.
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
