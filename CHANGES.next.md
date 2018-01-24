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

### New features:
- Windows benchmarks can now be run from linux controllers
- MXNet benchmarks can now be run from linux controllers
- Added initial support for preprovisioning benchmark binaries in the cloud.

### Enhancements:
- Support for ProfitBricks API v4:
  - Add `profitbricks_image_alias` flag and support for image aliases
  - Add new location, `us/ewr`
- Add `aws_image_name_filter` flag to ease specifying images.
- Add c5/m5 support for NVME disks.
- Add MNIST benchmark support for TPU, CPU and GPU
- Created KubernetesPodSpec which allows the user to specify kubernetes resource
  requests and limits, including fractional CPUs.
- Add `skip_pending_runs_file` flag to workaround SIGINT issues.
- Add support for `specsfs2014_load` parameter as an integer list.
- Publishers can be extended through external modules.
- Add `run_processes_delay` flag to stagger parallel runs.
- Add support for SPECspeed.
- Add new `os_types` Ubuntu1404 and Ubuntu1604.
- Make it easier to RDP to PKB VMs
- Avoid setting up thread pool etc when run_processes is set
  to 1 to simplify debugging using --run_with_pdb flag
- Added a sample benchmark for descriptive purposes.
- Added GPU peer to peer topology information to metadata.
- Added a flag, hpcg_run_as_root which allows OpenMPI to run HPCG in the case
  that the user is root.
- Modified KubernetesVirtualMachine to ensure that ssh is installed on the
  container.
- Added container_cluster_num_vms flag.
- Added the cluster's worker shape and worker count into the dpb service metadata.

### Bug fixes and maintenance updates:
- Moved GPU-related specs from GceVmSpec to BaseVmSpec
- Fix ProfitBricks issue with extra `/` in the API url
- Fix ProfitBricks volume availability zone issue
- Bulk AllowPort restored.
- Moved CustomMachineTypeSpec and related decoders to their own module
- Updated GKE engine version to 1.8.6-gke.0 when using a GPU-accelerated cluster
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
