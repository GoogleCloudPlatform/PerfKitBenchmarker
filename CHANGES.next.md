Breaking changes:
- Removed gpus_per_node metadata from stencil2d and hpcg benchmarks (GH-1455)

New features:
- Added YCSB benchmark for Cloud Spanner (GH-1387 from @haih-g)
- Added CUDA-enabled HPCG benchmark (GH-1395)
- Added ManagedRelationalDatabase base classes and tests (GH-1405)
- Added gpus to GceVmSpec. This is now the only way to create VMs with gpus on GCE due to a gcloud API change (GH-1406)
- Added flag_zip_defs which functions like flag_matrix_defs, but performs a zip
  operation on the axes instead of a cross-product (GH-1414)
- Added TensorFlow Benchmarks. (GH-1420)

Enhancements:
- Added basic auth support for Mesos provider. (GH-1390)
- Added --failed_run_samples_error_length flag to limit failed run error length (GH-1391)
- Added `__eq__` and `__ne__` to IntegerList (GH-1395)
- Added total_free_memory_kb to VirtualMachine class and implemented it for
  Linux vms (GH-1397)
- Created hpc_util for a place to share common HPC functions
- Added --runspec_estimate_spec flag to calculate an estimated spec score (GH-1401)
- Added support for P100 gpus on Google Cloud Platform (GH-1450)
- Added gpu type to cuda_toolkit_8 metadata (GH-1453)
- Added num_gpus to cuda_toolkit_8 metadata (GH-1455)
- Added range mode to gpu_pcie_bandwidth which calculates average bandwidth over
  a provided range of transfer sizes (GH-1482)
- Added size support to fio_benchmark scenarios (GH-1489)

Bug fixes and maintenance updates:
- Fixed provision phase of memcached_ycsb benchmark for non-managed memcached instances (GH-1384)
- Fixed GPU benchmarks on GCE to work with new GPU API (GH-1407)
- Changed cuda_toolkit_8 to use the gpu-specific base clock speed as the default (GH-1453)
- Changed default AWS P2 region for gpu benchmarks (GH-1454)
- Fix multi-threading issue in fio.job filename (GH-1474)
- Decreased speccpu2006 default disk size to 50gb (GH-1484)
- Added azure_host_caching to metadata (GH-1500)
