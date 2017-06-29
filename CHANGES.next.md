Breaking changes:
-

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

Bug fixes and maintenance updates:
- Fixed provision phase of memcached_ycsb benchmark for non-managed memcached instances (GH-1384)
- Fixed GPU benchmarks on GCE to work with new GPU API (GH-1407)
