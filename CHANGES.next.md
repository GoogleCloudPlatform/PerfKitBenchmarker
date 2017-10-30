###Breaking changes:
- Replaced gflags with absl-py. (GH-1518)
- Renamed GPU-related flags like so (old_flag_value -> new_flag_value):
  - gce_gpu_count -> gpu_count
  - gce_gpu_type -> gpu_type
- Replaced object_storage_objects_written_file* flags with prefix versions.

###New features:
- Windows benchmarks can now be run from linux controllers

###Enhancements:

###Bug fixes and maintenance updates:
- Moved GPU-related specs from GceVmSpec to BaseVmSpec

