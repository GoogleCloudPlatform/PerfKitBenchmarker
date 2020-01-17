### Breaking changes:
- Remove `ubuntu1404` and `debian` OS types.
  - Ubuntu 14.04 is now in LTS and has not been fully maintained since April
    2019.
  - `debian` os_type alias referred to Ubuntu 14.04 on all clouds.
  - Ubuntu 16.04 is now the default.

### New features:
- Add infiniband support in nccl benchmark.

### Enhancements:

### Bug fixes and maintenance updates:
- AWSBaseVirtualMachine subclasses require IMAGE_OWNER and IMAGE_NAME_FILTER.
  Fixes issue where some windows 2012 AMIs were selected from the wrong
  project.
