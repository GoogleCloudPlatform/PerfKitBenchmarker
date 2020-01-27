### Breaking changes:
- Remove `ubuntu1404` and `debian` OS types.
  - Ubuntu 14.04 is now in LTS and has not been fully maintained since April
    2019.
  - `debian` os_type alias referred to Ubuntu 14.04 on all clouds.
  - Ubuntu 16.04 is now the default.
- Deprecate versionless `windows` os_type.
  - It will stop working in April 2020, and you will have to specify
      `windows2012`.

### New features:
- Add infiniband support in nccl benchmark.
- Added AwsVpcS3Endpoint for VPC connectivity to S3.
- Add `--os_type=debian-9` support for AWS and Azure Providers.
- Add `--os_type=debian-10` support for GCP, AWS and Azure Providers.

### Enhancements:

-   Update Specsfs2014 to use the SP2 update rather than SP1.

### Bug fixes and maintenance updates:

-   AWSBaseVirtualMachine subclasses require IMAGE_OWNER and IMAGE_NAME_FILTER.
    Fixes issue where some windows 2012 AMIs were selected from the wrong
    project.
-   Cassandra build uses HTTPS maven repository instead of HTTP
-   Retry `yum install` commands to bypass transient issues.
