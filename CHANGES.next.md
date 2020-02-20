### Breaking changes:
- Remove `ubuntu1404` and `debian` OS types.
  - Ubuntu 14.04 is now in LTS and has not been fully maintained since April
    2019.
  - `debian` os_type alias referred to Ubuntu 14.04 on all clouds.
  - Ubuntu 16.04 is now the default.
- Deprecate versionless `windows` os_type.
  - It will stop working in April 2020, and you will have to specify
      `windows2012`.
- Deprecate versionless `rhel` os_type.
  - It will stop working in April 2020, and you will have to specify
      `rhel7` on most providers or `amazonlinux1` on AWS.

### New features:

- Add infiniband support in nccl benchmark.
- Added AwsVpcS3Endpoint for VPC connectivity to S3.
- Add `--tcp_max_receive_buffer` flag to set net.ipv4.tcp_rmem sysctl value
- Add `--tcp_max_send_buffer` flag to set net.ipv4.tcp_wmem sysctl value
- Add `--rmem_max` flag to set net.core.rmem_max sysctl value
- Add `--wmem_max` flag to set net.core.wmem_max sysctl value
- Add `--os_type=debian9` support for AWS and Azure Providers.
- Add `--os_type=debian10` support for GCP, AWS and Azure Providers.
- Add function to tune NCCL parameters.
- Add Coremark demo scripts at PerfKitBenchmarker/tools/demos/coremark.
- Add RHEL 8 to AWS, Azure and GCP providers
- Add CentOS 8 to Azure and GCP providers

### Enhancements:

-   Update Specsfs2014 to use the SP2 update rather than SP1.

### Bug fixes and maintenance updates:

-   AWSBaseVirtualMachine subclasses require IMAGE_OWNER and IMAGE_NAME_FILTER.
    Fixes issue where some windows 2012 AMIs were selected from the wrong
    project.
-   Cassandra build uses HTTPS maven repository instead of HTTP
-   Retry `yum install` commands to bypass transient issues.
-   Azure defaults to no placement group created.
-   Add Sql Server support to managed relational DB
-   Remove `py27` tox tests.
-   Update AKS cluster creation service principle handling, which was broken
-   Update glibc version to 2.31.
