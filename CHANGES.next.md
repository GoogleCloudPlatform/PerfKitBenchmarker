### Breaking changes:

-   Remove `ubuntu1404` and `debian` OS types.
    -   Ubuntu 14.04 is now in LTS and has not been fully maintained since
        April 2019.
    -   `debian` os_type alias referred to Ubuntu 14.04 on all clouds.
    -   Ubuntu 16.04 is now the default.
-   Deprecate versionless `windows` os_type.
    -   It will stop working in April 2020, and you will have to specify
        `windows2012`.
-   Deprecate versionless `rhel` os_type.
    -   It will stop working in April 2020, and you will have to specify `rhel7`
        on most providers or `amazonlinux1` on AWS.
-   Remove the `Runtime` metric for Horovod.

### New features:

-   Add infiniband support in nccl benchmark.
-   Added AwsVpcS3Endpoint for VPC connectivity to S3.
-   Add `--tcp_max_receive_buffer` flag to set net.ipv4.tcp_rmem sysctl value
-   Add `--tcp_max_send_buffer` flag to set net.ipv4.tcp_wmem sysctl value
-   Add `--rmem_max` flag to set net.core.rmem_max sysctl value
-   Add `--wmem_max` flag to set net.core.wmem_max sysctl value
-   Add `--os_type=debian9` support for AWS and Azure Providers.
-   Add `--os_type=debian10` support for GCP, AWS and Azure Providers.
-   Add function to tune NCCL parameters.
-   Add Coremark demo scripts at PerfKitBenchmarker/tools/demos/coremark.
-   Add RHEL 8 to AWS, Azure and GCP providers
-   Add CentOS 8 to Azure and GCP providers
-   Add BERT models in Horovod benchmark
-   Add timeline support in Horovod benchmark

### Enhancements:

-   Update Specsfs2014 to use the SP2 update rather than SP1.
-   Update Multichase version to enable AARCH64 build support.
-   Added flag '--cassandra_maven_repo_url' to use a maven repo when building
    cassandra, for example https://maven-central.storage-download.googleapis.com
    /maven2

### Bug fixes and maintenance updates:

-   AWSBaseVirtualMachine subclasses require IMAGE_OWNER and IMAGE_NAME_FILTER.
    Fixes issue where some windows 2012 AMIs were selected from the wrong
    project.
-   Retry `yum install` commands to bypass transient issues.
-   Azure defaults to no placement group created.
-   Add Sql Server support to managed relational DB
-   Remove `py27` tox tests.
-   Update AKS cluster creation service principle handling, which was broken
-   Update glibc version to 2.31.
-   Propagate Azure credentials to AKS when running as a service principal. This
    is consistent with propagating VM credentials in azure_credentials.py.
    -   This can be disabled by setting
        `--bootstrap_azure_service_principal=False`.
-   Registered AWS VPC quota failures as quota failures.
-   Do not try to use the GCP service account that is running PKB inside a
    GKE cluster if it obviously belongs to the wrong project.
