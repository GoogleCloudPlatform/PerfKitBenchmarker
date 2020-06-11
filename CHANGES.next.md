### Breaking changes:

-   Remove `ubuntu1404` and `debian` OS types.
    -   Ubuntu 14.04 is now in LTS and has not been fully maintained since
        April 2019.
    -   `debian` os_type alias referred to Ubuntu 14.04 on all clouds.
    -   Ubuntu 16.04 is now the default.
-   Remove the `Runtime` metric for Horovod.
-   Change `windows201X` os_types to `windows201X_core` and `windows201X_base`
    to `windows201X_desktop`.
    -   Windows Server Core is now referred to as `windows201X_core`.
        -   This is recommended for most benchmarks, because PKB does not
            benefit from GUIs except for debugging.
    -   Windows Server with Desktop Experience is now referred to as
        `windows201X_desktop`.
    -   See
        [the microsoft documentation](https://docs.microsoft.com/en-us/windows-server/administration/server-core/what-is-server-core)
        for more information on the differences.
-   Remove deprecated versionless `windows` os_type.
    -   You will have to specify `windows2012_core`.
-   Removed deprecated versionless `rhel` os_type.
    -   You will have to specify `rhel7` on most providers or `amazonlinux1` on
        AWS.
-   Deprecate Amazon Linux 1 `amazonlinux1` os_type.
    -   AL1 is EOL on 2021-01-01:
        https://aws.amazon.com/blogs/aws/update-on-amazon-linux-ami-end-of-life/
        at which point it will be removed from PKB.
    -   You can use the recommended `amazonlinux2` instead.
-   Remove `ping_also_run_using_external_ip` from ping benchmark
    -   You should now use the `ip_addresses` flag to specify whether to test
        with internal IPs, external IPs or both. This brings ping into alignment
        with how other network benchmarks function.
-   The flag `--emr-release-label` is now deprecated in favor of setting
    `dpb_service.version`.
-   In EMR DPB benchmarks setting: `dpb_service.version` is now Required.

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
-   Add GceNfsService to support Filestore on GCP.
-   Add maven package and use it in ycsb.
-   Add replication cluster support to bigtable.
-   Add AWS and Azure VPC peering support
-   Add Snowflake Warehouse support
-   Add large_scale_boot benchmark for linux and windows.
-   Add Python client library for GCS alongside Boto connection library.
-   Enabled OpenMPI version selection
-   Adds support for running network benchmarks across a VPN.
    -   Includes GCP implementation with static routes.
-   Add a directory for PKB tutorials.
-   Add two new tutorials: one for beginners and one for network dashboard.
-   Update Boto client library for S3 to boto3.
-   Add MultiStreamDelete to Object Storage API Tests.
-   Add GCE Placement Group support. By default, placement groups are not
    created.
-   Add ICMP ping support for Azure using EXTERNAL ip address.

### Enhancements:

-   Update Specsfs2014 to use the SP2 update rather than SP1.
-   Update Multichase version to enable AARCH64 build support.
-   Added flag `--cassandra_maven_repo_url` to use a maven repo when building
    cassandra, for example
    https://maven-central.storage-download.googleapis.com/maven2
-   Optimize Azure Files settings.
-   Migrate EKS cluster creation to use eksctl (requires a local eksctl).
    -   Deprecate `--eks_zones` in favor of setting
        `container_cluster.vm_spec.AWS.zone`.
    -   Make `container_cluster.vm_spec.AWS.zone` accept a region or comma
        separated list of zones.
    -   Delete the obsolete `--eks-verify-ssl`
-   Separate NVIDIA driver functionality from CUDA toolkit
-   Added --runspec_tar option to SpecCPU (specifying installation tarball).
-   Added generic support for using `PreprovisionedData`
    -   Added the `preprovision_ignore_checksum` flag that can be used when
        downloading content that is not registered in `PREPROVISIONED_DATA`
    -   Allow `_InstallData` in virtual_machine.py to catch the `NotImplemented`
        error, in order to proceed to downloading resources via their URL
-   Implemented `PreprovisionedData` in the Maven package and make Maven more
    robust
    -   Added `maven_mirror_url` to specify a custom Maven mirror
    -   Added sha256 checksums for a few Maven versions
    -   Made retrieving JAVA_HOME more robust
    -   Fixed a java lib issue on CentOS
-   Add OpenFOAM support for timing individual commands.
-   Added --gce_subnet_name to specify existing GCE subnet to use.
-   Run a Full Sweep (Create/Stat/Delete) on Mdtest when not dropping caches.
-   Added `version` field to `dpb_service` configs. This corresponds to image
    version in Google Cloud Dataproc and release label in AWS EMR.
-   Support delete timing on provider implementations of object_storage_api
    script
-   Enable support for AWS's m6g family.
-   Add SPEC CPU 2017 flag allowing to build but not run a suite.
-   Enable support for specifying newer versions of GCC than what the OS
    supports by default.
-   Sort AWS AMIs by CreationDate instead of name to get latest image.
-   Add Spec17 configs compatible with v1.1.
-   Better support for pytyping.
    -   Make BaseVirtualMachine inherit from BaseOsMixin.
-   Add connect_via_internal_ip flag for testing windows tests over the internal
    IP, similar to the ssh_via_internal_ip flag for linux.
    -   Deprecate `--ssh_via_internal_ip` in favor of this new flag.

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
-   Do not try to use the GCP service account that is running PKB inside a GKE
    cluster if it obviously belongs to the wrong project.
-   Upgrade psutil version to 5.6.6 from 3.0.0.
-   Change default ior script to use 1GB sizes rather than 4GB.
-   Remove nfs_gce_ip_range from filestore options.
-   Upgrade YCSB to version 0.17.0.
-   Do not pass a default `--boot-disk-size` or `--boot-disk-type` to GCE.
    -   This allows GCE to set the preferred options for both.
    -   Fixes an issue where CentOS and RHEL images grew beyond the default 10GB
        (to 20GB).
    -   The current defaults leave other Linux OSes at 10GB and Windows OSes at
        50GB.
-   Explicitly setting Netperf to Python 3.
-   Update PKB cloud datastore ycsb benchmark:
    -   change private_keyfile_dir from a constant value to a FLAG
    -   delete all db entries as part of Cleanup
-   Remove 'default' keyword from AWS and Azure boot_disk_size
-   Check ycsb proportion explicitly against none.
-   Fix issue in calculating Geometric means.
-   Add support to PKB cloud datastore ycsb benchmark to use GCS files for
    datastore keyfiles.
-   Add SSH keys to dpb EMR clusters for debuggability.
    -   Security groups of the clusters will have to be manually edited to allow
        SSH.
-   Cast sample values to floats, ensuring they conform to the specification
-   Improve database deletion performance of Cloud datastore YCSB benchmark.
-   After installing the python package tries to set default version of python
    if not set.
-   Add check in Prepare stage of PKB cloud datastore ycsb benchmark to ensure
    db is empty before running.
-   Categorize k8s cluster creation errors better.
-   Improve database deletion performance of Cloud datastore YCSB benchmark.
-   Change deletion batch size of Cloud datastore YCSB benchmark.
