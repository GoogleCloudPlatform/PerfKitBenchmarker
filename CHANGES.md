# v0.16.0

* **Breaking Change** Added a new scratch disk type: "local" (ephemeral
  storage bundled with the instance). As a result, the type "ssd" was
  changed to "remote_ssd". (GH-253)
* You can now omit "--run_uri" under certain circumstances. (GH-255)
* Support for striping multiple scratch disks together has been added. (GH-259)
* Add hbase_ycsb benchmark. (GH-263)
* Use the boto API differently when downloading a one byte object to avoid
  the unnecessary HEAD request (and issue GET request only). (GH-264)
* Move VM name to BaseVirtualMachine. (GH-265)
* Write an SSH config file to the run temp directory. (GH-266)
* Fix a typo in package_managers.py. (GH-267)
* Add a Cassandra package. (GH-268)
* GCE: do not restart on host failure. (GH-273)
* Fix Hash Sum mismatch errors. (GH-275)

# v0.15.0

* Add ability to inject environment variables to IssueCommand. (GH-231)
* More SPEC CPU 2006 execution flexibility. (GH-230)
* Enable different disk types for cassandra_stress test. (GH-252)
* Add the option to skip installing packages. (GH-223)
* Expose VM count and scratch disk requirements in help. (GH-240, GH-241)
* Add iops and against device flag. (GH-236)
* Netperf: Parse CSV output; add p50, p90, p99. (GH-222)
* Logging fixes and enhancements. (GH-68)
* Fixes some apt-get update flakiness. (GH-250)
* Updates AWS to use _Exists for resources. (GH-228)
* Fixes small bug wth GCP metadata. (GH-242)
* Call benchmark specific cleanup() when there is exception. (GH-239)
* Fix the command that creates a storage account for Azure. (GH-238)
* Defines BENCHMARK_INFO for all benchmarks. (GH-232)
* Fixes issue with retry. (GH-233)
* Make resource deletion continue even when exceptions are thrown. (GH-224)
* Fix a DivideByZeroError in side-by-side. (GH-226)

# v0.14.0

* Added the Silo filesystem benchmark (GH-170)
* Added Azure Blobs support to the object_storage_service_benchmark (GH-209)
* Logging now prints in color to make it easier to read (GH-212)
* Improvement to allow SSH port selection is static machine configs (GH-201)
* Bug fix for the filesystem workloads when using PIOPS (GH-190)
* Bug fixes for the side-by-side tool to use the right workload name. (GH-220)
* Bug fix for a race condition in an exception path that could leave resources behind (GH-214)

# v0.13.0

* Show metadata in StreamPublisher. (GH-178)
* **Breaking Change**: Use parser to extract fio benchmark results, and
  change metric name from job_name:bandwidth/latency to
  job_name:read/write:bandwidth/latency. (GH-192)
* Add unit test to check proper handling of scratch disk property. (GH-191)
* Fix bug where exceptions raised in a runthreaded thread may not have
  been raised after the threads had been joined. (GH-194)
* Stop mutating globals in GetInfo. (GH-198)
* **New Benchmark**: oldisim. (GH-200)
* Add oldisim benchmark to google_set and stanford_set.
* Add max_throughput_for_completion_latency_under_1ms metric to redis and
  aerospike benchmarks. (GH-203)

# v0.12.1

* Fix a tagging issue from prior release (v.0.11.0) where a prior commit made
  directly to master was not properly included.

# v0.12.0

* Add per-phase timestamp samples (GH171)
* Regex improvement (GH182)
* Fix a bug in mesh network benchmarks (GH163)
* Fix for running on AWS when user has a non-JSON output format
  configured(GH167)
* Clarify how to provide scratch disk for static VMs (GH166)
* Add benchmark name, run URI to log messages (GH157)
* Fix fio_benchmark to support static VMs (GH162)
* Optional patch to the unix bench for 16+ core VMs (GH 135)

# v0.11.1

* Fixed CHANGES.md

# v0.11.0

* Create Intel benchmark set (integrate from main v0.10.1)
* Support adding custom resource tags to AWS and GCE runs
* Limited hadoop terasort resource usage to 90% of the VM available memory
* Download and install package epel-release if not present
* Fixed dead link in README.md

# v0.10.0

* Various improvements to object_storage_service benchmarks. This includes
  reporting results in percentiles, adding a list consistency benchmark,
  and more. (GH-100 through GH110).
* Rename test_sample to sample_test. (GH-111)
* Updated the EPEL repo on RHEL and CentOS to use `epel-release` available
  through yum rather than a downloaded rpm. (GH-99)
* Added more benchmark sets

# v0.9.0

* **Breaking change**: removed `--json_output` flag. JSON samples are always
  written (GH-41).
* **Breaking change**: `object_storage_service` requires a `.boto` file
  configured for AWS and GCS to function.
* **Breaking change:** updated benchmark names to be more consistent (GH-72).
  Specific changes:

    + `cassandra` → `cassandra_stress`
    + `copy_benchmark` → `copy_throughput`
    + `fio_benchmark` → `fio`
    + `hadoop_benchmark` → `hadoop_terasort`
    + `mesh_benchmark` → `mesh_network`
    + `netperf_simple` → `netperf`
    + `object_storage_benchmark` → `object_storage_service`
    + `synthetic_storage_workloads_benchmark` → `block_storage_workload`
    + `sysbench_oltp_benchmark` → `sysbench_oltp`
    + `UnixBench_benchmark` → `unixbench`

* Added support for C4 instance types on EC2 (GH-63).
* Added support for specifying `--product_name` on the command line (GH-55).
* Added side-by-side comparison tool (GH-39, GH-61, GH-62).
* Factored out package management to support RHEL, CentOS (GH-54).
* Improved accuracy of cluster boot time (GH-69, GH-73).
* Introduced a class to represent performance samples (GH-71)
* Updated Hadoop benchmark to calculate per-core terasort throughput (GH-75).
* Added a results parser for bonnie++ benchmark (GH-70).
* Added a results parser for fio benchmark (GH-32).
* Added prerequisite checking to benchmarks (GH-49).
* Switch to Apache distribution of Cassandra (GH-92).
* Improved default behavior for machine types with no local storage (GH-88).
* Updated `object_storage_service` benchmark to test both command line tool
  performance and direct API calls (GH-59, GH-90).
* Added benchmark sets: predefined collections of benchmarks to run (GH-80).
* Modified HPCC benchmark to use 80% of available memory rather than 80% of
  total. Prevents crashes on low-memory systems (GH-81).
* Updated the default Azure image (GH-84).
* Improved the Cassandra stress benchmark to incorporate a user-specified
  number of rows, with defaults that run on all cloud platforms with default
  quotas (GH-31).
* Improved the Cassandra stress benchmark to incorporate a user-specified
  number of cassandra-stress threads on client node, with defaults of 50
  (originally default was 300 which caused the benchmark to crash on small
  instance types). As a result, on large instance types, the throughput
  reported by cassandra-stress tool is lower than previous version (GH-31).

# v0.8.0

* Documentation cleanup (GH-19, GH-34).
* Fix incorrect assignment of `ip_type` metadata in `netperf_simple` benchmark (GH-26).
* Added `--gcloud_scopes` flag, to support providing permissions to created instances on GCP.
* Changed GCP default image from `debian-7-backports` to `ubuntu-14-04`. All cloud providers now run Ubuntu 14.04 by default (GH-43).
* Added results parser for MongoDB (GH-36) and UnixBench++ (GH-45).
* Improved unit test coverage (GH-21).

# v0.7.1

* GCE VM SSH keys are now provided via a temporary file rather than the command
  line, which fixes a compatibility issue between versions of `gcloud` (GH-19).

# v0.7.0

* New benchmark: `aerospike` (GH-13).
* `iperf`: Run benchmark in both directions (VM A -> VM B and B -> A) (GH-7).
* `hadoop_benchmark`: Bump Hadoop to version 2.5.2 (GH-5).
* `synthetic_storage_workloads_benchmark`: Fix IO sizes passed to `fio`.
* Add a verbose log to `/tmp/perfkitbenchmarker/run_<run_uri>/perfkitbenchmarker.log`
  (exact file name announced to stderr at start of run) (GH-3).
* Merge `perfkitbenchmarker_lib` into `vm_util` (GH-9)
* Refactor result publishing and metadata collection (GH-10).
* Add a Google Cloud Storage publisher (GH-14)
* Change the default Azure machine type to "Small".
* Added unit tests.
* Style fixes.

# v0.6.0

Initial release under Apache 2.0 license.

## Unreleased

# v0.5.1

* Fix for HPCC result parser.
* Fix MySQL configuration in sysbench OLTP benchmark.

# v0.5

v0.5 contains primarily bugfixes and internal improvements.

New dependency:
`jinja2`. Install with `pip install jinja2`.

New Benchmarks:
* `synthetic_storage_workloads_benchmark`: new `fio` benchmarks to simulate
  logging, database and streaming workloads.

Fixes and usability improvements:

* Bugfixes in Cassandra benchmark: variable redefinition, invalid method name.
* Add default config file for Cassandra benchmark.
* Add an `--ip_addresses` flag for networking benchmarks
* Replace sed with jinja2 templates for Hadoop configuration.
* Make the default image Debian backports for GCE.
* Add scratch disks for static VMs.
* No longer specifying absolute path to azure.
* Fixe a bug preventing AWS t2 types from working.
* Add local drives.
* Run scp copy benchmark on internal IP if accessible.
* Give immediate feedback on an exception during an Artemis run.
* Add placement groups for AWS.
* Add a method to burn cpu and dirty cache.
* Standardize SCP zone metadata to match iperf, netperf

# v0.4

New Benchmarks:
* Add a Cassandra benchmark.

New features of note:
* Static VM files are now JSON format, support the optional zone specification.

Fixes:

* Fix coremark compilation on Ubuntu 14.04.
* Do not include network creation time in VM creation time.
* Set GCE VMs to terminate on host maintenance rather than live migrating.
* Enable DNS hostnames on AWS.
* Fixed configuration error in Hadoop benchmark.
* Fixed an error in dd benchmark.

# v0.3

New Benchmarks:

New features of note:
Resource tagging -
* Add a "user" tag defaulting to the logged in user name to GCE and EC2 VMs.

Fixes:
* VMs, Networks, and disks now inherit from resource.BaseResource for uniform resource lifecycle management.
* Update object_storage_benchmark match latest CLI from all providers; retrieve credentials file from default location for each cloud provider.
* Add __setstate__ and __getstate__ methods so that pickling of GCE and AWS firewalls works again
* Exit with an error when invalid benchmark names are specified.
* Add support for running hadoop_benchmark on EC2.
* Upgrade from Ubuntu 12.04 LTS (Precise) to 14.04 LTS (Trusty) on AWS and Azure.
* Fix AzureDisk to be compatible with the latest version of the CLI

# v0.2
* Added end to end run time metric.
* Renamed `storage_benchmark` to `object_storage_benchmark`.
* Embedded PerfKitBenchmarker version in published metadata.
* Improved help message in PerfKitBenchmarker.
* Allow PerfKitBenchmarker to use multiple types of VMs and multiple disks with different sizes in a single benchmark.
* Fixed iperf parsing regex results
* Cleaned up versioning - Checked out specific versions from git in the MongoDB, OpenBLAS, Redis, and storage benchmarks.
* Fixed corner cases in SPEC 2006 causing it to fail on large instances.

# v0.1

* Support static vms (i.e. machine not provisioned via Cloud APIs. We call all machines VMs). All static VMs provided will be used before any non-static VMs are provisioned.
* See static_virtual_machine.py for detailed description.
* Added copy benchmark.
* Added storage benchmark.
* Added ping benchmark.
* Added SpecCPU2006 benchmark.

# v0.0

Initial release.
