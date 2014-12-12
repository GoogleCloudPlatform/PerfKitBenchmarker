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
