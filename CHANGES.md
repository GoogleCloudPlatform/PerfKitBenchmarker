# 1.5.0

New features:
* Add cloudsuite graph analytics benchmark (thanks @mdrumond; GH-986)
* Add initial implementation of multichase benchmark (GH-977)
* Add cloudsuite media streaming benchmark (thanks @ivonindza; GH-993)
* Add analysis functions for processing multistream object storage benchmark data (GH-905)
* Add CloudSuite Data-caching benchmark (thanks @neo-apz; GH-970)
* Add ability to bundle PKB into a python-executable zip file (GH-971)
* Add os_type for Juju Mixin (thanks @AdamIsrael; GH-764)
* Add CloudSuite in-memory analytics benchmark (thanks @ivonindza; GH-934)

Breaking changes:
* Replace the old CloudSuite Web Search benchmark with the docker version (thanks @javpicorel; GH-931)
* Update Aerospike version (GH-978)

Enhancements:
* Add --fio_blocksize option (GH-943)
* Update speccpu2006 benchmark to support iso file (GH-944)
* Allow --run_stage to specify multiple stages (GH-935)
* Add flag to control iperf timeout (GH-994)

Bugfixes and maintenance updates:
* Fix cloud bigtable and hbase (GH-1001)
* Install openssl for the object storage benchmark (GH-1000)
* Fix bug in MongoDB YCSB benchmark (GH-999)
* Change import_util.LoadModulesForPath to also load packages (GH-983)
* Add 'ap-northeast-2' to the S3 regions table (GH-990)
* Change check-lint.sh to invoke flake8 via tox (GH-979)
* Change GetLastRunUri to handle the new temp directory structure (GH-985)
* Support S3 Signature Version 4 (GH-984)
* Add back support for python-gflags version 2. (GH-973)
* Define units.Unit.\_\_ne\_\_ (GH-976)
* Reorder some PKB initialization steps (GH-965)
* Move to gflags version 3.0.4 (GH-969)
* Modify semantics of UnitsParser's convertible_to parameter (GH-968)
* Add percent (%) as a recognized pint unit (GH-964)
* Improve Azure CLI version check (GH-967, GH-958)
* Don't request SSH verbose output when --log_level=debug (GH-962)
* Move per-provider package requirement files (GH-961)
* Move pint unit registry code into a separate module (GH-960)
* Only check python package requirements if requirements.txt exists (GH-959)
* Changed LoadProvider to accept un-lowered cloud provider name (GH-957)
* Change bg tests to patch and verify call counts for each VM separately (GH-954)
* Make BenchmarkSpec.vms order consistent across runs (GH-953)
* Fix os type related issues and bugs (GH-950, GH-955, GH-952, GH-951, GH-949)
* Call CheckPrerequisites earlier (GH-947)
* Add percentiles option to PercentilesCalculator (GH-910)
* Fix publisher bug when disk_size is None (GH-946)
* Handle unset PYTHONPATH in tox.ini (GH-941)
* Add a helper function to BenchmarkConfigSpec to redirect flags (GH-936)
* Add support for interrupting child threads created by RunThreaded (GH-926)
* Fix bugs and improve the Cloud Bigtable benchmark (GH-937, GH-933, GH-932)
* Add a helpful error message if a UnitsParser parses a unitless value (GH-963)

# 1.4.0

New features:

* openstack: Add support for optional floating ip pool (thanks @meteorfox, GH-861)
* openstack: Use Keystone session for handling authentication (thanks @meteorfox, GH-870) 
* Support object storage classes in object storage service benchmarks (GH-895)
* Add Object Size Distributions in object storage service benchmarks (GH-888)
* Add MultiStreamThroughput benchmark to object storage benchmarks (GH-840)
* Adds a SPEC SFS 2014 benchmark which runs against GlusterFS (GH-876)
* gce_virtual_machine: Support user provided instance metadata (GH-859)

Improvements and fixes:

* openstack: Wait until VM deletion has been completed (thanks @meteorfox, GH-904)
* openstack: Fix floating IP allocation and deallocation (thanks @meteorfox, GH-862)
* rackspace: Fix missing flags bug (thanks @meteorfox, GH-903)
* Allow user to specify a flag when they run object storage benchmark on GCP, and default that flag to the latest known working version. (GH-925)
* Update mechanism used to get iperf server process id. (GH-921)
* Rename variables and improve documentation of BaseOsMixin.PullFile. (GH-923)
* Fix WindowsMixin._GetNumCpus when more than one Win32_processor exists. (GH-920)
* Add exception types to except clauses. (GH-893)
* Add Flag for List Consistency Iterations (GH-889)
* Add unit tests for scripts (GH-882)
* Add disk type examples to README.md (GH-871)
* Copy-edit the README (GH-877)
* Turn off selinux for mongodb. (thanks @akrzos, GH-867)
* Use temp files for Popen stdout and stderr in IssueCommand. (GH-878)

# 1.3.0

External contributions:
* Add RHEL based virtual machines to OpenStack provider. (thanks @akrzos;
  GH-858)
* Change rackspace provider information to use official CLI (thanks @meteorfox; GH-844)
* Add rackspace requirements (thanks @meteorfox; GH-805)

New features:
* Support flags in YAML format. (GH-857)
* Check version of required packages at runtime (GH-834)
* User-specified multiregion for GCS benchmarking (GH-845)
* Support metadata for gcp instances (GH-859)

Bugfixes and maintenance updates:
* Change rackspace's AllowPort to enable UDP (thanks @meteorfox; GH-805)
* Allow most recent verison gcs-oauth2-boto-plugin (GH-849)
* Require Pint >= 0.7 (GH-850)
* Update PIP (GH-842)
* Fix windows log message (GH-832)
* Properly Pickle Pint Quantities (GH-830)
* os_type added to boot benchmark metadata (GH-826)
* Better handle Azure timeouts (GH-825)
* Better handling of AWS integration tests. (GH-869, GH-868)

# 1.2.0

New features:
* Introduced Object Sizes in flags (GH-808).
* Add ListDecoder for verifying a config option that expects list values
  (GH-807).

Enhancements:
* Bump HBase to 1.0.3 in hbase_ycsb benchmark (GH-822).
* Change MockFlags to be more like the real FlagValues (GH-812).
* Rename test_flag_util.py and add FlagDictSubstitution tests (GH-811).
* Create BenchmarkConfigSpec to aggregate benchmark input checking (GH-810).
* Remove flag proxy objects (GH-802).

Bugfixes and maintenance updates:
* Fix sample iperf config (GH-801).
* FixIntDecoder and FloatDecoder behavior when min=0 or max=0 (GH-800).

# 1.1.0

External contributions:
* Add additional percentile samples to mysql_service benchmark and others
  (thanks @zbjornson; GH-729).
* Add Mesos-Marathon as a provider (thanks @mateusz-blaszkowski; GH-679,
  GH-731).
* Use only available floating-ips for OpenStack (thanks @meteorfox; GH-733).
* Add OpenStack CLI-based support for object_storage_service benchmark (thanks
  @meteorfox; GH-738).
* Make printed benchmark results more readable (thanks @wonderfly; GH-747).
* Clean up formatting, grammar, and markdown in README.md (thanks @mbrukman;
  GH-748).
* Add links to projects and licenses in README.md (thanks @mbrukman; GH-763).
* Fix a bug that caused --helpxml to not work (thanks @jldiaz; GH-782).
* Make OpenStack VM username configurable with a new --openstack_image_username
  flag (thanks @lwatta; GH-788).

New features:
* Allow users to specify a storage region for the object_storage_service
  benchmark with a new --object_storage_region flag (GH-609).
* Add support for running a background CPU workload while executing a benchmark
  (GH-715, GH-762).
* Allow creating GCE custom VMs easily from command-line flags (GH-727).
* Add the --gce_network_name to allow GCE users to use pre-existing networks
  (GH-746).
* Publish a PTRANS sample from the hpcc benchmark (GH-785).
* Add support for running a background network workload while executing a
  benchmark (GH-786).
* Add a CSV sample output format (GH-791).

Enhancements:
* Trim blocks while formatting and mounting disks (GH-692).
* Raise an error when no requests succeed in redis_benchmark (GH-702).
* Update the Google benchmark set (GH-705).
* Add input-checking to disk specs (GH-675, GH-779).
* Let disks on Windows machines return Windows DeviceIds (GH-743).
* Speed up Cassandra cluster boot by setting "auto_bootstrap" to false (GH-751).
* Defer package update for Ubuntu (GH-752).
* Add provider-specific requirements.txt files (GH-753).
* Increase preload thread count for YCSB benchmarks (GH-760). This change
  affects some of the samples generated by the aerospike_ycsb, cassandra_ycsb,
  and mongodb_ycsb benchmarks, including the overall Runtime and overall
  Throughput.
* Guarantee order of VM and disk spec config option decoding (GH-792).

Bugfixes and maintenance updates:
* Create one AWS VPC/IGW/PlacementGroup per region (GH-687, GH-798).
* Update README.md instructions for setting up PKB for GCE (GH-689).
* Update comments in pkb.py to reflect new provision and teardown stages
  (GH-691).
* Fix bug that prevented object_storage_service benchmark from running in stages
  (GH-694).
* Add test helpers that make it easier to mock command-line flags (GH-695,
  GH-730).
* Delete set-interrupts.sh (GH-703).
* Make process exit status non-zero upon benchmark failure (GH-713).
* Move "To run again" message from teardown to cleanup stage (GH-714).
* Fix creation of AWS VMs using Placement Groups (GH-721).
* Create AWS/GCE default access rules before creating VMs (GH-726, GH-756).
  This change affects the Boot Time measured by the cluster_boot benchmark.
* Fix a GCE boot disk creation bug (GH-736).
* ntttcp benchmark fixes (GH-737).
* Force mongodb_ycsb benchmark to use only one VM (GH-755).
* Unmount /mnt if it exists prior to creating a scratch filesystem (GH-757).
* Move provider constants to a separate module (GH-758).
* Fix a bug when creating an AWS VPC outside the user's default region (GH-774).
* Add tests for generating help strings (GH-782).
* Kill servers started by the iperf benchmark as part of its cleanup (GH-784).
* Fix some style inconsistencies (GH-789).
* Move operating system constants to a separate module (GH-790).
* Fix some broken tests (GH-795, GH-796).

# 1.0.1

* Fix for benchmark_compatibility_checking flag. (thanks @mateusz-blaszkowski;
  GH-707)

# 1.0.0

New features:
* Added decoders to configs which allow for additional config validation
  (GH-672)
* Add support for GCE custom VMs (GH-664)
* Providers can now selectively support benchmarks (GH-690)

Breaking changes:
* Enhanced config usage by multi-node benchmarks - this changes some of their
  default metadata. (GH-669)
* YCSB histogram results are now not included by default (GH-656)

Enhancements:
* Ping benchmark is now bi-directional (GH-685)
* Metadata flag can be specified multiple times (GH-684)
* Allow AWS regions as well as zones (GH-658)
* Added 'vm_count' metadata to all benchmarks (GH-659)
* GCP now creates networks (GH-648)
* Added provision and teardown run_stages (GH-652)
* Update tomcat_wrk to report more samples (GH-650)
* Add a flag to control the log file log level (GH-651)

Bugfixes and maintenance updates:
* Added helper for mocking FLAGS (GH-678)
* Fix for silo benchmark which fails when running behind proxy (thanks
  @mateusz-blaszkowski; GH-680)
* Fix for not-installed curl package (thanks @mateusz-blaszkowski; GH-681)
* Invoke YCSB from its installation directory (GH-677)
* Fix bug with detecting run_uris (GH-673)
* Fix the image_project flag so it works as intended (thanks @wonderfly; GH-666)
* Add explicit --boot-disk-auto-delete to GCE VM creation (GH-670)
* Improved the README layout and fixed errors (GH-661, GH-660)
* Broaden exception caught during PKB run (GH-667)
* Fixed bug with run stages (GH-665)
* Fixed formatting of docstring (thanks @wonderfly; GH-662)
* Fixed bug with GCP networks (GH-663)
* Add GetMachineTypeDict() to replace machine_type in Sample metadata (GH-653)
* Add GcloudCommand helper (GH-649)
* Fixed Tomcat download URL (GH-700)
* Retry RemoteCopy operations (GH-699)
* Various fixes to data disks for OpenStack (thanks @meteorfox; GH-688)

# 0.24.0

New features:
* CS2 rachel web serving benchmark (thanks @rmend016; GH-451)
* Add [AliCloud](http://intl.aliyun.com/) provider (thanks @hicrazyboy; GH-611)
* Add tomcat/wrk benchmark (GH-598)

Breaking changes:
* Remove `--parallelism` flag (GH-633)
* Rename `fio` flags (GH-518, GH-581)
* Added new disk type options to clarify use. (Breaking change) we will remove
  the current options in a coming release (GH-599)

Enhancements:
* Docker support in DigitalOcean and StaticVirtualMachine (thanks @maxking; GH-528)
* Always use latest Azure package (GH-585)
* Support `read/counter_read/mixed` in cassandra_stress benchmark (GH-607,
  GH-592)
* Add pretty-printed benchmark run summary table (GH-620)
* Support new configs in the
  [Cloudsuite web serving](http://parsa.epfl.ch/cloudsuite/web.html) benchmark
  (GH-605)

Bugfixes and maintenance updates:
* Move providers to their own directory (GH-617)
* Automatically register VM classes and specs (GH-600)
* Only load needed provider modules (GH-635, GH-636)
* Prefix benchmark and packages directories with `linux_` (GH-640)
* Update to OpenStack Nova (thanks @meteorfox; GH-613)
* Add `AUTHORS` file; update copyright header (GH-618)
* Continue executing benchmarks after one fails (GH-614)
* Disk integration tests for GCE, AWS, Azure (GH-571, GH-595)
* Update Windows `cluster_boot` benchmark to match Linux one (GH-647)
* Update `README` for lazy-loading providers (GH-645)
* Only call `SetupPackageManager` if `install_packages` is True (GH-644)
* Generate per-VM-group SSH aliases (GH-639)
* Move AWS and Azure `_GetDefaultImage` calls to `_CreateDependencies` (GH-641)
* Get Azure default image name from `azure vm image list` (GH-637)
* Add `boto` lib version to object storage benchmark metadata (GH-632)
* Improve the guide on using Docker (thanks @mateusz-blaszkowski; GH-628)
* Improvements to VM pool handling (GH-626, GH-608)
* Make benchmark metadata specify benchmark name (GH-615)
* Support overriding Azure client lib version (GH-587)
* Update Cassandra version to 2.1.10 (GH-577)


# v0.23.0

Known Issues:

* Resources fail to be cleaned up when PerfKitBenchmarker receives SIGINT while
  running with `--parallel` (GH-529).

New Features:

* Added [CloudStack](http://cloudstack.apache.org) provider support (thanks @syed; GH-558).
* **❢ BREAKING CHANGE:** Added a more flexible benchmark configuration system
  (GH-510, GH-546). This supports running the same benchmark multiple times,
  and running a benchmark using heterogeneous machine types and clouds. See [the
  wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/PerfkitBenchmarker-Configurations)
  for more information. This replaces the configuration system under
  `perfkitbenchmarker/deployment`.
* New benchmark: `redis_ycsb`, which runs YCSB against a single YCSB node (GH-511).

Enhancements:

* Added flag: `--archive_bucket`, to archive the results of a run to Google Cloud
  Storage or S3 (GH-489).
* Added `--prefill_device` option to fio benchmark. Note that the `fio` benchmark
  now defaults to *not* pre-filling the device when testing against a filesystem (GH-515, FH-516).
* Updated `cloud_bigtable_ycsb` benchmark to pre-split table, following HBase recommendions (GH-524).
* Added additional logging to `LinuxVirtualMachine.RobustRemoteCommand` (GH-534).
* Added support for local disks with EC2 `d2` types.
* Kubernetes: Support `emptyDisk` disk types (thanks @mateusz-blaszkowski GH-565).
* `cluster_boot`: Add `num_vms` metadata (GH-575).
* Add published `run_uri` to VM metadata (GH-579).
* Add a flag to specify the Azure Python library version in object storage benchmark,
  default to the latest version (GH-585, GH-587).

Bugfixes and maintenance updates:

* Kubernetes: Adapt provider to work with different versions of `kubectl`
  (Thanks @mateusz-blaszkowski; GH-574).
* Eliminated a spurious resource warning (GH-522).
* Fixed a string comparison bug in Object storage benchmark (GH-526).
* Rackspace; Removed an extra parameter from firewall implementation (thanks @meteorfox, GH-531).
* Rackspace: Fixed an SSH key upload bug (thanks @meteorfox, GH-539).
* Fix an issue with loggin errors in `vm_util.RunThreaded` (GH-542; thanks @mateusz-blaszkowski for reporting).
* `mongodb_ycsb`: Update default write concern to "Acknowledged" (equivalent to the previous deprecated option, safe) (GH-543).
* Specify SSH port when using SCP (thanks @mateusz-blaszkowski GH-548).
* Fix string conversion (thanks @mateusz-blaszkowski GH-556).
* `hbase_ycsb`: Make load records use a BufferedMutator (GH-566).

# v0.22.1

* Update the default image used in Azure tests to Ubuntu 14.04.3 (runs failed
  with the current image.) (cherry-pick of #520)

# v0.22.0
* New Features:
  * Add Kubernetes as a provider where benchmarks can be run (thanks @mateusz-blaszkowski) (GH-475)
  * Add EPFL CloudSuite WebSearch Workload (thanks @Vukasin92) (GH-422, GH-479)
  * Aerospike_yscb_benchmark (GH-486)

* Enhancements:
  * OpenStack checks/changes for attaching volumes, default volume size, config improvements, … (thanks @kivio) (GH-454, GH-459, GH-464)
  * Fio test improvements and clean-up - template files, logging, ...  (GH-421, GH-501, GH-502)
  * Add improvement to use compiled crcmod when benchmarking Google Cloud Storage (GH-461)
  * Collect Fio data over time via “--run_for_minutes” flag (GH-477)
  * Execute command improvements to add better logging and parallelism (GH-488, GH-470)

* Bugfixes and maintenance updates:
  * Update to allow PKB to work with the latest Azure CLI 0.9.9 (GH-485)
  * Updated fio version to 2.2.10 (GH-509)
  * fio iodepth flag fix “--io_depths”  (GH-495)
  * Fixes a race condition on AWS/Azure where networks in use were deleted (GH-506)
  * Added a note on how to fix dependencies when tox tests fail (GH-505)
  * Fixed  broken Apache project links (GH-492, GH-467)
  * Netperf timeout to work around occasional hangs (GH-483)
  * Fixed lock pickling issues (GH-471)

# v0.21.0

* New features:
  * Add support for OpenStack affinity and anti-affinity groups. (GH-440, thanks to @kivio)
  * Add large object scenario for CLI tests in object_storage_service. (GH-445)
  * Add support for pre-emptible GCE VMs. (GH-415)

* Enhancements:
  * Parallelize cassandra_ycsb and hbase_ycsb benchmark setups. (GH-435)
  * Add more config options for aerospike benchmark. (GH-450)

* Bugfixes and maintenance updates:
  * Refactor OpenStack network IP management. (GH-438, thanks to @kivio)
  * Fix thread lock pickling bug. (GH-425, thanks to @kivio)
  * Update jars used in cloud_bigtable_ycsb benchmark. (GH-444)
  * Update YCSB to v0.3.0. (GH-428)
  * Fix object_storage_service bug introduced by Azure breaking change. (GH-446)
  * Fix object storage CLI output format. (GH-449)

# v0.20.0

* Enhancements:
  * Specify project to gcloud alpha bigtable clusters list (GH-433)
  * More Samples in FIO (GH-416)

* Bugfixes and maintenance updates:
  * pkb label problem in OpenStack driver fixes (GH-410)
  * On exception, only cleanup if run stage is all/cleanup (GH-412)
  * Fix issue with using a flag before flags are parsed (GH-413)
  * Umount disk when running fio against raw device. (GH-417)
  * Clarify warnings from `ycsb._CombineResults`. (GH-432)

# v0.19.0

* New features:
  * New mysql_service benchmark. This benchmarks a cloud's managed MySQL
    offering using sysbench. (GH-387)

* Enhancements:
  * Added option to disable iptables if your image requires it (GH-361)
  * mongodb_ycsb now installs client and server dependencies in parallel,
    speeding up the end to end run time for the benchmark. (GH-402)
  * The netperf and iperf benchmarks now only add firewall rules if they are
    running over external ips. (GH-382)

* Bugfixes and maintenance updates:
  * The iperf package will now check the 'redhat-release' version and install
    directly from an RPM (this enables iperf to be run on Scientific Linux 6.x).
    (GH-392, thanks to @Vukasin92)
  * Fix bug where VM temporary directory wasn't created before use on RHEL based
    static VMs. (GH-389, thanks to @Vukasin92)
  * netperf package url changed since version 2.6.0 is now in archive/ (GH-390)
  * Fixed DigitalOcean package installation error (GH-396)
  * The object_storage_service benchmark no longer copies gcloud logs as part of copying
    the gcloud configuration to the VM. (GH-383)
  * Correctly cleanup network resources when run stages are used. (GH-386)
  * Added timeout to apt-get update command because it will occasionally hang.
    (GH-391)
  * Update copy_throughput benchmark so it works with
    ContainerizedVirtualMachines. (GH-408)
  * Install python inside ContainerizedVirtualMachines so that
    RobustRemoteCommand works on them. (GH-404)

* Benchmark-specific changes:
  * speccpu2006 will no longer report results if the run was incomplete. This
    behavior can be modified with a flag. (GH-397)
  * The mongodb benchmark has been completely removed since mongodb_ycsb
    replaced it with greater functionality. (GH-403)
  * The fio benchmark now has more latency percentiles included in sample
    metadata. (GH-399)
  * Cassandra version bumped up to 2.0.16 since 2.0.0 has known issues (GH-393)


# v0.18.0

(See also https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues/369
which includes this change log with clickable GH-* links.)

* New features:
  * Support OpenStack as cloud provider (GH-305, GH-353, thanks @kivio and
    @mateusz-blaszkowski)
  * Support Rackspace as cloud provider (GH-336, thanks @meteorfox and @jrperritt)
  * Add support for ContainerizedVM using docker exec (GH-333, thanks @gablg1)
  * Windows guest VM support on Static VM (GH-350), Azure (GH-349, GH-374), AWS
    (GH-347), and GCE (GH-338)
  * Add NTttcp Windows networking benchmark (GH-348)

* Enhancements:
  * Support using proxies in VMs (GH-339, GH-337, thanks @kivio)
  * Enable optional migration on GCE (GH-343)
  * Execute long running commands via a remote agent (GH-310)
  * Add resource creation/deletion times to logs (GH-316)

* Bugfixes and maintenance updates:
  * Update PKB to work with Azure version 0.9.3 (GH-312)
  * Fix AWS CLI usage on Windows host (GH-313)
  * Auto-fetch AMI IDs for AWS images (GH-364)
  * Fix publisher missing info for default image and machine type (GH-357)
  * Fix 'no attribute pkb_thread_log_context' error for sub-thread logs (GH-322)

* Benchmark-specific changes:
  * aerospike: config/flag handling bugfixes (GH-367, GH-360, GH-354)
  * cassandra_ycsb: move num_vms prerequisite check
  * fio: add latency percentiles for results (GH-344)
  * hadoop_terasort: Fix bad SSH option (GH-328)
  * iperf: add lower bounds to arguments (GH-314)
  * iperf: add timeout to parallel benchmark runs to handle iperf hangs (GH-375)
  * netperf: Support confidence intervals, increase test length, report stddev
    (GH-317, GH-306)
  * ycsb: Drop unaggregatable results from samples (GH-324)

* Development and testing:
  * **Breaking Change** Automated testing now uses `tox` (GH-330)
  * Refactored hook scripts, including new opt-in pre-push hook (GH-363)
  * Use travis for CI testing (GH-340)
  * Speed up tests using timeouts (GH-299)

* Internals:
  * Move defaults from benchmark_spec to VM classes, move network instantiation
    out of benchmark spec (GH-342)
  * Add event hook support (GH-315)
  * Refactor VM classes (GH-321)

# v0.17.0

* Add initial support for DigitalOcean as a cloud provider (GH-291).
* Add SciMark2 Benchmark (GH-271, thanks @zlim!).
* New NoSQL benchmarks: `cloud_bigtable_ycsb` (GH-283), `mongodb_ycsb`
  (GH-279), `cassandra_ycsb` (GH-278).
* Allow PerfKitBenchmarker to run on a Windows controller (GH-274).
* Add a `--dstat` flag, to collect performance metrics during benchmark runs
  (GH-282).
* Add support for parallel iperf clients to the iperf benchmark (GH-262).
* Add scratch disk information to sample metadata (GH-277).
* Add a 5 minute timeout to local commands (GH-289, GH-293).
* Do not use FLAG values in generating benchmark documentation (GH-280).
* Bump HBase to v1.0.1.1 (GH-281).
* Fix an issue preventing resources from being cleaned up (GH-276).

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
* Updates AWS to use `_Exists` for resources. (GH-228)
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
