### [absl.app ](../absl/app.py)

#### Description:

No description available

#### Flags:

`--[no]only_check_args`: Set to true to validate args and exit.
    (default: 'false')

`--[no]pdb_post_mortem`: Set to true to handle uncaught exceptions with PDB post
    mortem.
    (default: 'false')

`--profile_file`: Dump profile information to a file (for python -m pstats).
    Implies --run_with_profiling.

`--[no]run_with_pdb`: Set to true for PDB debug mode
    (default: 'false')

`--[no]run_with_profiling`: Set to true for profiling the script. Execution will
    be slower, and the output format might change over time.
    (default: 'false')

`--[no]use_cprofile_for_profiling`: Use cProfile instead of the profile module
    for profiling. This has no effect unless --run_with_profiling is set.
    (default: 'true')

### [absl.logging ](../absl/logging.py)

#### Description:

No description available

#### Flags:

`--[no]alsologtostderr`: also log to stderr?
    (default: 'false')

`--log_dir`: directory to write logfiles into
    (default: '')

`--[no]logtostderr`: Should only log to stderr?
    (default: 'false')

`--[no]showprefixforinfo`: If False, do not prepend prefix to info messages when
    it's logged to stderr, --verbosity is set to INFO level, and python logging
    is used.
    (default: 'true')

`--stderrthreshold`: log messages at this level, or more severe, to stderr in
    addition to the logfile.  Possible values are 'debug', 'info', 'warning',
    'error', and 'fatal'.  Obsoletes --alsologtostderr. Using --alsologtostderr
    cancels the effect of this flag. Please also note that this flag is subject
    to --verbosity and requires logfile not be stderr.
    (default: 'fatal')
  -v,--verbosity: Logging verbosity level. Messages logged at this level or
    lower will be included. Set to 1 for debug logging. If the flag was not set
    or supplied, the value will be changed from the default of -1 (warning) to 0
    (info) after flags are parsed.
    (default: '-1')
    (an integer)

### [perfkitbenchmarker.app_service ](../perfkitbenchmarker/app_service.py)

#### Description:

Module containing class for BaseAppService and BaseAppServiceSpec.

#### Flags:

`--app_runtime`: Runtime environment of app service uses. e.g. python, java

`--app_type`: Type of app packages builders should built.

`--appservice`: Type of app service. e.g. AppEngine

`--appservice_backend`: Backend instance type of app service uses.

`--appservice_count`: Copies of applications to launch.
    (default: '1')
    (an integer)

`--appservice_region`: Region of deployed app service.

### [perfkitbenchmarker.background_tasks ](../perfkitbenchmarker/background_tasks.py)

#### Description:

Background tasks that propagate PKB thread context.

TODO(skschneider): Many of the threading module flaws have been corrected in
Python 3. When PKB switches to Python 3, this module can be simplified.

PKB tries its best to clean up provisioned resources upon SIGINT. By default,
Python raises a KeyboardInterrupt upon a SIGINT, but none of the built-in
threading module classes are designed to handle a KeyboardInterrupt very well:

- threading.Lock has an atomic acquire method that cannot be interrupted and
  hangs forever if the same thread tries to acquire twice. Its release method
  can be called by any thread but raises thread.error if an unacquired Lock is
  released.

- More complicated classes (threading.RLock, threading.Event, threading.Thread,
  Queue.Queue) use internal Locks in such a way that a KeyboardInterrupt can
  cause a thread that has acquired a Lock to jump out of its current action
  without releasing the Lock. For example, in the below code, a
  KeyboardInterrupt can be raised immediately after the acquire call but before
  entering the try block:
    lock.acquire()
    try:
      ...
    except:
      lock.release()

Taken together, this means that there is a possibility to leave an internal Lock
acquired, and when later cleanup steps on the same or different thread attempt
to acquire the Lock, they will hang forever, unresponsive to even a second
KeyboardInterrupt. A KeyboardInterrupt during Thread.start() or Thread.join()
can even trigger an unbalanced acquire on a global lock used to keep track of
active threads, so that later attempts to start or join any Thread will hang
forever.

While it would take a significant and impractical redesign of PKB's code to
completely eliminate any risk of deadlock following a KeyboardInterrupt, the
code in this module is designed to allow interrupting parallel tasks while
keeping the risk of deadlock low.


#### Flags:

`--max_concurrent_threads`: Maximum number of concurrent threads to use when
    running a benchmark.
    (an integer)

### [perfkitbenchmarker.beam_benchmark_helper ](../perfkitbenchmarker/beam_benchmark_helper.py)

#### Description:

Helper methods for Apache Beam benchmarks.

This file contains methods which are common to all Beam benchmarks and
executions.


#### Flags:

`--beam_extra_properties`: Allows to specify list of key-value pairs that will
    be forwarded to target mvn command as system properties

`--beam_filesystem`: Defines filesystem which will be used in tests. If not
    specified it will use runner's local filesystem.

`--beam_it_module`: Gradle module containing integration test. Use full module
    starting and separated by colon, like :sdk:python

`--beam_it_timeout`: Integration Test Timeout.
    (default: '600')
    (an integer)

`--beam_location`: Location of already checked out Beam codebase.

`--[no]beam_prebuilt`: Set this to indicate that the repo in beam_location does
    not need to be rebuilt before being used
    (default: 'false')

`--beam_python_attr`: Test decorator that is used in Beam Python to filter a
    specific category.
    (default: 'IT')

`--beam_python_sdk_location`: Python SDK tar ball location. It is a required
    option to run Python pipeline.

`--beam_runner`: Defines runner which will be used in tests
    (default: 'dataflow')

`--beam_runner_option`: Overrides any pipeline options to specify the runner.

`--beam_sdk`: <java|python>: Which BEAM SDK is used to build the benchmark
    pipeline.

`--beam_version`: Version of Beam to download. Use tag from Github as value. If
    not specified, will use HEAD.

`--git_binary`: Path to git binary.
    (default: 'git')

`--gradle_binary`: Set to use a different gradle binary than gradle wrapper from
    the repository

### [perfkitbenchmarker.benchmark_sets ](../perfkitbenchmarker/benchmark_sets.py)

#### Description:

Benchmark set specific functions and definitions.

#### Flags:

`--flag_matrix`: The name of the flag matrix to run.

`--flag_zip`: The name of the flag zip to run.

`--num_benchmark_copies`: The number of copies of each benchmark config to run.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.benchmark_spec ](../perfkitbenchmarker/benchmark_spec.py)

#### Description:

Container for all data required for a benchmark to run.

#### Flags:

`--benchmark_compatibility_checking`: <strict|permissive|none>: Method used to
    check compatibility between the benchmark  and the cloud.  strict runs the
    benchmark only if the cloud provider has declared it supported. permissive
    runs the benchmark unless it has been declared not supported by the cloud
    provider. none does not do the compatibility check.
    (default: 'strict')

`--cloud`: <GCP|Azure|AWS|DigitalOcean|Kubernetes|OpenStack|Rackspace|CloudStack
    |AliCloud|Mesos|ProfitBricks>: Name of the cloud to use.
    (default: 'GCP')

`--create_and_boot_post_task_delay`: Delay in seconds to delay in between boot
    tasks.
    (an integer)

`--postrun_script`: Script to run right after run stage.

`--scratch_dir`: Base name for all scratch disk directories in the VM. Upon
    creation, these directories will have numbers appended to them (for example
    /scratch0, /scratch1, etc).

`--startup_script`: Script to run right after vm boot.

### [perfkitbenchmarker.capacity_reservation ](../perfkitbenchmarker/capacity_reservation.py)

#### Description:

Module containing abstract class for a capacity reservation for VMs.

#### Flags:

`--[no]use_capacity_reservations`: Whether to use capacity reservations for
    virtual machines. Only supported on AWS.
    (default: 'false')

### [perfkitbenchmarker.cloud_tpu ](../perfkitbenchmarker/cloud_tpu.py)

#### Description:

Module containing class for TPU.

#### Flags:

`--tpu_accelerator_type`: TPU accelerator type for the TPU.
    (default: 'tpu-v2')

`--tpu_cidr_range`: CIDR Range for the TPU. The IP
    range that the TPU will select an IP address from. Must be
    in CIDR notation and a /29 range, for example
    192.168.0.0/29. Errors will occur if the CIDR range has
    already been used for a currently existing TPU, the CIDR
    range conflicts with any networks in the user's provided
    network, or the provided network is peered with another
    network that is using that CIDR range.

`--tpu_cores_per_donut`: The number of cores per TPU donut. This is 8 because
    each TPU has 4 chips each with 2 cores.
    (default: '8')
    (an integer)

`--tpu_description`: Specifies a text description of the TPU.

`--tpu_name`: The name of the TPU to create.

`--tpu_network`: Specifies the network that this TPU will be a part of.

`--[no]tpu_preemptible`: Use preemptible TPU or not.
    (default: 'false')

`--tpu_tf_version`: TensorFlow version for the TPU.

### [perfkitbenchmarker.configs ](../perfkitbenchmarker/configs.py)

#### Description:

No description available

#### Flags:

`--benchmark_config_file`: The file path to the user config file which will
    override benchmark defaults. This should either be a path relative to the
    current working directory, an absolute path, or just the name of a file in
    the configs/ directory.

`--config_override`: This flag can be used to override any config value. It is
    applied after the user config (specified via --benchmark_config_file_path),
    so it has a higher priority than that config. The value of the flag should
    be fully.qualified.key=value (e.g.
    --config_override=cluster_boot.vm_groups.default.vm_count=4).;
    repeat this option to specify a list of values

### [perfkitbenchmarker.container_service ](../perfkitbenchmarker/container_service.py)

#### Description:

Contains classes related to managed container services.

For now this just consists of a base cluster class that other container
services will be derived from and a Kubernetes specific variant. This enables
users to run PKB VM based benchmarks on container providers (e.g. Kubernetes)
without pre-provisioning container clusters. In the future, this may be
expanded to support first-class container benchmarks.


#### Flags:

`--container_cluster_cloud`: Sets the cloud to use for the container cluster.
    This will override both the value set in the config and the value set using
    the generic "cloud" flag.

`--container_cluster_num_vms`: Number of nodes in the cluster. Defaults to
    container_cluster.vm_count
    (an integer)

`--container_cluster_type`: The type of container cluster.
    (default: 'Kubernetes')

`--container_cluster_version`: Optional version flag to pass to the cluster
    create command. If not specified, the cloud-specific container
    implementation will chose an appropriate default.

`--[no]force_container_build`: Whether to force PKB to build container images
    even if they already exist in the registry.
    (default: 'false')

`--kubeconfig`: Path to kubeconfig to be used by kubectl. If unspecified, it
    will be set to a file in this run's temporary directory.

`--kubectl`: Path to kubectl tool
    (default: 'kubectl')

`--[no]local_container_build`: Force container images to be built locally rather
    than just as a fallback if there is no remote image builder associated with
    the registry.
    (default: 'false')

`--[no]static_container_image`: Whether container images are static (i.e. are
    not managed by PKB). If this is set, PKB will accept the image as fully
    qualified (including repository) and will not attempt to build it.
    (default: 'true')

### [perfkitbenchmarker.data ](../perfkitbenchmarker/data.py)

#### Description:

No description available

#### Flags:

`--data_search_paths`: Additional paths to search for data files. These paths
    will be searched prior to using files bundled with PerfKitBenchmarker.;
    repeat this option to specify a list of values
    (default: "['.']")

### [perfkitbenchmarker.disk ](../perfkitbenchmarker/disk.py)

#### Description:

Module containing abstract classes related to disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.


#### Flags:

`--fstab_options`: Additional arguments to supply to fstab.
    (default: '')
    (a comma separated list)

`--mount_options`: Additional arguments to supply when mounting.
    (default: '')
    (a comma separated list)

`--nfs_directory`: Directory to mount if using a StaticNfsService. This
    corresponds to the "VOLUME_NAME" of other NfsService classes.

`--nfs_ip_address`: If specified, PKB will target this ip address when mounting
    NFS "disks" rather than provisioning an NFS Service for the corresponding
    cloud.

`--nfs_retries`: NFS Retries.
    (default: '2')
    (an integer)

`--nfs_rsize`: NFS read size.
    (default: '1048576')
    (an integer)

`--nfs_timeout`: NFS timeout.
    (default: '60')
    (an integer)

`--[no]nfs_timeout_hard`: Whether to use hard or soft for NFS mount.
    (default: 'true')

`--nfs_wsize`: NFS write size.
    (default: '1048576')
    (an integer)

`--smb_version`: SMB version.
    (default: '3.0')

### [perfkitbenchmarker.dpb_service ](../perfkitbenchmarker/dpb_service.py)

#### Description:

Benchmarking support for Data Processing Backend Services.

In order to benchmark Data Processing Backend services such as Google
Cloud Platform's Dataproc and Dataflow or Amazon's EMR, we create a
BaseDpbService class.  Classes to wrap specific backend services are in
the corresponding provider directory as a subclass of BaseDpbService.


#### Flags:

`--dpb_job_classname`: Classname of the job implementation in the jar file

`--dpb_job_jarfile`: Executable Jarfile containing workload implementation

`--dpb_log_level`: Manipulate service log level
    (default: 'INFO')

`--dpb_service_zone`: The zone for provisioning the dpb_service instance.

`--static_dpb_service_instance`: If set, the name of the pre created dpb
    implementation,assumed to be ready.

### [perfkitbenchmarker.edw_service ](../perfkitbenchmarker/edw_service.py)

#### Description:

Resource encapsulating provisioned Data Warehouse in the cloud Services.

Classes to wrap specific backend services are in the corresponding provider
directory as a subclass of BaseEdwService.


#### Flags:

`--edw_query_execution_mode`: <sequential|concurrent>: The mode for executing
    the queries on the edw cluster.
    (default: 'sequential')

`--edw_service_cluster_concurrency`: Number of queries to run concurrently on
    the cluster.
    (default: '5')
    (an integer)

`--edw_service_cluster_db`: If set, the db on cluster to use during the
    benchmark (only applicable when using snapshots).

`--edw_service_cluster_identifier`: If set, the preprovisioned edw cluster.

`--edw_service_cluster_password`: If set, the password authorized on cluster
    (only applicable when using snapshots).

`--edw_service_cluster_snapshot`: If set, the snapshot to restore as cluster.

`--edw_service_cluster_user`: If set, the user authorized on cluster (only
    applicable when using snapshots).

`--edw_service_endpoint`: If set, the preprovisioned edw cluster endpoint.

`--edw_service_resource_group`: Needed to manage Azure clusters.

### [perfkitbenchmarker.flag_util ](../perfkitbenchmarker/flag_util.py)

#### Description:

Utility functions for working with user-supplied flags.

#### Flags:

`--fio_blocksize`: The block size for fio operations. Default is given by the
    scenario when using --generate_scenarios. This flag does not apply when
    using --fio_jobfile.
    (A quantity with a unit. Ex: 12.3MB.)

`--gpu_clock_speeds`: desired gpu clock speeds in the form [memory clock,
    graphics clock]
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--object_storage_object_sizes`: Size of objects to send and/or receive. Only
    applies to the api_multistream scenario. Examples: 1KB, {1KB: 50%, 10KB:
    50%}
    (default: '1KB\n...\n')
    (A YAML expression.)

### [perfkitbenchmarker.hpc_util ](../perfkitbenchmarker/hpc_util.py)

#### Description:

HPC utility functions.

#### Flags:

`--[no]mpirun_allow_run_as_root`: Whether to allow mpirun to be run by the root
    user.
    (default: 'false')

### [perfkitbenchmarker.kubernetes_helper ](../perfkitbenchmarker/kubernetes_helper.py)

#### Description:

No description available

#### Flags:

`--k8s_get_retry_count`: Maximum number of waits for getting LoadBalancer
    external IP
    (default: '18')
    (an integer)

`--k8s_get_wait_interval`: Wait interval for getting LoadBalancer external IP
    (default: '10')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.aerospike_benchmark ](../perfkitbenchmarker/linux_benchmarks/aerospike_benchmark.py)

#### Description:

Runs Aerospike (http://www.aerospike.com).

Aerospike is an opensource NoSQL solution. This benchmark runs a read/update
load test with varying numbers of client threads against an Aerospike server.

This test can be run in a variety of configurations including memory only,
remote/persistent ssd, and local ssd. The Aerospike configuration is controlled
by the "aerospike_storage_type" and "data_disk_type" flags.


#### Flags:

`--aerospike_client_threads_step_size`: The number to increase the Aerospike
    client threads by for each iteration of the test.
    (default: '8')
    (a positive integer)

`--aerospike_max_client_threads`: The maximum number of Aerospike client
    threads.
    (default: '128')
    (a positive integer)

`--aerospike_min_client_threads`: The minimum number of Aerospike client
    threads.
    (default: '8')
    (a positive integer)

`--aerospike_num_keys`: The number of keys to load Aerospike with. The index
    must fit in memory regardless of where the actual data is being stored and
    each entry in the index requires 64 bytes.
    (default: '1000000')
    (an integer)

`--aerospike_read_percent`: The percent of operations which are reads.
    (default: '90')
    (an integer in the range [0, 100])

### [perfkitbenchmarker.linux_benchmarks.aerospike_certification_tool_benchmark ](../perfkitbenchmarker/linux_benchmarks/aerospike_certification_tool_benchmark.py)

#### Description:

Runs a aerospike certification tool benchmark.

See https://github.com/aerospike/act for more info.


#### Flags:

`--[no]act_stop_on_complete`: Stop the benchmark when completing current load.
    This can be useful deciding maximum sustained load for stress tests.
    (default: 'true')

### [perfkitbenchmarker.linux_benchmarks.beam_integration_benchmark ](../perfkitbenchmarker/linux_benchmarks/beam_integration_benchmark.py)

#### Description:

Generic benchmark running Apache Beam Integration Tests as benchmarks.

This benchmark provides the piping necessary to run Apache Beam Integration
Tests as benchmarks. It provides the minimum additional configuration necessary
to get the benchmark going.


#### Flags:

`--beam_it_args`: Args to provide to the IT. Deprecated & replaced by
    beam_it_options

`--beam_it_class`: Path to IT class

`--beam_it_options`: Pipeline Options sent to the integration test.

`--beam_kubernetes_scripts`: A local path to the Kubernetes scripts to run which
    will instantiate a datastore.

`--beam_options_config_file`: A local path to the yaml file defining static and
    dynamic pipeline options to use for this benchmark run.

### [perfkitbenchmarker.linux_benchmarks.bidirectional_network_benchmark ](../perfkitbenchmarker/linux_benchmarks/bidirectional_network_benchmark.py)

#### Description:

Generates bidirectional network load using netperf.

docs:
https://hewlettpackard.github.io/netperf/doc/netperf.html

Runs TCP_STREAM and TCP_MAERTS benchmark from netperf between several machines
to fully saturate the NIC on the primary vm.


#### Flags:

`--bidirectional_network_test_length`: bidirectional_network test length, in
    seconds
    (default: '60')
    (a positive integer)

`--bidirectional_network_tests`: The network tests to run.
    (default: 'TCP_STREAM,TCP_MAERTS,TCP_MAERTS')
    (a comma separated list)

`--bidirectional_stream_num_streams`: Number of netperf processes to run.
    (default: '8')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.blazemark_benchmark ](../perfkitbenchmarker/linux_benchmarks/blazemark_benchmark.py)

#### Description:

Run Blazemark benchmark.

#### Flags:

`--blazemark_kernels`: A list of additional flags send to blazemark, in order to
    enable/disable kernels/libraries. Currently only support blaze. See
    following link for more details: https://bitbucket.org/blaze-
    lib/blaze/wiki/Blazemark#!command-line-parameters
    (default: '-only-blaze')
    (a comma separated list)

`--blazemark_set`: A set of blazemark benchmarks to run.See following link for a
    complete list of benchmarks to run: https://bitbucket.org/blaze-
    lib/blaze/wiki/Blazemark.
    (default: 'all')
    (a comma separated list)

### [perfkitbenchmarker.linux_benchmarks.block_storage_workloads_benchmark ](../perfkitbenchmarker/linux_benchmarks/block_storage_workloads_benchmark.py)

#### Description:

Runs fio benchmarks to simulate logging, database and streaming.

Man: http://manpages.ubuntu.com/manpages/natty/man1/fio.1.html
Quick howto: http://www.bluestop.org/fio/HOWTO.txt

Simulated logging benchmark does the following things (PD only):
0) Do NOT use direct IO for any tests below, simply go through the FS.
1) Sequentially write x GB with queue depth equal to 8, where x is decided by
   the test VM's total memory. (A larger VM will write more bytes)
2) Random read of 10% of the bytes written.
3) Sequential read of all of the bytes written.

Simulated database benchmark does the following things (PD, PD-SSD, local SSD):
1) 4K Random R on a file using queue depths 1, 16 and 64 (each queue depth
   is a different benchmark).
2) 4K Random W on a file using queue depths 1, 16 and 64 (each queue depth
   is a different benchmark).
3) 4K Random 90% R/ 10% W on a file using queue depths 1, 16 and 64 (each
   queue depth is a different benchmark).
4) The size of the test file is decided by the test VM's total memory and capped
   at 1GB to ensure this test finishes within reasonable time.

Simulated streaming benchmark (PD only):
1) 1M Seq R at queue depth 1 and 16 (streaming).
2) 1M Seq W at queue depth 1 and 16 (streaming).

For AWS, where use PD, we should use EBS-GP and EBS Magnetic, for PD-SSD use
EBS-GP and PIOPS.


#### Flags:

`--iodepth_list`: A list of iodepth parameter used by fio command in simulated
    database and streaming scenarios only.
    (default: '')
    (a comma separated list)

`--maxjobs`: The maximum allowed number of jobs to support.
    (default: '0')
    (an integer)

`--workload_mode`: <logging|database|streaming>: Simulate a logging, database or
    streaming scenario.
    (default: 'logging')

### [perfkitbenchmarker.linux_benchmarks.cassandra_stress_benchmark ](../perfkitbenchmarker/linux_benchmarks/cassandra_stress_benchmark.py)

#### Description:

Runs cassandra.

Cassandra homepage: http://cassandra.apache.org
cassandra-stress tool page:
http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html


#### Flags:

`--cassandra_stress_command`:
    <write|counter_write|user|read|counter_read|mixed>: cassandra-stress command
    to use.
    (default: 'write')

`--cassandra_stress_consistency_level`:
    <ONE|QUORUM|LOCAL_ONE|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY>: Set the consistency
    level to use during cassandra-stress.
    (default: 'QUORUM')

`--cassandra_stress_mixed_ratio`: Read/write ratio of cassandra-stress. Only
    valid if --cassandra_stress_command=mixed. By default, 50% read and 50%
    write.
    (default: 'write=1,read=1')

`--cassandra_stress_operations`: Specify what operations (inserts and/or
    queries) to run and the ratio of each operation. Only valid if
    --cassandra_stress_command=user.
    (default: 'insert=1')

`--cassandra_stress_population_distribution`: <EXP|EXTREME|QEXTREME|GAUSSIAN|UNI
    FORM|~EXP|~EXTREME|~QEXTREME|~GAUSSIAN|~UNIFORM>: The population
    distribution cassandra-stress uses. By default, each loader vm is given a
    range of keys [min, max], and loaders will read/insert keys sequentially
    from min to max.

`--cassandra_stress_population_parameters`: Additional parameters to use with
    distribution. This benchmark will calculate min, max for each distribution.
    Some distributions need more parameters. See: "./cassandra-stress help -pop"
    for more details. Comma-separated list.
    (default: '')
    (a comma separated list)

`--cassandra_stress_population_size`: The size of the population across all
    clients. By default, the size of the population equals to
    max(num_keys,cassandra_stress_preload_num_keys).
    (an integer)

`--cassandra_stress_preload_num_keys`: Number of keys to preload into cassandra
    database. Read/counter_read/mixed modes require preloading cassandra
    database. If not set, the number of the keys preloaded will be the same as
    --num_keys for read/counter_read/mixed mode, the same as the number of
    loaders for write/counter_write/user mode.
    (an integer)

`--cassandra_stress_profile`: Path to cassandra-stress profile file. Only valid
    if --cassandra_stress_command=user.
    (default: '')

`--cassandra_stress_replication_factor`: Number of replicas.
    (default: '3')
    (an integer)

`--cassandra_stress_retries`: Number of retries when error encountered during
    stress.
    (default: '1000')
    (an integer)

`--num_cassandra_stress_threads`: Number of threads used in cassandra-stress
    tool on each loader node.
    (default: '150')
    (an integer)

`--num_keys`: Number of keys used in cassandra-stress tool across all loader
    vms. If unset, this benchmark will use 2000000 * NumCpusForBenchmark() on
    data nodes as the value.
    (default: '0')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.ch_block_storage_benchmark ](../perfkitbenchmarker/linux_benchmarks/ch_block_storage_benchmark.py)

#### Description:

Runs a cloudharmony benchmark.

See https://github.com/cloudharmony/block-storage for more info.


#### Flags:

`--ch_block_tests`: A list of tests supported by CloudHarmony block storage
    benchmark.;
    repeat this option to specify a list of values
    (default: "['iops']")

### [perfkitbenchmarker.linux_benchmarks.cloud_bigtable_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloud_bigtable_ycsb_benchmark.py)

#### Description:

Runs YCSB against Cloud Bigtable.

Cloud Bigtable (https://cloud.google.com/bigtable/) is a managed NoSQL database
with an HBase-compatible API.

Compared to hbase_ycsb, this benchmark:
  * Modifies hbase-site.xml to work with Cloud Bigtable.
  * Adds the Bigtable client JAR.
  * Adds netty-tcnative-boringssl, used for communication with Bigtable.


#### Flags:

`--google_bigtable_admin_endpoint`: Google API endpoint for Cloud Bigtable table
    administration.
    (default: 'bigtableadmin.googleapis.com')

`--google_bigtable_endpoint`: Google API endpoint for Cloud Bigtable.
    (default: 'bigtable.googleapis.com')

`--google_bigtable_hbase_jar_url`: URL for the Bigtable-HBase client JAR.
    (default: 'https://oss.sonatype.org/service/local/repositories/releases/cont
    ent/com/google/cloud/bigtable/bigtable-hbase-1.x-hadoop/1.4.0/bigtable-
    hbase-1.x-hadoop-1.4.0.jar')

`--google_bigtable_instance_name`: Bigtable instance name. If not specified, new
    instance will be created and deleted on the fly.

### [perfkitbenchmarker.linux_benchmarks.cloud_datastore_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloud_datastore_ycsb_benchmark.py)

#### Description:

Run YCSB benchmark against Google Cloud Datastore

Before running this benchmark, you have to download your P12
service account private key file to local machine, and pass the path
via 'google_datastore_keyfile' parameters to PKB.

Service Account email associated with the key file is also needed to
pass to PKB.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Datastore.


#### Flags:

`--google_datastore_datasetId`: The project ID that has Cloud Datastore service

`--google_datastore_debug`: The logging level when running YCSB
    (default: 'false')

`--google_datastore_keyfile`: The path to Google API P12 private key file

`--google_datastore_serviceAccount`: The service account email associated
    withdatastore private key file

### [perfkitbenchmarker.linux_benchmarks.cloud_redis_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloud_redis_ycsb_benchmark.py)

#### Description:

Runs the YCSB benchmark against managed Redis services.

Spins up a cloud redis instance, runs YCSB against it, then spins it down.


#### Flags:

`--redis_region`: The region to spin up cloud redis in.
    (default: 'us-central1')

### [perfkitbenchmarker.linux_benchmarks.cloud_spanner_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloud_spanner_ycsb_benchmark.py)

#### Description:

Run YCSB benchmark against Google Cloud Spanner

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Spanner. Configure the number of VMs via --ycsb_client_vms.


#### Flags:

`--cloud_spanner_ycsb_batchinserts`: The Cloud Spanner batch inserts used in the
    YCSB benchmark.
    (default: '1')
    (an integer)

`--cloud_spanner_ycsb_boundedstaleness`: The Cloud Spanner bounded staleness
    used in the YCSB benchmark.
    (default: '0')
    (an integer)

`--cloud_spanner_ycsb_custom_release`: If provided, the URL of a custom YCSB
    release

`--cloud_spanner_ycsb_custom_vm_install_commands`: A list of strings. If
    specified, execute them on every VM during the installation phase.
    (default: '')
    (a comma separated list)

`--cloud_spanner_ycsb_readmode`: <query|read>: The Cloud Spanner read mode used
    in the YCSB benchmark.
    (default: 'query')

### [perfkitbenchmarker.linux_benchmarks.cloudsuite_data_caching_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloudsuite_data_caching_benchmark.py)

#### Description:

Runs the data caching benchmark of Cloudsuite 3.0.

More info: http://cloudsuite.ch/datacaching


#### Flags:

`--cloudsuite_data_caching_memcached_flags`: Flags to be given to memcached.
    (default: '-t 1 -m 2048 -n 550')

`--cloudsuite_data_caching_rps`: Number of requests per second.
    (default: '18000')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.cloudsuite_data_serving_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloudsuite_data_serving_benchmark.py)

#### Description:

Runs the data_serving benchmark of Cloudsuite.

More info: http://cloudsuite.ch/dataserving/


#### Flags:

`--cloudsuite_data_serving_op_count`: Operation count to be executed.
    (default: '1000')
    (a positive integer)

`--cloudsuite_data_serving_rec_count`: Record count in the database.
    (default: '1000')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.cloudsuite_graph_analytics_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloudsuite_graph_analytics_benchmark.py)

#### Description:

Runs the graph analytics benchmark of Cloudsuite.

More info: http://cloudsuite.ch/graphanalytics/


#### Flags:

`--cloudsuite_graph_analytics_worker_mem`: Amount of memory for the worker, in
    gigabytes
    (default: '2')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.cloudsuite_in_memory_analytics_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloudsuite_in_memory_analytics_benchmark.py)

#### Description:

Runs the in-memory analytics benchmark of Cloudsuite.

More info: http://cloudsuite.ch/inmemoryanalytics/


#### Flags:

`--cloudsuite_in_memory_analytics_dataset`: Dataset to use for training.
    (default: '/data/ml-latest-small')

`--cloudsuite_in_memory_analytics_ratings_file`: Ratings file to give the
    recommendation for.
    (default: '/data/myratings.csv')

### [perfkitbenchmarker.linux_benchmarks.cloudsuite_web_search_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloudsuite_web_search_benchmark.py)

#### Description:

Runs the Web Search benchmark of Cloudsuite.

More info: http://cloudsuite.ch/websearch/


#### Flags:

`--cloudsuite_web_search_ramp_down`: Benchmark ramp down time in seconds.
    (default: '60')
    (a positive integer)

`--cloudsuite_web_search_ramp_up`: Benchmark ramp up time in seconds.
    (default: '90')
    (a positive integer)

`--cloudsuite_web_search_scale`: Number of simulated web search users.
    (default: '50')
    (a positive integer)

`--cloudsuite_web_search_server_heap_size`: Java heap size for Solr server in
    the usual java format.
    (default: '3g')

`--cloudsuite_web_search_steady_state`: Benchmark steady state time in seconds.
    (default: '60')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.cloudsuite_web_serving_benchmark ](../perfkitbenchmarker/linux_benchmarks/cloudsuite_web_serving_benchmark.py)

#### Description:

Runs the web serving benchmark of Cloudsuite.

More info: http://cloudsuite.ch/webserving/


#### Flags:

`--cloudsuite_web_serving_load_scale`: The maximum number of concurrent users
    that can be simulated.
    (default: '100')
    (integer >= 2)

`--cloudsuite_web_serving_pm_max_children`: The maximum number php-fpm pm
    children.
    (default: '150')
    (integer >= 8)

### [perfkitbenchmarker.linux_benchmarks.cluster_boot_benchmark ](../perfkitbenchmarker/linux_benchmarks/cluster_boot_benchmark.py)

#### Description:

Records the time required to boot a cluster of VMs.

#### Flags:

`--[no]cluster_boot_time_reboot`: Whether to reboot the VMs during the cluster
    boot benchmark to measure reboot performance.
    (default: 'false')

### [perfkitbenchmarker.linux_benchmarks.copy_throughput_benchmark ](../perfkitbenchmarker/linux_benchmarks/copy_throughput_benchmark.py)

#### Description:

Runs copy benchmarks.

cp and dd between two attached disks on same vm.
scp copy across different vms using external networks.


#### Flags:

`--copy_benchmark_mode`: <cp|dd|scp>: Runs either cp, dd or scp tests.
    (default: 'cp')

`--copy_benchmark_single_file_mb`: If set, a single file of the specified number
    of MB is used instead of the normal cloud-storage-workload.sh basket of
    files.  Not supported when copy_benchmark_mode is dd
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.coremark_benchmark ](../perfkitbenchmarker/linux_benchmarks/coremark_benchmark.py)

#### Description:

Runs coremark.

From Coremark's documentation:
CoreMark's primary goals are simplicity and providing a method for benchmarking
only a processor's core features.

Coremark homepage: http://www.eembc.org/coremark/


#### Flags:

`--coremark_parallelism_method`: <PTHREAD|FORK|SOCKET>: Method to use for
    parallelism in the Coremark benchmark.
    (default: 'PTHREAD')

### [perfkitbenchmarker.linux_benchmarks.dacapo_benchmark ](../perfkitbenchmarker/linux_benchmarks/dacapo_benchmark.py)

#### Description:

Runs DaCapo benchmarks.

This benchmark runs the various DaCapo benchmarks. More information can be found
at: http://dacapobench.org/


#### Flags:

`--dacapo_benchmark`: <avrora|batik|eclipse|fop|h2|jython|luindex|lusearch|pmd|s
    unflow|tomcat|tradebeans|tradesoap|xalan>: Name of specific DaCapo benchmark
    to execute.
    (default: 'luindex')

`--dacapo_jar_filename`: Filename of DaCapo jar file.
    (default: 'dacapo-9.12-MR1-bach.jar')

`--dacapo_num_iters`: Number of iterations to execute.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.dpb_cluster_boot_benchmark ](../perfkitbenchmarker/linux_benchmarks/dpb_cluster_boot_benchmark.py)

#### Description:

The benchmark reports the latency of creating a dpb cluster.

#### Flags:

`--dpb_cluster_boot_fs`: <gs|s3|hdfs>: File System to use in the dpb cluster
    boot benchmark
    (default: 'gs')

`--dpb_cluster_boot_fs_type`: <ephemeral|persistent>: File System to use in dpb
    cluster boot benchmark
    (default: 'ephemeral')

### [perfkitbenchmarker.linux_benchmarks.dpb_distcp_benchmark ](../perfkitbenchmarker/linux_benchmarks/dpb_distcp_benchmark.py)

#### Description:

Perform distributed copy of data on data processing backends.
Apache Hadoop MapReduce distcp is an open-source tool used to copy large
amounts of data. DistCp is very efficient because it uses MapReduce to copy the
files or datasets and this means the copy operation is distributed across
multiple nodes in a cluster.
Benchmark to compare the performance of of the same distcp workload on clusters
of various cloud providers.


#### Flags:

`--distcp_dest_fs`: <gs|s3|hdfs>: File System to use as destination of the
    distcp operation
    (default: 'gs')

`--distcp_file_size_mbs`: File size to use for each of the distcp source files
    (default: '10')
    (an integer)

`--distcp_num_files`: Number of distcp source files
    (default: '10')
    (an integer)

`--distcp_source_fs`: <gs|s3|hdfs>: File System to use as the source of the
    distcp operation
    (default: 'gs')

### [perfkitbenchmarker.linux_benchmarks.dpb_sparksql_benchmark ](../perfkitbenchmarker/linux_benchmarks/dpb_sparksql_benchmark.py)

#### Description:

Executes query on Spark SQL and records the latency.

The Data (TPCDS or TPCH) needs be generated first by user.
TPCDS and TPCH tools.
TPCDS: https://github.com/databricks/tpcds-kit
TPCH: https://github.com/databricks/tpch-dbgen

This benchmark uses queries from https://github.com/databricks/spark-sql-perf.
Because spark SQL doesn't support all the queries that using dialect netezza.


#### Flags:

`--dpb_sparksql_data`: The dataset to run Spark SQL query

`--dpb_sparksql_order`: The order of query templates in each query stream.
    (default: '')
    (a comma separated list)

`--dpb_sparksql_query`: <tpcds_2_4|tpch>: A list of query to run on
    dpb_sparksql_data
    (default: 'tpcds_2_4')

### [perfkitbenchmarker.linux_benchmarks.dpb_terasort_benchmark ](../perfkitbenchmarker/linux_benchmarks/dpb_terasort_benchmark.py)

#### Description:

Executes the 3 phases of Teasort phases on a Apache Hadoop MapReduce cluster.

TeraSort is a popular benchmark that measures the amount of time to sort a
configured amount of randomly distributed data on a given cluster. It is
commonly used to measure MapReduce performance of an Apache Hadoop cluster.
The following report compares performance of a YARN-scheduled TeraSort job on

A full TeraSort benchmark run consists of the following three steps:

* Generating the input data via TeraGen.
* Running the actual TeraSort on the input data.
* Validating the sorted output data via TeraValidate.

The benchmark reports the detailed latency of executing each phase.


#### Flags:

`--dpb_terasort_num_records`: Number of 100-byte rows to generate.
    (default: '10000')
    (an integer)

`--[no]dpb_terasort_pre_cleanup`: Cleanup the terasort directories on the
    specified filesystem.
    (default: 'false')

`--dpb_terasort_storage_type`: <ephemeral|persistent>: The type of storage for
    executing the Terasort benchmark
    (default: 'persistent')

### [perfkitbenchmarker.linux_benchmarks.dpb_testdfsio_benchmark ](../perfkitbenchmarker/linux_benchmarks/dpb_testdfsio_benchmark.py)

#### Description:

Perform Distributed i/o benchmark on data processing backends.
This test writes into and then subsequently reads a specified number of
files. File size is also specified as a parameter to the test.
The benchmark implementation accepts list of arguments for both the above
parameters and generates one sample for each cross product of the two
parameter values. Each file is accessed in a separate map task.


#### Flags:

`--dfsio_file_sizes_list`: A list of file sizes to use for each of the dfsio
    files.
    (default: '1')
    (a comma separated list)

`--dfsio_fs`: <gs|s3|hdfs>: File System to use in the dfsio operations
    (default: 'gs')

`--dfsio_num_files_list`: A list of number of dfsio files to use during
    individual runs.
    (default: '10')
    (a comma separated list)

### [perfkitbenchmarker.linux_benchmarks.dpb_wordcount_benchmark ](../perfkitbenchmarker/linux_benchmarks/dpb_wordcount_benchmark.py)

#### Description:

Runs the word count job on data processing backends.

WordCount example reads text files and counts how often words occur. The input
is text files and the output is text files, each line of which contains a word
and the count of how often it occurs, separated by a tab.
The disk size parameters that are being passed as part of vm_spec are actually
used as arguments to the dpb service creation commands and the concrete
implementations (dataproc, emr, dataflow, etc.) control using the disk size
during the cluster setup.

dpb_wordcount_out_base: The output directory to capture the word count results

For dataflow jobs, please build the dpb_job_jarfile based on
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven


#### Flags:

`--dpb_wordcount_fs`: <gs|s3>: File System to use for the job output
    (default: 'gs')

`--dpb_wordcount_input`: Input for word count

`--dpb_wordcount_out_base`: Base directory for word count output

### [perfkitbenchmarker.linux_benchmarks.edw_benchmark ](../perfkitbenchmarker/linux_benchmarks/edw_benchmark.py)

#### Description:

Runs Enterprise Data Warehouse (edw) performance benchmarks.

This benchmark adds the ability to run arbitrary sql workloads on hosted fully
managed data warehouse solutions such as Redshift and BigQuery.


#### Flags:

`--edw_benchmark_script`: Path to the sql script.

### [perfkitbenchmarker.linux_benchmarks.fio_benchmark ](../perfkitbenchmarker/linux_benchmarks/fio_benchmark.py)

#### Description:

Runs fio benchmarks.

Man: http://manpages.ubuntu.com/manpages/natty/man1/fio.1.html
Quick howto: http://www.bluestop.org/fio/HOWTO.txt


#### Flags:

`--[no]fio_bw_log`: Whether to collect a bandwidth log of the fio jobs.
    (default: 'false')

`--fio_fill_size`: The amount of device to fill in prepare stage. The valid
    value can either be an integer, which represents the number of bytes to fill
    or a percentage, which represents the percentage of the device. A filesystem
    will be unmounted before filling and remounted afterwards. Only valid when
    --fio_target_mode is against_device_with_fill or against_file_with_fill.
    (default: '100%')

`--fio_generate_scenarios`: Generate a job file with the given scenarios.
    Special scenario 'all' generates all scenarios. Available scenarios are
    sequential_write, sequential_read, random_write, and random_read. Cannot use
    with --fio_jobfile.
    (default: '')
    (a comma separated list)

`--[no]fio_hist_log`: Whether to collect clat histogram.
    (default: 'false')

`--fio_io_depths`: IO queue depths to run on. Can specify a single number, like
    --fio_io_depths=1, a range, like --fio_io_depths=1-4, or a list, like
    --fio_io_depths=1-4,6-8
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--[no]fio_iops_log`: Whether to collect an IOPS log of the fio jobs.
    (default: 'false')

`--fio_jobfile`: Job file that fio will use. If not given, use a job file
    bundled with PKB. Cannot use with --fio_generate_scenarios.

`--[no]fio_lat_log`: Whether to collect a latency log of the fio jobs.
    (default: 'false')

`--fio_log_avg_msec`: By default, this will average each log entry in the fio
    latency, bandwidth, and iops logs over the specified period of time in
    milliseconds. If set to 0, fio will log an entry for every IO that
    completes, this can grow very quickly in size and can cause performance
    overhead.
    (default: '1000')
    (a non-negative integer)

`--fio_log_hist_msec`: Same as fio_log_avg_msec, but logs entries for completion
    latency histograms. If set to 0, histogram logging is disabled.
    (default: '1000')
    (an integer)

`--fio_num_jobs`: Number of concurrent fio jobs to run.
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--fio_parameters`: Parameters to apply to all PKB generated fio jobs. Each
    member of the list should be of the form "param=value".
    (default: 'randrepeat=0')
    (a comma separated list)

`--fio_runtime`: The number of seconds to run each fio job for.
    (default: '600')
    (a positive integer)

`--fio_target_mode`: <against_device_with_fill|against_device_without_fill|again
    st_file_with_fill|against_file_without_fill>: Whether to run against a raw
    device or a file, and whether to prefill.
    (default: 'against_file_without_fill')

`--fio_working_set_size`: The size of the working set, in GB. If not given, use
    the full size of the device. If using --fio_generate_scenarios and not
    running against a raw device, you must pass --fio_working_set_size.
    (a non-negative integer)

### [perfkitbenchmarker.linux_benchmarks.glibc_benchmark ](../perfkitbenchmarker/linux_benchmarks/glibc_benchmark.py)

#### Description:

Runs Glibc Microbenchmark.

The glibc microbenchmark suite automatically generates code for specified
functions, builds and calls them repeatedly for given inputs to give some
basic performance properties of the function.

Homepage: https://fossies.org/linux/glibc/benchtests/README

Installs glibc (see linux_packages/glibc.py for the version). The benchmark
needs python 2.7 or later in addition to the dependencies required to build the
GNU C Library.


#### Flags:

`--glibc_benchset`: By default, it will run the whole set of benchmarks. To run
    only a subset of benchmarks, one may set "glibc_benchset = bench-math bench-
    pthread" by using the flag on the command line multiple times.;
    repeat this option to specify a list of values
    (default: "['bench-math', 'bench-pthread', 'bench-string', 'string-
    benchset', 'wcsmbs-benchset', 'stdlib-benchset', 'stdio-common-benchset',
    'math-benchset', 'malloc-thread']")

### [perfkitbenchmarker.linux_benchmarks.gpu_pcie_bandwidth_benchmark ](../perfkitbenchmarker/linux_benchmarks/gpu_pcie_bandwidth_benchmark.py)

#### Description:

Runs NVIDIA's CUDA PCI-E bandwidth test
      (https://developer.nvidia.com/cuda-code-samples)


#### Flags:

`--gpu_pcie_bandwidth_iterations`: number of iterations to run
    (default: '30')
    (a positive integer)

`--gpu_pcie_bandwidth_mode`: <quick|range>: bandwidth test mode to use. If range
    is selected, provide desired range in flag
    gpu_pcie_bandwidth_transfer_sizes. Additionally, if range is selected, the
    resulting bandwidth will be averaged over all provided transfer sizes.
    (default: 'quick')

`--gpu_pcie_bandwidth_transfer_sizes`: range of transfer sizes to use in bytes.
    Only used if gpu_pcie_bandwidth_mode is set to range
    (default: '67108864,1073741824,67108864')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

### [perfkitbenchmarker.linux_benchmarks.hadoop_terasort_benchmark ](../perfkitbenchmarker/linux_benchmarks/hadoop_terasort_benchmark.py)

#### Description:

Runs a jar using a cluster that supports Apache Hadoop MapReduce.

This benchmark takes runs the Apache Hadoop MapReduce Terasort benchmark on an
Hadoop YARN cluster. The cluster can be one supplied by a cloud provider,
such as Google's Dataproc or Amazon's EMR.

It records how long each phase (generate, sort, validate) takes to run.
For each phase, it reports the wall clock time, but this number should
be used with caution, as it some platforms (such as AWS's EMR) use polling
to determine when the job is done, so the wall time is inflated
Furthermore, if the standard output of the job is retrieved, AWS EMR's
time is again inflated because it takes extra time to get the output.

If available, it will also report a pending time (the time between when the
job was received by the platform and when it ran), and a runtime, which is
the time the job took to run, as reported by the underlying cluster.

For more on Apache Hadoop, see: http://hadoop.apache.org/


#### Flags:

`--[no]terasort_append_timestamp`: Append a timestamp to the directories given
    by terasort_unsorted_dir, terasort_sorted_dir, and terasort_validate_dir
    (default: 'true')

`--terasort_data_base`: The benchmark will append to this to create three
    directories: one for the generated, unsorted data, one for the sorted data,
    and one for the validate data.  If using a static cluster or if using object
    storage buckets, you must cleanup.
    (default: 'terasort_data/')

`--terasort_num_rows`: Number of 100-byte rows to generate.
    (default: '10000')
    (an integer)

`--terasort_unsorted_dir`: Location of the unsorted data. TeraGen writes here,
    and TeraSort reads from here.
    (default: 'tera_gen_data')

### [perfkitbenchmarker.linux_benchmarks.hbase_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/hbase_ycsb_benchmark.py)

#### Description:

Runs YCSB against HBase.


HBase is a scalable NoSQL database built on Hadoop.
https://hbase.apache.org/

A running installation consists of:
  * An HDFS NameNode.
  * HDFS DataNodes.
  * An HBase master node.
  * HBase regionservers.
  * A zookeeper cluster (https://zookeeper.apache.org/).

See: http://hbase.apache.org/book.html#_distributed.

This benchmark provisions:
  * A single node functioning as HDFS NameNode, HBase master, and zookeeper
    quorum member.
  * '--num_vms - 1' nodes serving as both HDFS DataNodes and HBase region
    servers (so region servers and data are co-located).
By default only the master node runs Zookeeper. Some regionservers may be added
to the zookeeper quorum with the --hbase_zookeeper_nodes flag.


HBase web UI on 15030.
HDFS web UI on  50070.


#### Flags:

`--[no]hbase_use_snappy`: Whether to use snappy compression.
    (default: 'true')

`--hbase_zookeeper_nodes`: Number of Zookeeper nodes.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.horovod_benchmark ](../perfkitbenchmarker/linux_benchmarks/horovod_benchmark.py)

#### Description:

Run Horovod distributed Tensorflow Training benchmark.

#### Flags:

`--horovod_batch_size`: Batch size per compute device.
    (default: '64')
    (an integer)

`--horovod_deep_learning_examples_commit`: Commit hash of the AWS deep learning
    samples github repo to use for the benchmark.
    (default: '599adf2')

`--horovod_model`: <resnet18|resnet34|resnet50|resnet101|resnet152>: name of the
    model to run.
    (default: 'resnet50')

`--horovod_num_epochs`: Number of epochs to train for.
    (default: '10')
    (an integer)

`--horovod_precision`: <fp16|fp32>: Precision.
    (default: 'fp16')

`--[no]horovod_synthetic`: Whether to use synthetic data.
    (default: 'true')

`--[no]horovod_using_deep_learning_image`: Whether the VM under test is using a
    deep learning image. This will case PKB to skip the installation of Horovod
    and its dependencies.
    (default: 'false')

### [perfkitbenchmarker.linux_benchmarks.hpcc_benchmark ](../perfkitbenchmarker/linux_benchmarks/hpcc_benchmark.py)

#### Description:

Runs HPC Challenge.

Homepage: http://icl.cs.utk.edu/hpcc/

Most of the configuration of the HPC-Challenge revolves around HPL, the rest of
the HPCC piggybacks upon the HPL configration.

Homepage: http://www.netlib.org/benchmark/hpl/

HPL requires a BLAS library (Basic Linear Algebra Subprograms)
OpenBlas: http://www.openblas.net/
Intel MKL: https://software.intel.com/en-us/mkl

HPL also requires a MPI (Message Passing Interface) Library
OpenMPI: http://www.open-mpi.org/

MPI needs to be configured:
Configuring MPI:
http://techtinkering.com/2009/12/02/setting-up-a-beowulf-cluster-using-open-mpi-on-linux/

Once HPL is built the configuration file must be created:
Configuring HPL.dat:
http://www.advancedclustering.com/faq/how-do-i-tune-my-hpldat-file.html
http://www.netlib.org/benchmark/hpl/faqs.html


#### Flags:

`--hpcc_binary`: The path of prebuilt hpcc binary to use. If not provided, this
    benchmark built its own using OpenBLAS.

`--hpcc_mpi_env`: Comma separated list containing environment variables to use
    with mpirun command. e.g.
    MKL_DEBUG_CPU_TYPE=7,MKL_ENABLE_INSTRUCTIONS=AVX512
    (default: '')
    (a comma separated list)

`--hpcc_timeout_hours`: The number of hours to wait for the HPCC binary to
    complete before timing out and assuming it failed.
    (default: '4')
    (an integer)

`--memory_size_mb`: The amount of memory in MB on each machine to use. By
    default it will use the entire system's memory.
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.hpcg_benchmark ](../perfkitbenchmarker/linux_benchmarks/hpcg_benchmark.py)

#### Description:

Run HPCG.

Requires openmpi 1.10.2


#### Flags:

`--hpcg_gpus_per_node`: The number of gpus per node.
    (a positive integer)

`--hpcg_problem_size`: three dimensional problem size for each node. Must
    contain three integers
    (default: '256,256,256')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--hpcg_runtime`: hpcg runtime in seconds
    (default: '60')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.inception3_benchmark ](../perfkitbenchmarker/linux_benchmarks/inception3_benchmark.py)

#### Description:

Run Inception V3 benchmarks.

Tutorials: https://cloud.google.com/tpu/docs/tutorials/inception
Code: https://github.com/tensorflow/tpu/blob/master/models/experimental/inception/inception_v3.py
This benchmark is equivalent to tensorflow_benchmark with the inception3 model
except that this can target TPU.


#### Flags:

`--inception3_epochs_per_eval`: Number of training epochs to run between
    evaluations.
    (default: '2')
    (an integer)

`--inception3_eval_batch_size`: Global (not per-shard) batch size for evaluation
    (default: '1024')
    (an integer)

`--inception3_learning_rate`: Learning rate.
    (default: '0.165')
    (a number)

`--inception3_mode`: <train|eval|train_and_eval>: Mode to run: train, eval,
    train_and_eval
    (default: 'train_and_eval')

`--inception3_save_checkpoints_secs`: Interval (in seconds) at which the model
    data should be checkpointed. Set to 0 to disable.
    (default: '0')
    (an integer)

`--inception3_train_batch_size`: Global (not per-shard) batch size for training
    (default: '1024')
    (an integer)

`--inception3_train_epochs`: Number of epochs use for training.
    (default: '200')
    (a positive integer)

`--inception3_use_data`: <real|fake>: Whether to use real or fake data. If real,
    the data is downloaded from imagenet_data_dir. Otherwise, synthetic data is
    generated.
    (default: 'real')

### [perfkitbenchmarker.linux_benchmarks.ior_benchmark ](../perfkitbenchmarker/linux_benchmarks/ior_benchmark.py)

#### Description:

Runs IOR and mdtest benchmarks.

IOR is a tool used for distributed testing of filesystem performance.
mdtest is used for distributed testing of filesystem metadata performance.

See https://github.com/hpc/ior for more info.


#### Flags:

`--ior_num_procs`: The number of MPI processes to use for IOR.
    (default: '256')
    (an integer)

`--ior_script`: The IOR script to run. See
    https://github.com/hpc/ior/blob/master/doc/sphinx/userDoc/skripts.rst for
    more info.
    (default: 'default_ior_script')

`--mdtest_args`: Command line arguments to be passed to mdtest. Each set of args
    in the list will be run separately.
    (default: '-n 1000 -u')
    (a comma separated list)

`--[no]mdtest_drop_caches`: Whether to drop caches between the
    create/stat/delete phases. If this is set, mdtest will be run 3 times with
    the -C, -T, and -r options and the client page caches will be dropped
    between runs.
    (default: 'true')

`--mdtest_num_procs`: The number of MPI processes to use for mdtest.
    (default: '32')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.iperf_benchmark ](../perfkitbenchmarker/linux_benchmarks/iperf_benchmark.py)

#### Description:

Runs plain Iperf.

Docs:
http://iperf.fr/

Runs Iperf to collect network throughput.


#### Flags:

`--iperf_runtime_in_seconds`: Number of seconds to run iperf.
    (default: '60')
    (a positive integer)

`--iperf_sending_thread_count`: Number of connections to make to the server for
    sending traffic.
    (default: '1')
    (a positive integer)

`--iperf_timeout`: Number of seconds to wait in addition to iperf runtime before
    killing iperf client command.
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.jdbc_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/jdbc_ycsb_benchmark.py)

#### Description:

Run YCSB benchmark against managed SQL databases that support JDBC.

This benchmark does not provision VMs for the corresponding SQL database
cluster. The only VM group is client group that sends requests to specified
DB.

Before running this benchmark, you have to manually create `usertable` as
specified in YCSB JDBC binding.

Tested against Azure SQL database.



#### Flags:

`--jdbc_ycsb_db_batch_size`: The batch size for doing batched insert.
    (default: '0')
    (an integer)

`--jdbc_ycsb_db_driver`: The class of JDBC driver that connects to DB.

`--jdbc_ycsb_db_driver_path`: The path to JDBC driver jar file on local machine.

`--jdbc_ycsb_db_passwd`: The password of specified DB user.

`--jdbc_ycsb_db_url`: The URL that is used to connect to DB

`--jdbc_ycsb_db_user`: The username of target DB.

`--jdbc_ycsb_fetch_size`: The JDBC fetch size hinted to driver
    (default: '10')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.lmbench_benchmark ](../perfkitbenchmarker/linux_benchmarks/lmbench_benchmark.py)

#### Description:

Run LMbench.

Suite of simple, portable benchmarks. Compares different systems performance.
Homepage: http://www.bitmover.com/lmbench/index.html


#### Flags:

`--lmbench_hardware`: <YES|NO>: The decision to run BENCHMARK_HARDWARE tests:
    YES or NO. The default is NO
    (default: 'NO')

`--lmbench_mem_size`: The range of memory on which several benchmarks operate.
    If not provided, the memory size should be 8MB as default
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.memcached_memtier_benchmark ](../perfkitbenchmarker/linux_benchmarks/memcached_memtier_benchmark.py)

#### Description:

Runs memtier benchmark against memcached on cloud virtual machines.

Memcached is an in-memory key-value store for small chunks of arbitrary
data (strings, objects) from results of database calls, API calls, or page
rendering.
Memcached homepage: https://memcached.org/

Memtier_benchmark is a load generator created by RedisLabs to benchmark
NoSQL key-value databases.

Memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
Memtier_benchmark usage:
https://redislabs.com/blog/memtier_benchmark-a-high-throughput-benchmarking-tool-for-redis-memcached/


#### Flags:

`--memcached_memtier_client_machine_type`: Machine type to use for the memtier
    client if different from memcached server machine type.

`--memcached_memtier_server_machine_type`: Machine type to use for the memtier
    server if different from memcached client machine type.

### [perfkitbenchmarker.linux_benchmarks.memcached_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/memcached_ycsb_benchmark.py)

#### Description:

Runs YCSB against different memcached-like offerings.

This benchmark runs two workloads against memcached using YCSB (the Yahoo! Cloud
Serving Benchmark).
memcached is described in perfkitbenchmarker.linux_packages.memcached_server
YCSB and workloads described in perfkitbenchmarker.linux_packages.ycsb.


#### Flags:

`--memcached_elasticache_node_type`: <cache.t2.micro|cache.t2.small|cache.t2.med
    ium|cache.m3.medium|cache.m3.large|cache.m3.xlarge|cache.m3.2xlarge|cache.m4
    .large|cache.m4.xlarge|cache.m4.2xlarge|cache.m4.4xlarge|cache.m4.10xlarge>:
    The node type to use for AWS ElastiCache memcached servers.
    (default: 'cache.m3.medium')

`--memcached_elasticache_num_servers`: The number of memcached instances for AWS
    ElastiCache.
    (default: '1')
    (an integer)

`--memcached_elasticache_region`: <ap-northeast-1|ap-northeast-2|ap-southeast-1
    |ap-southeast-2|ap-south-1|cn-north-1|eu-central-1|eu-west-1|us-gov-west-1
    |sa-east-1|us-east-1|us-east-2|us-west-1|us-west-2>: The region to use for
    AWS ElastiCache memcached servers.
    (default: 'us-west-1')

`--memcached_managed`: <GCP|AWS>: Managed memcached provider (GCP/AWS) to use.
    (default: 'GCP')

`--memcached_scenario`: <custom|managed>: select one scenario to run:
    custom: Provision VMs and install memcached ourselves.
    managed: Use the specified provider's managed memcache.
    (default: 'custom')

### [perfkitbenchmarker.linux_benchmarks.mesh_network_benchmark ](../perfkitbenchmarker/linux_benchmarks/mesh_network_benchmark.py)

#### Description:

Runs mesh network benchmarks.

Runs TCP_RR, TCP_STREAM benchmarks from netperf and compute total throughput
and average latency inside mesh network.


#### Flags:

`--num_connections`: Number of connections between each pair of vms.
    (default: '1')
    (an integer)

`--num_iterations`: Number of iterations for each run.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.mlperf_benchmark ](../perfkitbenchmarker/linux_benchmarks/mlperf_benchmark.py)

#### Description:

Run MLPerf benchmarks.

#### Flags:

`--mlperf_benchmark`: <resnet>: MLPerf benchmark test to run.
    (default: 'resnet')

### [perfkitbenchmarker.linux_benchmarks.mnist_benchmark ](../perfkitbenchmarker/linux_benchmarks/mnist_benchmark.py)

#### Description:

Run MNIST benchmarks.

#### Flags:

`--imagenet_data_dir`: Directory where the input data is stored
    (default: 'gs://cloud-tpu-test-datasets/fake_imagenet')

`--imagenet_num_eval_images`: Size of ImageNet validation data set.
    (default: '50000')
    (an integer)

`--imagenet_num_train_images`: Size of ImageNet training data set.
    (default: '1281167')
    (an integer)

`--mnist_batch_size`: Mini-batch size for the training. Note that this is the
    global batch size and not the per-shard batch.
    (default: '1024')
    (an integer)

`--mnist_data_dir`: mnist train file for tensorflow

`--mnist_eval_epochs`: Total number of evaluation epochs. If `0`, evaluation
    after training is skipped.
    (default: '1')
    (an integer)

`--mnist_num_eval_images`: Size of MNIST validation data set.
    (default: '5000')
    (an integer)

`--mnist_num_train_images`: Size of MNIST training data set.
    (default: '55000')
    (an integer)

`--mnist_train_epochs`: Total number of training echos
    (default: '37')
    (a positive integer)

`--t2t_data_dir`: Directory where the input data is stored for tensor2tensor

`--tpu_iterations`: Number of iterations per TPU training loop.
    (default: '500')
    (an integer)

`--tpu_precision`: <bfloat16|float32>: Precision to use
    (default: 'bfloat16')

### [perfkitbenchmarker.linux_benchmarks.mongodb_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/mongodb_ycsb_benchmark.py)

#### Description:

Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki


#### Flags:

`--mongodb_readahead_kb`: Configure block device readahead settings.
    (an integer)

`--mongodb_writeconcern`: MongoDB write concern.
    (default: 'acknowledged')

### [perfkitbenchmarker.linux_benchmarks.multichase_benchmark ](../perfkitbenchmarker/linux_benchmarks/multichase_benchmark.py)

#### Description:

Runs a benchmark from the multichase benchmark suite.

multichase is a pointer chaser benchmark. It measures the average latency of
pointer-chase operations.

multichase codebase: https://github.com/google/multichase


#### Flags:

`--multichase_additional_flags`: Additional flags to use when executing
    multichase. Example: '-O 16 -y'.
    (default: '')

`--multichase_chase_arg`: Argument to refine the chase type specified with
    --multichase_chase_type. Applicable for the following types: critword,
    critword2, work.
    (default: '1')
    (an integer)

`--multichase_chase_type`: <critword|critword2|incr|movdqa|movntdqa|nta|parallel
    10|parallel2|parallel3|parallel4|parallel5|parallel6|parallel7|parallel8|par
    allel9|simple|t0|t1|t2|work>: Chase type to use when executing multichase.
    Passed to multichase via its -c flag.
    (default: 'simple')

`--multichase_memory_size_max`: Memory size to use when executing multichase.
    Passed to multichase via its -m flag. If it differs from
    multichase_memory_size_min, then multichase is executed multiple times,
    starting with a memory size equal to the min and doubling while the memory
    size does not exceed the max. Can be specified as a percentage of the total
    memory on the machine.
    (default: '256 mebibyte')
    (An explicit memory size that must be convertible to an integer number of
    bytes (e.g. '7.5 MiB') or a percentage of the total memory rounded down to
    the next integer byte (e.g. '97.5%', which translates to 1046898278 bytes if
    a total of 1 GiB memory is available).)

`--multichase_memory_size_min`: Memory size to use when executing multichase.
    Passed to multichase via its -m flag. If it differs from
    multichase_memory_size_max, then multichase is executed multiple times,
    starting with a memory size equal to the min and doubling while the memory
    size does not exceed the max. Can be specified as a percentage of the total
    memory on the machine.
    (default: '256 mebibyte')
    (An explicit memory size that must be convertible to an integer number of
    bytes (e.g. '7.5 MiB') or a percentage of the total memory rounded down to
    the next integer byte (e.g. '97.5%', which translates to 1046898278 bytes if
    a total of 1 GiB memory is available).)

`--multichase_numactl_options`: If provided, numactl is used to control memory
    placement and process CPU affinity. Examples: "--membind=0" or "--
    cpunodebind=0".

`--multichase_stride_size_max`: Stride size to use when executing multichase.
    Passed to multichase via its -s flag. If it differs from
    multichase_stride_size_min, then multichase is executed multiple times,
    starting with a stride size equal to the min and doubling while the stride
    size does not exceed the max. Can be specified as a percentage of the
    maximum memory (-m flag) of each multichase execution.
    (default: '256 byte')
    (An explicit memory size that must be convertible to an integer number of
    bytes (e.g. '7.5 MiB') or a percentage of the total memory rounded down to
    the next integer byte (e.g. '97.5%', which translates to 1046898278 bytes if
    a total of 1 GiB memory is available).)

`--multichase_stride_size_min`: Stride size to use when executing multichase.
    Passed to multichase via its -s flag. If it differs from
    multichase_stride_size_max, then multichase is executed multiple times,
    starting with a stride size equal to the min and doubling while the stride
    size does not exceed the max. Can be specified as a percentage of the
    maximum memory (-m flag) of each multichase execution.
    (default: '256 byte')
    (An explicit memory size that must be convertible to an integer number of
    bytes (e.g. '7.5 MiB') or a percentage of the total memory rounded down to
    the next integer byte (e.g. '97.5%', which translates to 1046898278 bytes if
    a total of 1 GiB memory is available).)

`--multichase_thread_count`: Number of threads (one per core), to use when
    executing multichase. Passed to multichase via its -t flag.
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

### [perfkitbenchmarker.linux_benchmarks.mxnet_benchmark ](../perfkitbenchmarker/linux_benchmarks/mxnet_benchmark.py)

#### Description:

Run MXnet benchmarks.

(https://github.com/apache/incubator-mxnet/tree/master/example/
image-classification).


#### Flags:

`--mx_batch_size`: The batch size for SGD training.
    (an integer)

`--mx_device`: <cpu|gpu>: Device to use for computation: cpu or gpu
    (default: 'gpu')

`--mx_image_shape`: The image shape that feeds into the network.

`--mx_key_value_store`:
    <local|device|nccl|dist_sync|dist_device_sync|dist_async>: Key-Value store
    types.
    (default: 'device')

`--mx_models`: The network to train
    (default: 'inception-v3,vgg,alexnet,resnet')
    (a comma separated list)

`--mx_num_epochs`: The maximal number of epochs to train.
    (default: '80')
    (an integer)

`--mx_num_layers`: Number of layers in the neural network, required by some
    networks such as resnet
    (an integer)

`--mx_precision`: <float16|float32>: Precision
    (default: 'float32')

### [perfkitbenchmarker.linux_benchmarks.netperf_benchmark ](../perfkitbenchmarker/linux_benchmarks/netperf_benchmark.py)

#### Description:

Runs plain netperf in a few modes.

docs:
http://www.netperf.org/svn/netperf2/tags/netperf-2.4.5/doc/netperf.html#TCP_005fRR
manpage: http://manpages.ubuntu.com/manpages/maverick/man1/netperf.1.html

Runs TCP_RR, TCP_CRR, and TCP_STREAM benchmarks from netperf across two
machines.


#### Flags:

`--netperf_benchmarks`: The netperf benchmark(s) to run.
    (default: 'TCP_RR,TCP_CRR,TCP_STREAM,UDP_RR')
    (a comma separated list)

`--[no]netperf_enable_histograms`: Determines whether latency histograms are
    collected/reported. Only for *RR benchmarks
    (default: 'true')

`--netperf_max_iter`: Maximum number of iterations to run during confidence
    interval estimation. If unset, a single iteration will be run.
    (an integer in the range [3, 30])

`--netperf_num_streams`: Number of netperf processes to run. Netperf will run
    once for each value in the list.
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--netperf_test_length`: netperf test length, in seconds
    (default: '60')
    (a positive integer)

`--netperf_thinktime`: Time in nanoseconds to do work for each request.
    (default: '0')
    (an integer)

`--netperf_thinktime_array_size`: The size of the array to traverse for
    thinktime.
    (default: '0')
    (an integer)

`--netperf_thinktime_run_length`: The number of contiguous numbers to sum at a
    time in the thinktime array.
    (default: '0')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.nginx_benchmark ](../perfkitbenchmarker/linux_benchmarks/nginx_benchmark.py)

#### Description:

Runs HTTP load generators against an Nginx server.

#### Flags:

`--nginx_conf`: The path to an Nginx config file that should be applied to the
    server instead of the default one.

`--nginx_content_size`: The size of the content Nginx will serve in bytes.
    (default: '10000')
    (an integer)

`--nginx_load_configs`: For each load spec in the list, wrk2 will be run once
    against Nginx with those parameters. The format is
    "target_request_rate:duration:threads:connections", with each value being
    per client (so running with 2 clients would double the target rate, threads,
    and connections (but not duration since they are run concurrently)). The
    target request rate is measured in requests per second and the duration is
    measured in seconds.
    (default: '100:60:1:1')
    (a comma separated list)

### [perfkitbenchmarker.linux_benchmarks.object_storage_service_benchmark ](../perfkitbenchmarker/linux_benchmarks/object_storage_service_benchmark.py)

#### Description:

Object (blob) Storage benchmark tests.

There are two categories of tests here: 1) tests based on CLI tools, and 2)
tests that use APIs to access storage provider.

For 1), we aim to simulate one typical use case of common user using storage
provider: upload and downloads a set of files with different sizes from/to a
local directory.

For 2), we aim to measure more directly the performance of a storage provider
by accessing them via APIs. Here are the main scenarios covered in this
category:
  a: Single byte object upload and download, measures latency.
  b: List-after-write and list-after-update consistency measurement.
  c: Single stream large object upload and download, measures throughput.


#### Flags:

`--cli_test_size`: <normal|large>: size of the cli tests. Normal means a mixture
    of various
    object sizes up to 32MiB (see data/cloud-storage-workload.sh).
    Large means all objects are of at least 1GiB.
    (default: 'normal')

`--object_storage_bucket_name`: If set, the bucket will be created with this
    name

`--[no]object_storage_dont_delete_bucket`: If True, the storage bucket won't be
    deleted. Useful for running the api_multistream_reads scenario multiple
    times against the same objects.
    (default: 'false')

`--object_storage_gcs_multiregion`: Storage multiregion for GCS in object
    storage benchmark.

`--object_storage_latency_histogram_interval`: If set, a latency histogram
    sample will be created with buckets of the specified interval in seconds.
    Individual histogram samples are created for each different object size in
    the distribution, because it is easy to aggregate the histograms during
    post-processing, but impossible to go in the opposite direction.
    (a number)

`--object_storage_list_consistency_iterations`: Number of iterations to perform
    for the api_namespace list consistency benchmark. This flag is mainly for
    regression testing in the benchmarks. Reduce the number to shorten the
    execution time of the api_namespace scenario. However, to get useful metrics
    from the api_namespace scenario, a high number of iterations should be used
    (>=200).
    (default: '200')
    (an integer)

`--object_storage_multistream_objects_per_stream`: Number of objects to send
    and/or receive per stream. Only applies to the api_multistream scenario.
    (default: '1000')
    (a positive integer)

`--object_storage_object_naming_scheme`:
    <sequential_by_stream|approximately_sequential>: How objects will be named.
    Only applies to the api_multistream benchmark. sequential_by_stream: object
    names from each stream will be sequential, but different streams will have
    different name prefixes. approximately_sequential: object names from all
    streams will roughly increase together.
    (default: 'sequential_by_stream')

`--object_storage_objects_written_file_prefix`: If specified, the bucket and all
    of the objects will not be deleted, and the list of object names will be
    written to a file with the specified prefix in the following format:
    <bucket>/<object>. This prefix can be passed to this benchmark in a later
    run via via the object_storage_read_objects_prefix flag. Only valid for the
    api_multistream and api_multistream_writes scenarios. The filename is
    appended with the date and time so that later runs can be given a prefix and
    a minimum age of objects. The later run will then use the oldest objects
    available or fail if there is no file with an old enough date. The prefix is
    also appended with the region so that later runs will read objects from the
    same region.

`--object_storage_read_objects_min_hours`: The minimum number of hours from
    which to read objects that were written on a previous run. Used in
    combination with object_storage_read_objects_prefix.
    (default: '72')
    (an integer)

`--object_storage_read_objects_prefix`: If specified, no new bucket or objects
    will be created. Instead, the benchmark will read the objects listed in a
    file with the specified prefix that was written some number of hours before
    (as specifed by object_storage_read_objects_min_hours). Only valid for the
    api_multistream_reads scenario.

`--object_storage_region`: Storage region for object storage benchmark.

`--object_storage_scenario`: <all|cli|api_data|api_namespace|api_multistream|api
    _multistream_writes|api_multistream_reads>: select all, or one particular
    scenario to run:
    ALL: runs all scenarios. This is the default.
    cli: runs the command line only scenario.
    api_data: runs API based benchmarking for data paths.
    api_namespace: runs API based benchmarking for namespace operations.
    api_multistream: runs API-based benchmarking with multiple upload/download
    streams.
    api_multistream_writes: runs API-based benchmarking with multiple upload
    streams.
    (default: 'all')

`--object_storage_storage_class`: Storage class to use in object storage
    benchmark.

`--object_storage_streams_per_vm`: Number of independent streams per VM. Only
    applies to the api_multistream scenario.
    (default: '10')
    (a positive integer)

`--object_storage_worker_output`: If set, the worker threads' output will be
    written to thepath provided.

`--[no]record_individual_latency_samples`: If set, record the latency of each
    download and upload in its own sample.
    (default: 'false')

`--storage`: <GCP|AWS|Azure|OpenStack>: storage provider
    (GCP/AZURE/AWS/OPENSTACK) to use.
    (default: 'GCP')

### [perfkitbenchmarker.linux_benchmarks.oldisim_benchmark ](../perfkitbenchmarker/linux_benchmarks/oldisim_benchmark.py)

#### Description:

Runs oldisim.

oldisim is a framework to support benchmarks that emulate Online Data-Intensive
(OLDI) workloads, such as web search and social networking. oldisim includes
sample workloads built on top of this framework.

With its default config, oldisim models an example search topology. A user query
is first processed by a front-end server, which then eventually fans out the
query to a large number of leaf nodes. The latency is measured at the root of
the tree, and often increases with the increase of fan-out. oldisim reports a
scaling efficiency for a given topology. The scaling efficiency is defined
as queries per second (QPS) at the current fan-out normalized to QPS at fan-out
1 with ISO root latency.

Sample command line:

./pkb.py --benchmarks=oldisim --project='YOUR_PROJECT' --oldisim_num_leaves=4
--oldisim_fanout=1,2,3,4 --oldisim_latency_target=40
--oldisim_latency_metric=avg

The above command will build a tree with one root node and four leaf nodes. The
average latency target is 40ms. The root node will vary the fanout from 1 to 4
and measure the scaling efficiency.


#### Flags:

`--oldisim_fanout`: a list of fanouts to be tested. a root can connect to a
    subset of leaf nodes (fanout). the value of fanout has to be smaller than
    num_leaves.
    (default: '')
    (a comma separated list)

`--oldisim_latency_metric`: <avg|50p|90p|95p|99p|99.9p>: Allowable metrics for
    end-to-end latency
    (default: 'avg')

`--oldisim_latency_target`: latency target in ms
    (default: '30.0')
    (a number)

`--oldisim_num_leaves`: number of leaf nodes
    (default: '4')
    (an integer in the range [1, 64])

### [perfkitbenchmarker.linux_benchmarks.pgbench_benchmark ](../perfkitbenchmarker/linux_benchmarks/pgbench_benchmark.py)

#### Description:

Pgbench benchmark for PostgreSQL databases.

  Pgbench is a TPC-B like database benchmark for Postgres and
  is published by the PostgreSQL group.

  This implementation of pgbench in PKB uses the ManagedRelationalDB
  resource. A client VM is also required. To change the specs of the
  database server, change the vm_spec nested inside
  managed_relational_db spec. To change the specs of the client,
  change the vm_spec nested directly inside the pgbench spec.

  The scale factor can be used to set the size of the test database.
  Additionally, the runtime per step, as well as the number of clients
  at each step can be specified.

  This benchmark is written for pgbench 9.5, which is the default
  (as of 10/2017) version installed on Ubuntu 16.04.


#### Flags:

`--pgbench_client_counts`: array of client counts passed to pgbench
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--pgbench_scale_factor`: scale factor used to fill the database
    (default: '1')
    (a positive integer)

`--pgbench_seconds_per_test`: number of seconds to run each test phase
    (default: '10')
    (a positive integer)

`--pgbench_seconds_to_pause_before_steps`: number of seconds to pause before
    each client load step
    (default: '30')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.ping_benchmark ](../perfkitbenchmarker/linux_benchmarks/ping_benchmark.py)

#### Description:

Runs ping.

This benchmark runs ping using the internal, and optionally external, ips of
vms in the same zone.


#### Flags:

`--[no]ping_also_run_using_external_ip`: If set to True, the ping command will
    also be executed using the external ips of the vms.
    (default: 'false')

### [perfkitbenchmarker.linux_benchmarks.redis_benchmark ](../perfkitbenchmarker/linux_benchmarks/redis_benchmark.py)

#### Description:

Run memtier_benchmark against Redis.

memtier_benchmark is a load generator created by RedisLabs to benchmark
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark


#### Flags:

`--redis_clients`: Number of redis loadgen clients
    (default: '5')
    (an integer)

`--redis_numprocesses`: Number of Redis processes to spawn per processor.
    (default: '1')
    (an integer)

`--redis_setgetratio`: Ratio of reads to write performed by the memtier
    benchmark, default is '1:0', ie: writes only.
    (default: '1:0')

### [perfkitbenchmarker.linux_benchmarks.redis_ycsb_benchmark ](../perfkitbenchmarker/linux_benchmarks/redis_ycsb_benchmark.py)

#### Description:

Run YCSB against Redis.

Redis homepage: http://redis.io/


#### Flags:

`--redis_ycsb_processes`: Number of total ycsb processes across all clients.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.resnet_benchmark ](../perfkitbenchmarker/linux_benchmarks/resnet_benchmark.py)

#### Description:

Run ResNet benchmarks.

Tutorials: https://cloud.google.com/tpu/docs/tutorials/resnet
Code: https://github.com/tensorflow/tpu/tree/master/models/official/resnet
This benchmark is equivalent to tensorflow_benchmark with the resnet model
except that this can target TPU.


#### Flags:

`--resnet_data_format`: <channels_first|channels_last>: A flag to override the
    data format used in the model. The value is either channels_first or
    channels_last. To run the network on CPU or TPU, channels_last should be
    used. For GPU, channels_first will improve performance.
    (default: 'channels_last')

`--resnet_depth`: <18|34|50|101|152|200>: Depth of ResNet model to use. Deeper
    models require more training time and more memory and may require reducing
    --resnet_train_batch_size to prevent running out of memory.
    (default: '50')

`--resnet_epochs_per_eval`: Controls how often evaluation is performed. Since
    evaluation is fairly expensive, it is advised to evaluate as infrequently as
    possible (i.e. up to --train_steps, which evaluates the model only after
    finishing the entire training regime).
    (default: '2')
    (integer >= 2)

`--resnet_eval_batch_size`: Global (not per-shard) batch size for evaluation
    (default: '1024')
    (an integer)

`--resnet_mode`: <train|eval|train_and_eval>: Mode to run: train, eval,
    train_and_eval
    (default: 'train_and_eval')

`--[no]resnet_skip_host_call`: Skip the host_call which is executed every
    training step. This is generally used for generating training summaries
    (train loss, learning rate, etc...). When --skip_host_call=false, there
    could be a performance drop if host_call function is slow and cannot keep up
    with the TPU-side computation.
    (default: 'false')

`--resnet_train_batch_size`: Global (not per-shard) batch size for training
    (default: '1024')
    (an integer)

`--resnet_train_epochs`: The Number of epochs to use for training.
    (default: '90')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.silo_benchmark ](../perfkitbenchmarker/linux_benchmarks/silo_benchmark.py)

#### Description:

Runs Silo.

Silo is a high performance, scalable in-memory database for modern multicore
machines

Documentation & code: https://github.com/stephentu/silo


#### Flags:

`--silo_benchmark`: benchmark to run with silo. Options include tpcc, ycsb,
    queue, bid
    (default: 'tpcc')

### [perfkitbenchmarker.linux_benchmarks.spark_benchmark ](../perfkitbenchmarker/linux_benchmarks/spark_benchmark.py)

#### Description:

Runs a jar using a cluster that supports Apache Spark.

This benchmark takes a jarfile and class name, and runs that class
using an Apache Spark cluster.  The Apache Spark cluster can be one
supplied by a cloud provider, such as Google's Dataproc.

By default, it runs SparkPi.

It records how long the job takes to run.  It always reports the
wall clock time, but this number should be used with caution, as it
some platforms (such as AWS's EMR) use polling to determine when
the job is done, so the wall time is inflated.  Furthermore, if the standard
output of the job is retrieved, AWS EMR's time is again inflated because
it takes extra time to get the output.

If available, it will also report a pending time (the time between when the
job was received by the platform and when it ran), and a runtime, which is
the time the job took to run, as reported by the underlying cluster.

Secondarily, this benchmark can be used be used to run Apache Hadoop MapReduce
jobs if the underlying cluster supports it by setting the spark_job_type flag
to hadoop, eg:
  ./pkb.py --benchmarks=spark --spark_job_type=hadoop \
      --spark_jarfile=file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar\
      --spark_classname=''\
      --spark_job_arguments=bbp,1,1000,10,bbp_dir

For Amazon's EMR service, if the provided jar file has a main class, you
should pass in an empty class name for hadoop jobs.

For more on Apache Spark, see: http://spark.apache.org/
For more on Apache Hadoop, see: http://hadoop.apache.org/


#### Flags:

`--spark_classname`: Classname to be used
    (default: 'org.apache.spark.examples.SparkPi')

`--spark_jarfile`: If none, use the spark sample jar.

`--spark_job_arguments`: Arguments to be passed to the class given by
    spark_classname
    (default: '')
    (a comma separated list)

`--spark_job_type`: <spark|hadoop>: Type of the job to submit.
    (default: 'spark')

`--[no]spark_print_stdout`: Print the standard output of the job
    (default: 'true')

### [perfkitbenchmarker.linux_benchmarks.speccpu2006_benchmark ](../perfkitbenchmarker/linux_benchmarks/speccpu2006_benchmark.py)

#### Description:

Runs SPEC CPU2006.

From the SPEC CPU2006 documentation:
"The SPEC CPU 2006 benchmark is SPEC's next-generation, industry-standardized,
CPU-intensive benchmark suite, stressing a system's processor, memory subsystem
and compiler."

SPEC CPU2006 homepage: http://www.spec.org/cpu2006/


#### Flags:

`--benchmark_subset`: <fp|GemsFDTD|cactusADM|omnetpp|int|mcf|povray|gcc|hmmer|sp
    hinx3|h264ref|milc|perlbench|tonto|bwaves|lbm|gamess|wrf|bzip2|leslie3d|namd
    |gromacs|libquantum|all|xalancbmk|sjeng|calculix|astar|zeusmp|dealII|soplex|
    gobmk>: Used by the PKB speccpu2006 benchmark. Specifies a subset of SPEC
    CPU2006 benchmarks to run.
    (default: 'int')

`--runspec_metric`: <rate|speed>: SPEC test to run. Speed is time-based metric,
    rate is throughput-based metric.
    (default: 'rate')

### [perfkitbenchmarker.linux_benchmarks.speccpu2017_benchmark ](../perfkitbenchmarker/linux_benchmarks/speccpu2017_benchmark.py)

#### Description:

Runs SPEC CPU2017.

From the SPEC CPU2017 documentation:
The SPEC CPU 2017 benchmark package contains SPEC's next-generation,
industry-standardized, CPU intensive suites for measuring and comparing
compute intensive performance, stressing a system's processor,
memory subsystem and compiler.

SPEC CPU2017 homepage: http://www.spec.org/cpu2017/


#### Flags:

`--spec17_copies`: Number of copies to run for rate tests. If not set default to
    number of cpu cores using lscpu.
    (an integer)

`--[no]spec17_fdo`: Run with feedback directed optimization on peak. Default to
    False.
    (default: 'false')

`--spec17_subset`: Specify which speccpu2017 tests to run. Accepts a list of
    benchmark suites (intspeed, fpspeed, intrate, fprate) or individual
    benchmark names. Defaults to all suites.
    (default: 'intspeed,fpspeed,intrate,fprate')
    (a comma separated list)

`--spec17_threads`: Number of threads to run for speed tests. If not set default
    to number of cpu threads using lscpu.
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.specsfs2014_benchmark ](../perfkitbenchmarker/linux_benchmarks/specsfs2014_benchmark.py)

#### Description:

Runs SPEC SFS 2014.

SPEC SFS 2014 homepage: http://www.spec.org/sfs2014/

In order to run this benchmark copy your 'SPECsfs2014_SP1.iso'
and 'netmist_license_key' files into the data/ directory.

TODO: This benchmark should be decoupled from Gluster and allow users
to run against any file server solution. In addition, Gluster should
eventually become a "disk type" so that any benchmark that runs
against a filesystem can run against Gluster.


#### Flags:

`--[no]specsfs2014_auto_mode`: If True, automatically find the max passing score
    for each benchmark. This ignores other flags such as specsfs2014_load,
    specsfs2014_incr_load, and specsfs2014_num_runs.
    (default: 'false')

`--specsfs2014_auto_mode_upper_bound`: The upper bound for specsfs load.
    Relevant when specsfs2014_auto_mode is set to True.
    (default: 'inf')
    (a number)

`--specsfs2014_benchmarks`: The SPEC SFS 2014 benchmarks to run.
    (default: 'VDI,DATABASE,SWBUILD,VDA')
    (a comma separated list)

`--specsfs2014_config`: This flag can be used to specify an alternate SPEC
    config file to use. If this option is specified, none of the other benchmark
    specific flags which operate on the config file will be used (since the
    default config file will be replaced by this one).

`--specsfs2014_incr_load`: The amount to increment "load" by for each run.
    (default: '1')
    (a positive integer)

`--specsfs2014_load`: The starting load in units of SPEC "business metrics". The
    meaning of business metric varies depending on the SPEC benchmark (e.g. VDI
    has load measured in virtual desktops).
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--specsfs2014_num_runs`: The total number of SPEC runs. The load for the nth
    run is "load" + n * "specsfs_incr_load".
    (default: '1')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.stencil2d_benchmark ](../perfkitbenchmarker/linux_benchmarks/stencil2d_benchmark.py)

#### Description:

Runs the Stencil2D benchmark from the SHOC Benchmark Suite

#### Flags:

`--stencil2d_iterations`: number of iterations to run
    (default: '5')
    (a positive integer)

`--stencil2d_problem_sizes`: problem sizes to run. Can specify a single number,
    like --stencil2d_problem_sizes=4096 or a list like
    --stencil2d_problem_sizes=1024,4096
    (default: '4096')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

### [perfkitbenchmarker.linux_benchmarks.stress_ng_benchmark ](../perfkitbenchmarker/linux_benchmarks/stress_ng_benchmark.py)

#### Description:

Runs stress-ng.

From the stress-ng ubuntu documentation:
stress-ng will stress test a computer system in various selectable ways.
It was designed to exercise various physical subsystems of a computer as
well as the various operating system kernel interfaces. stress-ng also has
a wide range of CPU specific stress tests that exercise floating point,
integer, bit manipulation and control flow.

stress-ng manpage:
http://manpages.ubuntu.com/manpages/xenial/man1/stress-ng.1.html


#### Flags:

`--[no]stress_ng_calc_geomean`: Whether to calculate geomean or not.
    (default: 'true')

`--stress_ng_custom_stressors`: List of stressors to run against. Default
    combines cpu,cpu-cache, and memory suites
    (default: '')
    (a comma separated list)

`--stress_ng_duration`: Number of seconds to run the test.
    (default: '10')
    (an integer)

`--stress_ng_thread_workloads`: List of threads sizes to run against. Options
    aresmall (1 thread total), medium (1 thread per 2 cpus), and large (1 thread
    per cpu).
    (default: 'large')
    (a comma separated list)

### [perfkitbenchmarker.linux_benchmarks.sysbench_benchmark ](../perfkitbenchmarker/linux_benchmarks/sysbench_benchmark.py)

#### Description:

MySQL Service Benchmarks.

This is a set of benchmarks that measures performance of MySQL Databases on
managed MySQL services.

- On AWS, we will use RDS+MySQL.
- On GCP, we will use Cloud SQL v2 (Performance Edition).

As other cloud providers deliver a managed MySQL service, we will add it here.

As of May 2017 to make this benchmark run for GCP you must install the
gcloud beta component. This is necessary because creating a Cloud SQL instance
with a non-default storage size is in beta right now. This can be removed when
this feature is part of the default components.
See https://cloud.google.com/sdk/gcloud/reference/beta/sql/instances/create
for more information.
To run this benchmark for GCP it is required to install a non-default gcloud
component. Otherwise this benchmark will fail.

To ensure that gcloud beta is installed, type
        'gcloud components list'
into the terminal. This will output all components and status of each.
Make sure that
  name: gcloud Beta Commands
  id:  beta
has status: Installed.
If not, run
        'gcloud components install beta'
to install it. This will allow this benchmark to properly create an instance.


#### Flags:

`--sysbench_latency_percentile`: The latency percentile we ask sysbench to
    compute.
    (default: '100')
    (an integer)

`--sysbench_post_failover_seconds`: When non Zero, will run the benchmark an
    additional amount of time after failover is complete.  Useful for detecting
    if there are any differences in TPS becauseof failover.
    (default: '0')
    (an integer)

`--sysbench_pre_failover_seconds`: If non zero, then after the sysbench workload
    is complete, a failover test will be performed.  When a failover test is
    run, the database will be driven using the last entry in
    sysbench_thread_counts.  After sysbench_pre_failover_seconds, a failover
    will be triggered.  Time will be measured until sysbench is able to connect
    again.
    (default: '0')
    (an integer)

`--sysbench_report_interval`: The interval, in seconds, we ask sysbench to
    report results.
    (default: '2')
    (an integer)

`--sysbench_run_seconds`: The duration of the actual run in which results are
    collected, in seconds.
    (default: '10')
    (an integer)

`--sysbench_scale`: Scale parameter as used by TPCC benchmark.
    (default: '100')
    (an integer)

`--sysbench_table_size`: The number of rows of each table used in the oltp tests
    (default: '100000')
    (an integer)

`--sysbench_tables`: The number of tables used in sysbench oltp.lua tests
    (default: '4')
    (an integer)

`--sysbench_testname`: The built in oltp lua script to run
    (default: 'oltp_read_write')

`--sysbench_thread_counts`: array of thread counts passed to sysbench, one at a
    time
    (default: '64')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--sysbench_warmup_seconds`: The duration of the warmup run in which results are
    discarded, in seconds.
    (default: '10')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.t2t_benchmark ](../perfkitbenchmarker/linux_benchmarks/t2t_benchmark.py)

#### Description:

Run a Tensor2Tensor benchmark.

Code:
https://github.com/tensorflow/tensor2tensor
This benchmark can run any tensor2tensor model, including ones that target TPU's


#### Flags:

`--t2t_eval_steps`: Number of eval steps
    (default: '1')
    (an integer)

`--t2t_hparams_set`: Tensor2Tensor hyperparameters set

`--t2t_model`: Tensor2Tensor model to run

`--t2t_problem`: Tensor2Tensor problem to run

`--t2t_train_steps`: Number of train steps
    (default: '1000')
    (an integer)

### [perfkitbenchmarker.linux_benchmarks.tensorflow_benchmark ](../perfkitbenchmarker/linux_benchmarks/tensorflow_benchmark.py)

#### Description:

Run Tensorflow benchmarks (https://github.com/tensorflow/benchmarks).

This benchmark suports distributed and non-distributed runs. Distributed
TensorFlow involves splitting the job to different vms/nodes. To train a dataset
using hundreds of GPUs, use distributed TensorFlow. In Distributed TensorFlow,
there is communication between the parameter servers and the workers, and also
between the workers. Each worker process runs the same model. When a worker
needs a variable, it accesses it from the parameter server directly.


#### Flags:

`--tf_batch_sizes`: batch sizes per compute device. If not provided, the
    suggested batch size is used for the given model
    (a comma separated list)

`--tf_benchmark_args`: Arguments (as a string) to pass to tf_cnn_benchmarks.
    This can be used to run a benchmark with arbitrary parameters. Arguments
    will be parsed and added to the sample metadata. For example,
    --tf_benchmark_args="--nodistortions --optimizer=sgd will run
    tf_cnn_benchmarks.py --nodistortions --optimizer=sgd and put the following
    in the metadata: {'nodistortions': 'True', 'optimizer': 'sgd'}. All
    arguments must be in the form --arg_name=value. If there are GPUs on the VM
    and no 'num_gpus' flag in the tf_benchmarks_args flag, the num_gpus flag
    will automatically be populated with the number of available GPUs.

`--tf_data_dir`: Path to dataset in TFRecord format (aka Example protobufs). If
    not specified, synthetic data will be used.

`--tf_data_format`: <NCHW|NHWC>: Data layout to
    use: NHWC (TF native) or NCHW (cuDNN native).
    (default: 'NCHW')

`--tf_data_module`: Data path in preprovisioned data bucket.
    (default: 'tensorflow/ILSVRC2012')

`--tf_data_name`: <imagenet|flowers>: Name of dataset: imagenet or flowers.
    (default: 'imagenet')

`--tf_device`: <cpu|gpu>: Device to use for computation: cpu or gpu
    (default: 'gpu')

`--[no]tf_distortions`: Enable/disable distortions during image preprocessing.
    These include bbox and color distortions.
    (default: 'true')

`--[no]tf_distributed`: Run TensorFlow distributed
    (default: 'false')

`--tf_distributed_port`: The port to use in TensorFlow distributed job
    (default: '2222')

`--[no]tf_forward_only`: whether use forward-only or
    training for benchmarking
    (default: 'false')

`--tf_local_parameter_device`: <cpu|gpu>: Device to use as parameter server: cpu
    or gpu. For
    distributed training, it can affect where caching of
    variables happens.
    (default: 'cpu')

`--tf_models`: name of the models to run
    (default: 'inception3,vgg16,alexnet,resnet50,resnet152')
    (a comma separated list)

`--tf_num_files_train`: The number of files for training
    (default: '1024')
    (an integer)

`--tf_num_files_val`: The number of files for validation
    (default: '128')
    (an integer)

`--tf_precision`: <float16|float32>: Use 16-bit floats for certain tensors
    instead of 32-bit floats. This is currently experimental.
    (default: 'float32')

`--[no]tf_use_local_data`: Whether to use data from local machine. If true, the
    benchmark will use data from cloud storage (GCS, S3, etc).
    (default: 'false')

`--tf_variable_update`:
    <parameter_server|replicated|distributed_replicated|independent>: The method
    for managing variables: parameter_server,
    replicated, distributed_replicated, independent
    (default: 'parameter_server')

### [perfkitbenchmarker.linux_benchmarks.tensorflow_serving_benchmark ](../perfkitbenchmarker/linux_benchmarks/tensorflow_serving_benchmark.py)

#### Description:

Runs an online prediction Tensorflow Serving benchmark.

This benchmark is composed of the following:
  * this file, tensorflow_serving_benchmark.py
  * the package which builds tf serving, linux_packages/tensorflow_serving.py
  * a client workload generator, tensorflow_serving_client_workload.py
  * preprovisioned data consisting of the imagenet 2012 validation images
    and their labels

The benchmark uses two VMs: a server, and a client.
Tensorflow Serving is built from source (in a docker image), which takes
a significant amount of time (45 minutes on an n1-standard-8).
Note that both client and server VMs build the code. This is necessary to build
an optimized binary for the CPU it will be running on.

Once the code is built, the server prepares an ResNet model
for serving. It prepares a pre-trained ResNet model using a publicly
available SavedModel. The server then starts a platform-optimized
tensorflow_model_server binary using the prepared model.

The client VM downloads the imagenet 2012 validation images from cloud storage
and begins running a client-side load generator script which does the
following:
  * launches a specified number of threads
  * each thread chooses a random image from the dataset and sends a prediction
    request to the server, notes the latency, and repeats with a new random
    image
  * once the specified time period is up, the client script prints results
    to stdout, which this benchmark reads and uses to create samples.

When the benchmark is finished, all resources are torn down.


#### Flags:

`--tf_serving_client_thread_counts`: number of client worker threads
    (default: '16,32')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read
    as -1,3,5,6,7.)

`--tf_serving_runtime`: benchmark runtime in seconds
    (default: '60')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.tomcat_wrk_benchmark ](../perfkitbenchmarker/linux_benchmarks/tomcat_wrk_benchmark.py)

#### Description:

Run wrk against a simple Tomcat web server.

This is close to HTTP-RR:

  * Connections are reused.
  * The server does very little work.

Doubles connections up to a fixed count, reports single connection latency and
maximum error-free throughput.

`wrk` is a scalable web load generator.
`tomcat` is a popular Java web server.


#### Flags:

`--tomcat_wrk_max_connections`: Maximum number of simultaneous connections to
    attempt
    (default: '128')
    (a positive integer)

`--[no]tomcat_wrk_report_all_samples`: If true, report throughput/latency at all
    connection counts. If false (the default), report only the connection counts
    with lowest p50 latency and highest throughput.
    (default: 'false')

`--tomcat_wrk_test_length`: Length of time, in seconds, to run wrk for each
    connction count
    (default: '120')
    (a positive integer)

### [perfkitbenchmarker.linux_benchmarks.unixbench_benchmark ](../perfkitbenchmarker/linux_benchmarks/unixbench_benchmark.py)

#### Description:

Runs UnixBench.

Documentation & code: http://code.google.com/p/byte-unixbench/

Unix bench is a holistic performance benchmark, measuing CPU performance,
some memory bandwidth, and disk.


#### Flags:

`--[no]unixbench_all_cores`: Setting this flag changes the default behavior of
    Unix bench. It will now scale to the number of CPUs on the machine vs the
    limit of 16 CPUs today.
    (default: 'false')

### [perfkitbenchmarker.linux_packages.act ](../perfkitbenchmarker/linux_packages/act.py)

#### Description:

Module containing aerospike server installation and cleanup functions.

#### Flags:

`--act_duration`: Duration of act test in seconds.
    (default: '86400')
    (an integer)

`--act_load`: Load multiplier for act test per device.
    (default: '1.0')
    (a comma separated list)

`--act_num_queues`: Total number of transaction queues. Default is number of
    cores, detected by ACT at runtime.
    (an integer)

`--[no]act_parallel`: Run act tools in parallel. One copy per device.
    (default: 'false')

`--act_reserved_partitions`: Number of partitions reserved (not being used by
    act).
    (default: '0')
    (an integer)

`--act_threads_per_queue`: Number of threads per transaction queue. Default is 4
    threads/queue.
    (an integer)

### [perfkitbenchmarker.linux_packages.aerospike_server ](../perfkitbenchmarker/linux_packages/aerospike_server.py)

#### Description:

Module containing aerospike server installation and cleanup functions.

#### Flags:

`--aerospike_replication_factor`: Replication factor for aerospike server.
    (default: '1')
    (an integer)

`--aerospike_storage_type`: <memory|disk>: The type of storage to use for
    Aerospike data. The type of disk is controlled by the "data_disk_type" flag.
    (default: 'memory')

`--aerospike_transaction_threads_per_queue`: Number of threads per transaction
    queue.
    (default: '4')
    (an integer)

### [perfkitbenchmarker.linux_packages.aws_credentials ](../perfkitbenchmarker/linux_packages/aws_credentials.py)

#### Description:

Module containing AWS credential file installation and cleanup helpers.

AWS credentials consist of a secret access key and its ID, stored in a single
file. Following PKB's AWS setup instructions (see
https://github.com/GoogleCloudPlatform/PerfKitBenchmarker#install-aws-cli-and-setup-authentication),
the default location of the file will be at ~/.aws/credentials

This package copies the credentials file to the remote VM to make them available
for calls from the VM to other AWS services, such as SQS or Kinesis.


#### Flags:

`--aws_credentials_local_path`: Path where the AWS credential files can be found
    on the local machine.
    (default: '~/.aws')

`--[no]aws_credentials_overwrite`: When set, if an AWS credential file already
    exists at the destination specified by --aws_credentials_remote_path, it
    will be overwritten during AWS credential file installation.
    (default: 'false')

`--aws_credentials_remote_path`: Path where the AWS credential files will be
    written on remote machines.
    (default: '.aws')

`--aws_s3_region`: Region for the S3 bucket

### [perfkitbenchmarker.linux_packages.azure_sdk ](../perfkitbenchmarker/linux_packages/azure_sdk.py)

#### Description:

Package for installing the Azure SDK.

#### Flags:

`--azure_lib_version`: Use a particular version of azure client lib, e.g.: 1.0.2
    (default: '1.0.3')

### [perfkitbenchmarker.linux_packages.cassandra ](../perfkitbenchmarker/linux_packages/cassandra.py)

#### Description:

Installs/Configures Cassandra.

See 'perfkitbenchmarker/data/cassandra/' for configuration files used.

Cassandra homepage: http://cassandra.apache.org


#### Flags:

`--cassandra_concurrent_reads`: Concurrent read requests each server accepts.
    (default: '32')
    (an integer)

`--cassandra_replication_factor`: Num of replicas.
    (default: '3')
    (an integer)

### [perfkitbenchmarker.linux_packages.ch_block_storage ](../perfkitbenchmarker/linux_packages/ch_block_storage.py)

#### Description:

Contains cloudharmony block storage benchmark installation functions.

#### Flags:

`--ch_params`: A list of comma seperated "key=value" parameters passed into
    cloud harmony benchmarks.
    (default: '')
    (a comma separated list)

### [perfkitbenchmarker.linux_packages.cloud_tpu_models ](../perfkitbenchmarker/linux_packages/cloud_tpu_models.py)

#### Description:

Module containing cloud TPU models installation and cleanup functions.

#### Flags:

`--cloud_tpu_commit_hash`: git commit hash of desired cloud TPU models commit.
    (default: '0aecc4c539db2b753bec722a6e3dfc6f685959eb')

### [perfkitbenchmarker.linux_packages.cuda_toolkit ](../perfkitbenchmarker/linux_packages/cuda_toolkit.py)

#### Description:

Module containing CUDA toolkit installation and cleanup functions.

This module installs cuda toolkit from NVIDIA, configures gpu clock speeds
and autoboost settings, and exposes a method to collect gpu metadata. Currently
Tesla K80 and P100 gpus are supported, provided that there is only a single
type of gpu per system.


#### Flags:

`--cuda_toolkit_installation_dir`: installation directory to use for CUDA
    toolkit. If the toolkit is not installed, it will be installed here. If it
    is already installed, the installation at this path will be used.
    (default: '/usr/local/cuda')

`--cuda_toolkit_version`: <8.0|9.0|10.0|10.1>: Version of CUDA Toolkit to
    install
    (default: '9.0')

`--[no]gpu_autoboost_enabled`: whether gpu autoboost is enabled

### [perfkitbenchmarker.linux_packages.cudnn ](../perfkitbenchmarker/linux_packages/cudnn.py)

#### Description:

Module containing CUDA Deep Neural Network library installation functions.

#### Flags:

`--cudnn`: The NVIDIA CUDA Deep Neural Network library. Please put in data
    directory and specify the name

### [perfkitbenchmarker.linux_packages.gluster ](../perfkitbenchmarker/linux_packages/gluster.py)

#### Description:

Module containing GlusterFS installation and cleanup functions.

#### Flags:

`--gluster_replicas`: The number of Gluster replicas.
    (default: '3')
    (an integer)

`--gluster_stripes`: The number of Gluster stripes.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.linux_packages.hadoop ](../perfkitbenchmarker/linux_packages/hadoop.py)

#### Description:

Module containing Hadoop installation and cleanup functions.

For documentation of commands to run at startup and shutdown, see:
http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html#Hadoop_Startup


#### Flags:

`--hadoop_version`: Version of hadoop.
    (default: '2.8.4')

### [perfkitbenchmarker.linux_packages.hbase ](../perfkitbenchmarker/linux_packages/hbase.py)

#### Description:

Module containing HBase installation and cleanup functions.

HBase is a scalable NoSQL database built on Hadoop.
https://hbase.apache.org/


#### Flags:

`--hbase_bin_url`: Specify to override url from HBASE_URL_BASE.

`--[no]hbase_use_stable`: Whether to use the current stable release of HBase.
    (default: 'false')

`--hbase_version`: HBase version.
    (default: '1.3.2.1')

### [perfkitbenchmarker.linux_packages.hpcc ](../perfkitbenchmarker/linux_packages/hpcc.py)

#### Description:

Module containing HPCC installation and cleanup functions.

The HPC Challenge is a collection of High Performance Computing benchmarks,
including High Performance Linpack (HPL). More information can be found here:
http://icl.cs.utk.edu/hpcc/


#### Flags:

`--hpcc_benchmarks`: A list of benchmarks in HPCC to run. If none are specified
    (the default), then all of the benchmarks are run. In 1.5.0, the benchmarks
    may include the following: HPL, Latency/Bandwidth, MPI RandomAccess, MPI
    RandomAccess LCG, MPIFFT, PTRANS, SingleDGEMM, SingleFFT,
    SingleRandomAccess, SingleRandomAccess LCG, SingleSTREAM, StarDGEMM,
    StarFFT, StarRandomAccess, StarRandomAccess LCG, StarSTREAM
    (default: '')
    (a comma separated list)

`--hpcc_math_library`: <openblas|mkl>: The math library to use when compiling
    hpcc: openblas or mkl. The default is openblas.
    (default: 'openblas')

### [perfkitbenchmarker.linux_packages.memcached_server ](../perfkitbenchmarker/linux_packages/memcached_server.py)

#### Description:

Module containing memcached server installation and cleanup functions.

#### Flags:

`--memcached_num_threads`: Number of worker threads.
    (default: '4')
    (an integer)

`--memcached_size_mb`: Size of memcached cache in megabytes.
    (default: '64')
    (an integer)

### [perfkitbenchmarker.linux_packages.memtier ](../perfkitbenchmarker/linux_packages/memtier.py)

#### Description:

Module containing memtier installation and cleanup functions.

#### Flags:

`--memtier_clients`: Comma separated list of number of clients per thread.
    Specify more than 1 value to vary the number of clients. Defaults to [50].
    (default: '50')
    (a comma separated list)

`--memtier_data_size`: Object data size. Defaults to 32 bytes.
    (default: '32')
    (an integer)

`--memtier_key_pattern`: Set:Get key pattern. G for Gaussian distribution, R for
    uniform Random, S for Sequential. Defaults to R:R.
    (default: 'R:R')

`--memtier_pipeline`: Number of pipelines to use for memtier. Defaults to 1,
    i.e. no pipelining.
    (default: '1')
    (an integer)

`--memtier_protocol`: <memcache_binary|redis|memcache_text>: Protocol to use.
    Supported protocols are redis, memcache_text, and memcache_binary. Defaults
    to memcache_binary.
    (default: 'memcache_binary')

`--memtier_ratio`: Set:Get ratio. Defaults to 9x Get versus Sets (9 Gets to 1
    Set in 10 total requests).
    (default: '9')
    (an integer)

`--memtier_requests`: Number of total requests per client. Defaults to 10000.
    (default: '10000')
    (an integer)

`--memtier_run_count`: Number of full-test iterations to perform. Defaults to 1.
    (default: '1')
    (an integer)

`--memtier_threads`: Number of threads. Defaults to 4.
    (default: '4')
    (an integer)

### [perfkitbenchmarker.linux_packages.mxnet ](../perfkitbenchmarker/linux_packages/mxnet.py)

#### Description:

Module containing MXNet installation and cleanup functions.

#### Flags:

`--mx_version`: mxnet pip package version
    (default: '1.4.0')

### [perfkitbenchmarker.linux_packages.mxnet_cnn ](../perfkitbenchmarker/linux_packages/mxnet_cnn.py)

#### Description:

Module containing MXNet CNN installation and cleanup functions.

#### Flags:

`--mxnet_commit_hash`: git commit hash of desired mxnet commit.
    (default: '2700ddbbeef212879802f7f0c0812192ec5c2b77')

### [perfkitbenchmarker.linux_packages.netperf ](../perfkitbenchmarker/linux_packages/netperf.py)

#### Description:

Module containing netperf installation and cleanup functions.

#### Flags:

`--netperf_histogram_buckets`: The number of buckets per bucket array in a
    netperf histogram. Netperf keeps one array for latencies in the single usec
    range, one for the 10-usec range, one for the 100-usec range, and so on
    until the 10-sec range. The default value that netperf uses is 100. Using
    more will increase the precision of the histogram samples that the netperf
    benchmark produces.
    (default: '100')
    (an integer)

### [perfkitbenchmarker.linux_packages.openjdk ](../perfkitbenchmarker/linux_packages/openjdk.py)

#### Description:

Module containing OpenJDK installation and cleanup functions.

#### Flags:

`--openjdk_version`: Version of openjdk to use. By default, the version of
    openjdk is automatically detected.

### [perfkitbenchmarker.linux_packages.openmpi ](../perfkitbenchmarker/linux_packages/openmpi.py)

#### Description:

Module containing OpenMPI installation and cleanup functions.

#### Flags:

`--[no]openmpi_enable_shared`: Whether openmpi should build shared libraries in
    addition to static ones.
    (default: 'false')

### [perfkitbenchmarker.linux_packages.redis_enterprise ](../perfkitbenchmarker/linux_packages/redis_enterprise.py)

#### Description:

Module containing redis enterprise installation and cleanup functions.

#### Flags:

`--enterprise_redis_disable_cpu_ids`: List of cpus to disable by id.
    (a comma separated list)

`--enterprise_redis_latency_threshold`: The latency threshold in microseconds
    until the test stops.
    (default: '1100')
    (an integer)

`--enterprise_redis_load_records`: Number of keys to pre-load into Redis.
    (default: '1000000')
    (an integer)

`--enterprise_redis_loadgen_clients`: Number of clients per loadgen vm.
    (default: '24')
    (an integer)

`--enterprise_redis_max_threads`: Maximum number of memtier threads to use.
    (default: '40')
    (an integer)

`--enterprise_redis_min_threads`: Minimum number of memtier threads to use.
    (default: '18')
    (an integer)

`--[no]enterprise_redis_pin_workers`: Whether to pin the proxy threads after
    startup.
    (default: 'false')

`--enterprise_redis_pipeline`: Number of pipelines to use.
    (default: '9')
    (an integer)

`--enterprise_redis_proxy_threads`: Number of redis proxy threads to use.
    (default: '24')
    (an integer)

`--enterprise_redis_run_records`: Number of requests per loadgen client to send
    to the Redis server.
    (default: '1000000')
    (an integer)

`--enterprise_redis_shard_count`: Number of redis shard. Each shard is a redis
    thread.
    (default: '6')
    (an integer)

`--enterprise_redis_thread_increment`: Number of memtier threads to increment
    by.
    (default: '1')
    (an integer)

`--[no]enterprise_redis_tune_on_startup`: Whether to tune core config during
    startup.
    (default: 'true')

### [perfkitbenchmarker.linux_packages.redis_server ](../perfkitbenchmarker/linux_packages/redis_server.py)

#### Description:

Module containing redis installation and cleanup functions.

#### Flags:

`--[no]redis_enable_aof`: Enable append-only file (AOF) with appendfsync always.
    (default: 'false')

`--redis_server_version`: Version of redis server to use.
    (default: '5.0.5')

`--redis_total_num_processes`: Total number of redis server processes.
    (default: '1')
    (a positive integer)

### [perfkitbenchmarker.linux_packages.speccpu ](../perfkitbenchmarker/linux_packages/speccpu.py)

#### Description:

Module to install, uninstall, and parse results for SPEC CPU 2006 and 2017.


#### Flags:

`--runspec_build_tool_version`: Version of gcc/g++/gfortran. This should match
    runspec_config. Note, if neither runspec_config and
    runspec_build_tool_version is set, the test install gcc/g++/gfortran-4.7,
    since that matches default config version. If runspec_config is set, but not
    runspec_build_tool_version, default version of build tools will be
    installed. Also this flag only works with debian.

`--runspec_config`: Used by the PKB speccpu benchmarks. Name of the cfg file to
    use as the SPEC CPU config file provided to the runspec binary via its
    --config flag. If the benchmark is run using an .iso file, then the cfg file
    must be placed in the local PKB data directory and will be copied to the
    remote machine prior to executing runspec/runcpu. Defaults to None. See
    README.md for instructions if running with a repackaged .tgz file.

`--runspec_define`: Used by the PKB speccpu benchmarks. Optional comma-separated
    list of SYMBOL[=VALUE] preprocessor macros provided to the runspec binary
    via repeated --define flags. Example: numa,smt,sse=SSE4.2
    (default: '')

`--[no]runspec_enable_32bit`: Used by the PKB speccpu benchmarks. If set,
    multilib packages will be installed on the remote machine to enable use of
    32-bit SPEC CPU binaries. This may be useful when running on memory-
    constrained instance types (i.e. less than 2 GiB memory/core), where 64-bit
    execution may be problematic.
    (default: 'false')

`--[no]runspec_estimate_spec`: Used by the PKB speccpu benchmarks. If set, the
    benchmark will report an estimated aggregate score even if SPEC CPU did not
    compute one. This usually occurs when --runspec_iterations is less than 3.
    --runspec_keep_partial_results is also required to be set. Samples will
    becreated as estimated_SPECint(R)_rate_base and
    estimated_SPECfp(R)_rate_base.  Available results will be saved, and PKB
    samples will be marked with a metadata value of partial=true. If unset,
    SPECint(R)_rate_base20** and SPECfp(R)_rate_base20** are listed in the
    metadata under missing_results.
    (default: 'false')

`--runspec_iterations`: Used by the PKB speccpu benchmarks. The number of
    benchmark iterations to execute, provided to the runspec binary via its
    --iterations flag.
    (default: '3')
    (an integer)

`--[no]runspec_keep_partial_results`: Used by the PKB speccpu benchmarks. If
    set, the benchmark will report an aggregate score even if some of the SPEC
    CPU component tests failed with status "NR". Available results will be
    saved, and PKB samples will be marked with a metadata value of partial=true.
    If unset, partial failures are treated as errors.
    (default: 'false')

`--spec_runmode`: <base|peak|all>: Run mode to use. Defaults to base.
    (default: 'base')

### [perfkitbenchmarker.linux_packages.tensorflow ](../perfkitbenchmarker/linux_packages/tensorflow.py)

#### Description:

Module containing TensorFlow installation and cleanup functions.

#### Flags:

`--t2t_pip_package`: Tensor2Tensor pip package to install. By default, PKB will
    install tensor2tensor==1.7 .
    (default: 'tensor2tensor==1.7')

`--tf_cnn_benchmarks_branch`: TensorFlow CNN branchmarks branch that is
    compatible with A TensorFlow version.
    (default: 'cnn_tf_v1.12_compatible')

`--tf_cpu_pip_package`: TensorFlow CPU pip package to install. By default, PKB
    will install an Intel-optimized CPU build when using CPUs.
    (default: 'https://anaconda.org/intel/tensorflow/1.12.0/download/tensorflow-
    1.12.0-cp27-cp27mu-linux_x86_64.whl')

`--tf_gpu_pip_package`: TensorFlow GPU pip package to install. By default, PKB
    will install tensorflow-gpu==1.12 when using GPUs.
    (default: 'tensorflow-gpu==1.12.0')

### [perfkitbenchmarker.linux_packages.tensorflow_models ](../perfkitbenchmarker/linux_packages/tensorflow_models.py)

#### Description:

Module containing TensorFlow models installation and cleanup functions.

#### Flags:

`--tensorflow_models_commit_hash`: git commit hash of desired TensorFlow models
    commit.
    (default: '57e075203f8fba8d85e6b74f17f63d0a07da233a')

### [perfkitbenchmarker.linux_packages.tensorflow_serving ](../perfkitbenchmarker/linux_packages/tensorflow_serving.py)

#### Description:

Module containing TensorFlow Serving installation functions.



#### Flags:

`--tf_serving_branch`: GitHub branch to pull from
    (default: 'master')

### [perfkitbenchmarker.linux_packages.tomcat ](../perfkitbenchmarker/linux_packages/tomcat.py)

#### Description:

Module containing Apache Tomcat installation and cleanup functions.

Installing Tomcat via this module makes some changes to the default settings:

  * Http11Nio2Protocol is used (non-blocking).
  * Request logging is disabled.
  * The session timeout is decreased to 1 minute.

https://tomcat.apache.org/


#### Flags:

`--tomcat_url`: Tomcat 8 download URL.
    (default: 'https://archive.apache.org/dist/tomcat/tomcat-8/v8.0.28/bin
    /apache-tomcat-8.0.28.tar.gz')

### [perfkitbenchmarker.linux_packages.ycsb ](../perfkitbenchmarker/linux_packages/ycsb.py)

#### Description:

Install, execute, and parse results from YCSB.

YCSB (the Yahoo! Cloud Serving Benchmark) is a common method of comparing NoSQL
database performance.
https://github.com/brianfrankcooper/YCSB

For PerfKitBenchmarker, we wrap YCSB to:

  * Pre-load a database with a fixed number of records.
  * Execute a collection of workloads under a staircase load.
  * Parse the results into PerfKitBenchmarker samples.

The 'YCSBExecutor' class handles executing YCSB on a collection of client VMs.
Generally, clients just need this class. For example, to run against
HBase 1.0:

  >>> executor = ycsb.YCSBExecutor('hbase-10')
  >>> samples = executor.LoadAndRun(loader_vms)

By default, this runs YCSB workloads A and B against the database, 32 threads
per client VM, with an initial database size of 1GB (1k records).
Each workload runs for at most 30 minutes.


#### Flags:

`--ycsb_client_vms`: Number of YCSB client VMs.
    (default: '1')
    (an integer)

`--ycsb_field_count`: Number of fields in a record. Defaults to None which uses
    the ycsb default of 10.
    (an integer)

`--ycsb_field_length`: Size of each field. Defaults to None which uses the ycsb
    default of 100.
    (an integer)

`--[no]ycsb_histogram`: Include individual histogram results from YCSB (will
    increase sample count).
    (default: 'false')

`--[no]ycsb_include_individual_results`: Include results from each client VM,
    rather than just combined results.
    (default: 'false')

`--ycsb_load_parameters`: Passed to YCSB during the load stage. Comma-separated
    list of "key=value" pairs.
    (default: '')
    (a comma separated list)

`--[no]ycsb_load_samples`: Include samples from pre-populating database.
    (default: 'true')

`--ycsb_measurement_interval`: <op|intended|both>: Measurement interval to use
    for ycsb. Defaults to op.
    (default: 'op')

`--ycsb_measurement_type`: <histogram|hdrhistogram|timeseries>: Measurement type
    to use for ycsb. Defaults to histogram.
    (default: 'histogram')

`--ycsb_operation_count`: Number of operations *per client VM*.
    (an integer)

`--ycsb_preload_threads`: Number of threads per loader during the initial data
    population stage. Default value depends on the target DB.
    (an integer)

`--ycsb_readproportion`: The read proportion, Default is 0.5 in workloada and
    0.95 in YCSB.
    (a number)

`--ycsb_record_count`: Pre-load with a total dataset of records total. Overrides
    recordcount value in all workloads of this run. Defaults to None, where
    recordcount value in each workload is used. If neither is not set, ycsb
    default of 0 is used.
    (an integer)

`--[no]ycsb_reload_database`: Reload database, othewise skip load stage. Note,
    this flag is only used if the database is already loaded.
    (default: 'true')

`--ycsb_requestdistribution`: <uniform|zipfian|latest>: Type of request
    distribution.  This will overwrite workload file parameter

`--ycsb_run_parameters`: Passed to YCSB during the load stage. Comma-separated
    list of "key=value" pairs.
    (default: '')
    (a comma separated list)

`--ycsb_scanproportion`: The scan proportion, Default is 0 in workloada and 0 in
    YCSB.
    (a number)

`--ycsb_threads_per_client`: Number of threads per loader during the benchmark
    run. Specify a list to vary the number of clients.
    (default: '32')
    (a comma separated list)

`--ycsb_timelimit`: Maximum amount of time to run each workload / client count
    combination. Set to 0 for unlimited time.
    (default: '1800')
    (an integer)

`--ycsb_updateproportion`: The update proportion, Default is 0.5 in workloada
    and 0.05 in YCSB.
    (a number)

`--ycsb_version`: YCSB version to use. Defaults to version 0.9.0.
    (default: '0.9.0')

`--ycsb_workload_files`: Path to YCSB workload file to use during *run* stage
    only. Comma-separated list
    (default: 'workloada,workloadb')
    (a comma separated list)

### [perfkitbenchmarker.linux_virtual_machine ](../perfkitbenchmarker/linux_virtual_machine.py)

#### Description:

Module containing mixin classes for linux virtual machines.

These classes allow installation on both Debian and RHEL based linuxes.
They also handle some initial setup (especially on RHEL based linuxes
since by default sudo commands without a tty don't work) and
can restore the VM to the state it was in before packages were
installed.

To install a package on a VM, just call vm.Install(package_name).
The package name is just the name of the package module (i.e. the
file name minus .py). The framework will take care of all cleanup
for you.


#### Flags:

`--append_kernel_command_line`: String to append to the kernel command line. The
    presence of any non-empty string will cause a reboot to occur after VM
    prepare. If unspecified, the kernel command line will be unmodified.

`--disk_fill_size`: Size of file to create in GBs.
    (default: '0')
    (an integer)

`--[no]enable_transparent_hugepages`: Whether to enable or disable transparent
    hugepages. If unspecified, the setting is unchanged from the default in the
    OS.

`--[no]network_enable_BBR`: A shortcut to enable BBR congestion control on the
    network. equivalent to appending to --sysctls the following values
    "net.core.default_qdisc=fq, "net.ipv4.tcp_congestion_control=bbr" As with
    other sysctrls, will cause a reboot to happen.
    (default: 'false')

`--num_disable_cpus`: Number of CPUs to disable on the virtual machine.If the VM
    has n CPUs, you can disable at most n-1.
    (a positive integer)

`--set_files`: Arbitrary filesystem configuration. This flag should be a comma-
    separated list of path=value pairs. Each value will be written to the
    corresponding path. For example, if you pass
    --set_files=/sys/kernel/mm/transparent_hugepage/enabled=always, then PKB
    will write "always" to /sys/kernel/mm/transparent_hugepage/enabled before
    starting the benchmark.
    (default: '')
    (a comma separated list)

`--[no]setup_remote_firewall`: Whether PKB should configure the firewall of each
    remoteVM to make sure it accepts all internal connections.
    (default: 'false')

`--ssh_retries`: Default number of times to retry SSH.
    (default: '10')
    (a non-negative integer)

`--[no]ssh_via_internal_ip`: Whether to use internal IP addresses for running
    commands on and pushing data to VMs. By default, PKB interacts with VMs
    using external IP addresses.
    (default: 'false')

`--sysctl`: Sysctl values to set. This flag should be a comma-separated list of
    path=value pairs. Each pair will be appended to/etc/sysctl.conf.  The
    presence of any items in this list will cause a reboot to occur after VM
    prepare. For example, if you pass
    --sysctls=vm.dirty_background_ratio=10,vm.dirty_ratio=25, PKB will append
    "vm.dirty_background_ratio=10" and"vm.dirty_ratio=25" on separate lines to
    /etc/sysctrl.conf and then the machine will be rebooted before startingthe
    benchmark.
    (default: '')
    (a comma separated list)

### [perfkitbenchmarker.managed_memory_store ](../perfkitbenchmarker/managed_memory_store.py)

#### Description:

Module containing class for cloud managed memory stores.

#### Flags:

`--managed_memory_store_version`: The version of managed memory store to use.
    This flag overrides Redis or Memcached version defaults that is set in
    benchmark config. Defaults to None so that benchmark config defaults are
    used.

`--redis_failover_style`:
    <failover_none|failover_same_zone|failover_same_region>: Failover behavior
    of cloud redis cluster. Acceptable values are:failover_none,
    failover_same_zone, and failover_same_region
    (default: 'failover_none')

### [perfkitbenchmarker.managed_relational_db ](../perfkitbenchmarker/managed_relational_db.py)

#### Description:

Generate a random password 10 characters in length.

#### Flags:

`--[no]managed_db_backup_enabled`: Whether or not to enable automated backups
    (default: 'true')

`--managed_db_backup_start_time`: Time in UTC that automated backups (if
    enabled) will be scheduled. In the form HH:MM UTC. Defaults to 07:00 UTC
    (default: '07:00')

`--managed_db_cpus`: Number of Cpus in the database.
    (an integer)

`--managed_db_database_name`: Name of the database to create. Defaults to pkb-db
    -[run-uri]

`--managed_db_database_password`: Database password. Defaults to a random
    10-character alpha-numeric string

`--managed_db_database_username`: Database username. Defaults to pkb-db-user
    -[run-uri]

`--managed_db_disk_size`: Size of the database disk in GB.
    (an integer)

`--managed_db_disk_type`: Machine type of the database.

`--managed_db_engine`: Managed database flavor to use (mysql, postgres)

`--managed_db_engine_version`: Version of the database flavor selected, e.g. 5.7

`--[no]managed_db_high_availability`: Specifies if the database should be high
    availability
    (default: 'false')

`--managed_db_machine_type`: Machine type of the database.

`--managed_db_memory`: Amount of Memory in the database.  Uses the same format
    string as custom machine memory type.

`--managed_db_zone`: zone or region to launch the database in. Defaults to the
    client vm's zone.
    (a comma separated list)

### [perfkitbenchmarker.nfs_service ](../perfkitbenchmarker/nfs_service.py)

#### Description:

Resource encapsulating provisioned cloud NFS services.

Defines a resource for use in other benchmarks such as SpecSFS2014 and FIO.

Example --benchmark_config_file:

nfs_10_tb: &nfs_10_tb
  AWS:
    disk_type: nfs
    mount_point: /scratch

specsfs:
  name: specsfs2014
  flags:
    specsfs2014_num_runs: 1
    specsfs2014_load: 1
  vm_groups:
    clients:
      disk_spec: *nfs_10_tb
      vm_count: 1
      os_type: rhel
    gluster_servers:
      vm_count: 0


#### Flags:

`--nfs_tier`: NFS Mode

`--nfs_version`: NFS Version

### [perfkitbenchmarker.object_storage_service ](../perfkitbenchmarker/object_storage_service.py)

#### Description:

An interface to object storage services.

#### Flags:

`--boto_file_location`: The location of the boto file.

`--object_storage_credential_file`: Directory of credential file.

### [perfkitbenchmarker.os_types ](../perfkitbenchmarker/os_types.py)

#### Description:

Supported types of operating systems that a VM may host.

#### Flags:

`--os_type`: <amazonlinux2|centos7|debian|debian9|juju|rhel|ubuntu_container|ubu
    ntu1404|ubuntu1604|ubuntu1604_cuda9|ubuntu1710|ubuntu1804|windows|windows201
    2|windows2016|windows2019>: The VM's OS type. Ubuntu's os_type can also be
    specified as "debian" because it is largely built on Debian and uses the
    same package manager. Likewise, CentOS's os_type can be "rhel". In general
    if two OS's use the same package manager, and are otherwise very similar,
    the same os_type may work on both of them.  However, more specific os_types
    (and associated VirtualMachine subclasses can be developed.
    (default: 'ubuntu1604')

### [perfkitbenchmarker.pkb ](../perfkitbenchmarker/pkb.py)

#### Description:

Runs all benchmarks in PerfKitBenchmarker.

All benchmarks in PerfKitBenchmarker export the following interface:

GetConfig: this returns, the name of the benchmark, the number of machines
         required to run one instance of the benchmark, a detailed description
         of the benchmark, and if the benchmark requires a scratch disk.
Prepare: this function takes a list of VMs as an input parameter. The benchmark
         will then get all binaries required to run the benchmark and, if
         required, create data files.
Run: this function takes a list of VMs as an input parameter. The benchmark will
     then run the benchmark upon the machines specified. The function will
     return a dictonary containing the results of the benchmark.
Cleanup: this function takes a list of VMs as an input parameter. The benchmark
         will then return the machine to the state it was at before Prepare
         was called.

PerfKitBenchmarker has the following run stages: provision, prepare,
    run, cleanup, teardown, and all.

provision: Read command-line flags, decide what benchmarks to run, and
    create the necessary resources for each benchmark, including
    networks, VMs, disks, and keys, and generate a run_uri, which can
    be used to resume execution at later stages.
prepare: Execute the Prepare function of each benchmark to install
         necessary software, upload datafiles, etc.
run: Execute the Run function of each benchmark and collect the
     generated samples. The publisher may publish these samples
     according to PKB's settings. The Run stage can be called multiple
     times with the run_uri generated by the provision stage.
cleanup: Execute the Cleanup function of each benchmark to uninstall
         software and delete data files.
teardown: Delete VMs, key files, networks, and disks created in the
    'provision' stage.

all: PerfKitBenchmarker will run all of the above stages (provision,
     prepare, run, cleanup, teardown). Any resources generated in the
     provision stage will be automatically deleted in the teardown
     stage, even if there is an error in an earlier stage. When PKB is
     running in this mode, the run cannot be repeated or resumed using
     the run_uri.


#### Flags:

`--after_prepare_sleep_time`: The time in seconds to sleep after the prepare
    phase. This can be useful for letting burst tokens accumulate.
    (default: '0')
    (an integer)

`--after_run_sleep_time`: The time in seconds to sleep after the run phase. This
    can be useful for letting the VM sit idle after the bechmarking phase is
    complete.
    (default: '0')
    (an integer)

`--archive_bucket`: Archive results to the given S3/GCS bucket.

`--[no]before_cleanup_pause`: If true, wait for command line input before
    executing the cleanup phase. This is useful for debugging benchmarks during
    development.
    (default: 'false')

`--benchmarks`: Benchmarks and/or benchmark sets that should be run. The default
    is the standard set. For more information about benchmarks and benchmark
    sets, see the README and benchmark_sets.py.
    (default: 'standard_set')
    (a comma separated list)

`--[no]boot_samples`: Whether to publish boot time samples for all tests.
    (default: 'false')

`--completion_status_file`: If specified, this file will contain the completion
    status of each benchmark that ran (SUCCEEDED, FAILED, or SKIPPED). The file
    has one json object per line, each with the following format:
    { "name": <benchmark name>, "flags": <flags dictionary>, "status":
    <completion status> }

`--[no]create_failed_run_samples`: If true, PKB will create a sample specifying
    that a run stage failed. This sample will include metadata specifying the
    run stage that failed, the exception that occurred, as well as all the flags
    that were provided to PKB on the command line.
    (default: 'false')

`--[no]create_started_run_sample`: Whether PKB will create a sample at the start
    of the provision phase of the benchmark run.
    (default: 'false')

`--data_disk_size`: Size, in gb, for all data disks.
    (an integer)

`--data_disk_type`: Type for all data disks. If a provider keeps the operating
    system and user data on separate disks, this only affects the user data
    disk(s).If the provider has OS and user data on the same disk, this flag
    affectsthat disk.

`--[no]disable_interrupt_moderation`: Turn off the interrupt moderation
    networking feature
    (default: 'false')

`--[no]disable_rss`: Whether or not to disable the Receive Side Scaling feature.
    (default: 'false')

`--[no]dry_run`: If true, PKB will print the flags configurations to be run and
    exit. The configurations are generated from the command line flags, the
    flag_matrix, and flag_zip.
    (default: 'false')

`--duration_in_seconds`: duration of benchmarks. (only valid for mesh_benchmark)
    (an integer)

`--extra_zones`: Zones that will be appended to the "zones" list. This is
    functionally the same, but allows flag matrices to have two zone axes.
    (default: '')
    (a comma separated list)

`--failed_run_samples_error_length`: If create_failed_run_samples is true, PKB
    will truncate any error messages at failed_run_samples_error_length.
    (default: '10240')
    (an integer)

`--file_log_level`: <debug|info|warning|error>: Anything logged at this level or
    higher will be written to the log file.
    (default: 'debug')

`--ftp_proxy`: Specify a proxy for FTP in the form
    [user:passwd@]proxy.server:port.
    (default: '')

`--gpu_count`: Number of gpus to attach to the VM. Requires gpu_type to be
    specified.
    (an integer)

`--gpu_type`: <k80|p100|v100|p4|p4-vws>: Type of gpus to attach to the VM.
    Requires gpu_count to be specified.

`--helpmatch`: Shows only flags defined in a module whose name matches the given
    regex.
    (default: '')

`--helpmatchmd`: Markdown friendly help outputShows only flags defined in a
    module whose name matches the given regex.
    (default: '')

`--http_proxy`: Specify a proxy for HTTP in the form
    [user:passwd@]proxy.server:port.
    (default: '')

`--https_proxy`: Specify a proxy for HTTPS in the form
    [user:passwd@]proxy.server:port.
    (default: '')

`--[no]ignore_package_requirements`: Disables Python package requirement runtime
    checks.
    (default: 'false')

`--image`: Default image that will be linked to the VM

`--[no]install_packages`: Override for determining whether packages should be
    installed. If this is false, no packages will be installed on any VMs. This
    option should probably only ever be used if you have already created an
    image with all relevant packages installed.

`--[no]log_dmesg`: Whether to log dmesg from each VM to the PKB log file before
    the VM is deleted.
    (default: 'false')

`--log_level`: <debug|info|warning|error>: The log level to run at.
    (default: 'info')

`--machine_type`: Machine types that will be created for benchmarks that don't
    require a particular type.

`--num_striped_disks`: The number of data disks to stripe together to form one
    "logical" data disk. This defaults to 1 (except with local disks), which
    means no striping. When using local disks, they default to striping all
    disks together. The striped disks will appear as one disk (data_disk_0) in
    the metadata.
    (a positive integer)

`--num_vms`: For benchmarks which can make use of a variable number of machines,
    the number of VMs to use.
    (default: '1')
    (an integer)

`--owner`: Owner name. Used to tag created resources and performance records.
    (default: 'toor')

`--persistent_timeout_minutes`: An upper bound on the time in minutes that
    resources left behind by the benchmark. Some benchmarks purposefully create
    resources for other benchmarks to use. Persistent timeout specifies how long
    these shared resources should live.
    (default: '240')
    (an integer)

`--project`: GCP project ID under which to create the virtual machines

`--[no]publish_after_run`: If true, PKB will publish all samples available
    immediately after running each benchmark. This may be useful in scenarios
    where the PKB run time for all benchmarks is much greater than a single
    benchmark.
    (default: 'false')

`--publish_period`: The period in seconds to publish samples from repeated run
    stages. This will only publish samples if publish_after_run is True.
    (an integer)

`--[no]randomize_run_order`: When running with more than one benchmarks,
    randomize order of the benchmarks.
    (default: 'false')

`--run_processes`: The number of parallel processes to use to run benchmarks.
    (a positive integer)

`--run_processes_delay`: The delay in seconds between parallel processes'
    invocation. Increasing this value may reduce provider throttling issues.
    (a non-negative number)

`--run_stage_iterations`: PKB will run/re-run the run stage of each benchmark
    this many times. It defaults to 1, so benchmarks will only be run once
    unless some other value is specified. This flag and run_stage_time are
    mutually exclusive.
    (default: '1')
    (an integer)

`--run_stage_retries`: The number of allowable consecutive failures during the
    run stage. After this number of failures any exceptions will cause benchmark
    termination. If run_stage_time is exceeded, the run stage will not be
    retried even if the number of failures is less than the value of this flag.
    (default: '0')
    (an integer)

`--run_stage_time`: PKB will run/re-run the run stage of each benchmark until it
    has spent at least this many seconds. It defaults to 0, so benchmarks will
    only be run once unless some other value is specified. This flag and
    run_stage_iterations are mutually exclusive.
    (default: '0')
    (an integer)

`--run_uri`: Name of the Run. If provided, this should be alphanumeric and less
    than or equal to 12 characters in length.

`--scratch_disk_iops`: IOPS for Provisioned IOPS (SSD) volumes in AWS.
    (an integer)

`--scratch_disk_size`: Size, in gb, for all scratch disks.
    (an integer)

`--scratch_disk_type`: <standard|remote_ssd|piops|local>: Type for all scratch
    disks. The default is standard

`--skip_pending_runs_file`: If file exists, any pending runs will be not be
    executed.

`--spark_service_type`: <pkb_managed|managed>: Type of spark service to use

`--ssh_options`: Additional options to pass to ssh.
    (default: '')
    (a comma separated list)

`--static_vm_file`: The file path for the Static Machine file. See
    static_virtual_machine.py for a description of this file.

`--[no]stop_after_benchmark_failure`: Determines response when running multiple
    benchmarks serially and a benchmark run fails. When True, no further
    benchmarks are scheduled, and execution ends. When False, benchmarks
    continue to be scheduled. Does not apply to keyboard interrupts, which will
    always prevent further benchmarks from being scheduled.
    (default: 'false')

`--[no]time_commands`: Times each command issued.
    (default: 'false')

`--timeout_minutes`: An upper bound on the time in minutes that the benchmark is
    expected to run. This time is annotated or tagged on the resources of cloud
    providers.
    (default: '240')
    (an integer)

`--[no]use_ipv6`: Whether to use ipv6 for ssh/scp.
    (default: 'false')

`--[no]use_pkb_logging`: Whether to use PKB-specific logging handlers. Disabling
    this will use the standard ABSL logging directly.
    (default: 'true')

`--[no]version`: Display the version and exit.
    (default: 'false')

`--zone`: Similar to the --zones flag, but allows the flag to be specified
    multiple times on the commandline. For example, --zone=a --zone=b is
    equivalent to --zones=a,b. Furthermore, any values specified by --zone will
    be appended to those specfied by --zones.;
    repeat this option to specify a list of values
    (default: '[]')

`--zones`: A list of zones within which to run PerfKitBenchmarker. This is
    specific to the cloud provider you are running on. If multiple zones are
    given, PerfKitBenchmarker will create 1 VM in zone, until enough VMs are
    created as specified in each benchmark. The order in which this flag is
    applied to VMs is undefined.
    (default: '')
    (a comma separated list)

### [perfkitbenchmarker.providers.alicloud.flags ](../perfkitbenchmarker/providers/alicloud/flags.py)

#### Description:

No description available

#### Flags:

`--ali_bandwidth_in`: Inbound Bandwidth
    (default: '100')
    (an integer)

`--ali_bandwidth_out`: Outbound Bandwidth
    (default: '100')
    (an integer)

`--ali_eip_address_bandwidth`: The rate limit of the EIP in Mbps.
    (default: '5')
    (an integer)

`--ali_io_optimized`: IO optimized for disk in AliCloud. The default is None
    which means no IO optimized "optimized" means use IO optimized. If you
    choose optimized, you must specify the system disk type

`--ali_system_disk_type`: System disk catogory for AliCloud. The default is
    "cloud" for General cloud disk, "cloud_ssd" for cloud ssd disk,
    "cloud_efficiency" for efficiency cloud disk, "ephemeral_ssd" for local ssd
    disk
    (default: 'cloud')

`--[no]ali_use_vpc`: Use VPC to create networks
    (default: 'true')

`--ali_user_name`: This determines the user name that Perfkit will attempt to
    use. This must be changed in order to use any image other than ubuntu.
    (default: 'ubuntu')

### [perfkitbenchmarker.providers.aws.aws_dpb_emr ](../perfkitbenchmarker/providers/aws/aws_dpb_emr.py)

#### Description:

Module containing class for AWS's EMR service.

Clusters can be created and deleted.


#### Flags:

`--dpb_emr_release_label`: The emr version to use for the cluster.
    (default: 'emr-5.23.0')

### [perfkitbenchmarker.providers.aws.aws_dynamodb ](../perfkitbenchmarker/providers/aws/aws_dynamodb.py)

#### Description:

Module containing class for AWS' dynamodb tables.

Tables can be created and deleted.


#### Flags:

`--aws_dynamodb_attributetype`: <S|N|B>: The type of attribute, default to S
    (String).Alternates are N (Number) and B (Binary).
    (default: 'S')

`--aws_dynamodb_gsi_count`: Set amount of Global Secondary Indexes. Only set 0-5
    (default: '0')
    (an integer)

`--aws_dynamodb_lsi_count`: Set amount of Local Secondary Indexes. Only set 0-5
    (default: '0')
    (an integer)

`--aws_dynamodb_primarykey`: The primaryKey of dynamodb table.This switches to
    sortkey if using sort.If testing GSI/LSI, use the range keynameof the index
    you want to test
    (default: 'primary_key')

`--aws_dynamodb_read_capacity`: Set RCU for dynamodb table
    (default: '5')
    (an integer)

`--aws_dynamodb_sortkey`: The sortkey of dynamodb table.  This switches to
    primarykey if using sort.If testing GSI/LSI, use the primary keynameof the
    index you want to test
    (default: 'sort_key')

`--[no]aws_dynamodb_use_sort`: determine whether to use sort key or not
    (default: 'false')

`--aws_dynamodb_write_capacity`: Set WCU for dynamodb table
    (default: '5')
    (an integer)

`--[no]aws_dynamodb_ycsb_consistentReads`: Consistent reads cost 2x eventual
    reads. 'false' is default which is eventual
    (default: 'false')

### [perfkitbenchmarker.providers.aws.flags ](../perfkitbenchmarker/providers/aws/flags.py)

#### Description:

Module containing flags applicable across benchmark run on AWS.

#### Flags:

`--aws_boot_disk_size`: The boot disk size in GiB for AWS VMs.
    (an integer)

`--[no]aws_delete_file_system`: Whether to delete the EFS file system.
    (default: 'true')

`--aws_efs_token`: The creation token used to create the EFS resource. If the
    file system already exists, it will use that instead of creating a new one.

`--aws_elasticache_failover_zone`: AWS elasticache failover zone

`--aws_emr_job_wait_time`: The time to wait for an EMR job to finish, in seconds
    (default: '18000')
    (an integer)

`--aws_emr_loguri`: The log-uri parameter to pass to AWS when creating a
    cluster.  If not set, a bucket will be created.

`--aws_image_name_filter`: The filter to use when searching for an image for a
    VM. See usage details in aws_virtual_machine.py around IMAGE_NAME_FILTER.

`--aws_image_name_regex`: The Python regex to use to further filter images for a
    VM. This applies after the aws_image_name_filter. See usage details in
    aws_virtual_machine.py around IMAGE_NAME_REGEX.

`--aws_preprovisioned_data_bucket`: AWS bucket where pre-provisioned data has
    been copied.

`--aws_provisioned_iops`: IOPS for Provisioned IOPS (SSD) volumes in AWS.
    (an integer)

`--[no]aws_spot_instances`: Whether to use AWS spot instances for any AWS VMs.
    (default: 'false')

`--aws_spot_price`: The spot price to bid for AWS spot instances. Defaults to
    on-demand price when left as None.
    (a number)

`--aws_user_name`: This determines the user name that Perfkit will attempt to
    use. This must be changed in order to use any image other than ubuntu.
    (default: 'ubuntu')

`--cache_node_type`: The AWS cache node type to use for elasticache clusters.
    (default: 'cache.m4.large')

`--efs_provisioned_throughput`: The throughput limit of EFS (in MiB/s) when run
    in provisioned mode.
    (default: '1024.0')
    (a number)

`--efs_throughput_mode`: <provisioned|bursting>: The throughput mode to use for
    EFS.
    (default: 'provisioned')

`--[no]eks_verify_ssl`: Whether to verify the ssl certificate when communicating
    with the EKS service. This requires SNI support which is not available in
    the SSL modules of Python < 2.7.9.
    (default: 'true')

`--eks_zones`: The zones into which the EKS cluster will be deployed. There must
    be at least two zones and all zones must be from the same region.
    (default: 'us-east-1a,us-east-1c')
    (a comma separated list)

`--kops`: The path to the kops binary.
    (default: 'kops')

`--s3_custom_endpoint`: If given, a custom endpoint to use for S3 transfers. If
    this flag is not given, use the standard endpoint for the storage region.

### [perfkitbenchmarker.providers.azure.flags ](../perfkitbenchmarker/providers/azure/flags.py)

#### Description:

Module containing flags applicable across benchmark run on Azure.

#### Flags:

`--[no]azure_accelerated_networking`: Enable Azure Accelerated Networking. See
    https://docs.microsoft.com/en-us/azure/virtual-network/create-vm-
    accelerated-networking-clifor more information.
    (default: 'false')

`--azure_blob_account_kind`: <Storage|BlobStorage>: The type of storage account
    to use for blob storage. Choosing Storage will let you use ZRS storage.
    Choosing BlobStorage will give you access to Hot and Cold storage tiers.
    (default: 'BlobStorage')

`--azure_compute_units`: Number of compute units to allocate for the machine
    type
    (an integer)

`--azure_host_caching`: <None|ReadOnly|ReadWrite>: The type of host caching to
    use on Azure data disks.
    (default: 'None')

`--azure_preprovisioned_data_bucket`: Azure blob storage account where pre-
    provisioned data has been copied.

`--azure_redis_size`: <C0|C1|C2|C3|C4|C5|C6|P1|P2|P3|P4|P5>: Azure redis cache
    size to use.
    (default: 'C3')

`--azure_storage_type`:
    <Standard_LRS|Premium_LRS|Standard_ZRS|Standard_GRS|Standard_RAGRS>: The
    type of storage account to create. See http://azure.microsoft.com/en-
    us/pricing/details/storage/ for more information. To use remote ssd scratch
    disks, you must use Premium_LRS. If you use Premium_LRS, you must use the DS
    series of machines, or else VM creation will fail.
    (default: 'Standard_LRS')

`--azure_tier`: <Basic|Standard|Premium>: Performance tier to use for the
    machine type. Defaults to Basic.
    (default: 'Basic')

### [perfkitbenchmarker.providers.cloudstack.flags ](../perfkitbenchmarker/providers/cloudstack/flags.py)

#### Description:

No description available

#### Flags:

`--CS_API_KEY`: Key for API authentication

`--CS_API_SECRET`: Secret for API authentication

`--CS_API_URL`: API endpoint for Cloudstack.

`--cs_network_offering`: Name of the network offering
    (default: 'DefaultIsolatedNetworkOfferingForVpcNetworksNoLB')

`--[no]cs_use_vpc`: Use VPC to create networks
    (default: 'true')

`--cs_vpc_offering`: Name of the VPC offering
    (default: 'Default VPC offering')

### [perfkitbenchmarker.providers.gcp.flags ](../perfkitbenchmarker/providers/gcp/flags.py)

#### Description:

Module containing flags applicable across benchmark run on GCP.

#### Flags:

`--additional_gcloud_flags`: Additional flags to pass to gcloud.
    (default: '')
    (a comma separated list)

`--gce_accelerator_type_override`: When specified, override the accelerator_type
    string passed to the gcloud compute instance create command.

`--gce_boot_disk_size`: The boot disk size in GB for GCP VMs.
    (an integer)

`--gce_boot_disk_type`: <pd-standard|pd-ssd>: The boot disk type for GCP VMs.

`--[no]gce_migrate_on_maintenance`: If true, allow VM migration on GCE host
    maintenance.
    (default: 'true')

`--gce_network_name`: The name of an already created network to use instead of
    creating a new one.

`--gce_network_tier`: <premium|standard>: Network tier to use for all GCE VMs.
    Note that standard networking is only available in certain regions. See
    https://cloud.google.com/network-tiers/docs/overview
    (default: 'premium')

`--gce_num_local_ssds`: The number of ssds that should be added to the VM. Note
    that this is currently only supported in certain zones (see
    https://cloud.google.com/compute/docs/local-ssd).
    (default: '0')
    (an integer)

`--[no]gce_preemptible_vms`: If true, use preemptible VMs on GCE.
    (default: 'false')

`--gce_remote_access_firewall_rule`: The name of an already created firewall
    rule which allows remote access instead of creating a new one.

`--[no]gce_shielded_secure_boot`: Whether the image uses the shielded VM feature
    (default: 'false')

`--gce_ssd_interface`: <SCSI|NVME>: The ssd interface for GCE local SSD.
    (default: 'SCSI')

`--gce_subnet_addr`: Address range to the subnet, given in CDR notation. Not
    used unless --gce_subnet_region is given.
    (default: '10.128.0.0/20')

`--gce_subnet_region`: Region to create subnet in instead of automatically
    creating one in every region.

`--gce_tags`: List of --tags when creating a VM
    (a comma separated list)

`--gcloud_path`: The path for the gcloud utility.
    (default: 'gcloud')

`--gcloud_scopes`: If set, space-separated list of scopes to apply to every
    created machine

`--gcp_dataproc_image`: Specifies the custom image URI or the custom image name
    that will be used to create a cluster.

`--gcp_dataproc_property`: Specifies configuration properties for installed
    packages, such as Hadoop and Spark. Properties are mapped to configuration
    files by specifying a prefix, such as "core:io.serializations". See
    https://cloud.google.com/dataproc/docs/concepts/configuring-clusters
    /cluster-properties for details.;
    repeat this option to specify a list of values
    (default: '[]')

`--gcp_dataproc_subnet`: Specifies the subnet that the cluster will be part of.

`--gcp_instance_metadata`: A colon separated key-value pair that will be added
    to the "--metadata" flag of the gcloud cli (with the colon replaced by the
    equal sign). Multiple key-value pairs may be specified by separating each
    pair by commas. This option can be repeated multiple times. For information
    about GCP instance metadata, see: --metadata from `gcloud help compute
    instances create`.;
    repeat this option to specify a list of values
    (default: '[]')

`--gcp_instance_metadata_from_file`: A colon separated key-value pair that will
    be added to the "--metadata-from-file" flag of the gcloud cli (with the
    colon replaced by the equal sign). Multiple key-value pairs may be specified
    by separating each pair by commas. This option can be repeated multiple
    times. For information about GCP instance metadata, see: --metadata-from-
    file from `gcloud help compute instances create`.;
    repeat this option to specify a list of values
    (default: '[]')

`--[no]gcp_internal_ip`: Use internal ips for ssh or scp commands. gcloud
    betacomponents must be installed to use this flag.
    (default: 'false')

`--gcp_min_cpu_platform`:
    <none|sandybridge|ivybridge|haswell|broadwell|skylake>: When specified, the
    VM will have either the specified architecture or a newer one. Architecture
    availability is zone dependent.

`--gcp_node_type`: The node type of all sole tenant hosts that get created.

`--gcp_num_vms_per_host`: The number of VMs per dedicated host. If None, VMs
    will be packed on a single host until no more can be packed at which point a
    new host will be created.
    (an integer)

`--gcp_preprovisioned_data_bucket`: GCS bucket where pre-provisioned data has
    been copied.

`--gcp_redis_gb`: Size of redis cluster in gb
    (default: '5')
    (an integer)

`--gcp_service_account`: Service account to use for authorization.

`--gcp_service_account_key_file`: Local path to file that contains a private
    authorization key, used to activate gcloud.

`--[no]gke_enable_alpha`: Whether to enable alpha kubernetes clusters.
    (default: 'false')

`--image_family`: The family of the image that the boot disk will be initialized
    with. The --image flag will take priority over this flag. See:
    https://cloud.google.com/sdk/gcloud/reference/compute/instances/create

`--image_project`: The project against which all image references will be
    resolved. See:
    https://cloud.google.com/sdk/gcloud/reference/compute/disks/create

### [perfkitbenchmarker.providers.gcp.gcp_bigtable ](../perfkitbenchmarker/providers/gcp/gcp_bigtable.py)

#### Description:

Module containing class for GCP's bigtable instances.

Clusters can be created and deleted.


#### Flags:

`--bigtable_node_count`: Number of nodes to create in the bigtable cluster.
    (default: '3')
    (an integer)

`--bigtable_storage_type`: <ssd|hdd>: Storage class for the cluster
    (default: 'ssd')

`--google_bigtable_zone`: Bigtable zone.
    (default: 'us-central1-b')

### [perfkitbenchmarker.providers.gcp.gcp_dpb_dataflow ](../perfkitbenchmarker/providers/gcp/gcp_dpb_dataflow.py)

#### Description:

Module containing class for GCP's dataflow service.

No Clusters can be created or destroyed, since it is a managed solution
See details at: https://cloud.google.com/dataflow/


#### Flags:

`--dpb_dataflow_runner`: Flag to specify the pipeline runner at runtime.
    (default: 'DataflowRunner')

`--dpb_dataflow_sdk`: SDK used to build the Dataflow executable.

`--dpb_dataflow_staging_location`: Google Cloud Storage bucket for Dataflow to
    stage the binary and any temporary files. You must create this bucket ahead
    of time, before running your pipeline.

### [perfkitbenchmarker.providers.gcp.gcp_dpb_dataproc ](../perfkitbenchmarker/providers/gcp/gcp_dpb_dataproc.py)

#### Description:

Module containing class for GCP's dataproc service.

Clusters can be created, have jobs submitted to them and deleted. See details
at https://cloud.google.com/dataproc/


#### Flags:

`--dpb_dataproc_distcp_num_maps`: Number of maps to copy data.
    (an integer)

`--dpb_dataproc_image_version`: The image version to use for the cluster.

### [perfkitbenchmarker.providers.gcp.gcp_spanner ](../perfkitbenchmarker/providers/gcp/gcp_spanner.py)

#### Description:

Module containing class for GCP's spanner instances.

Instances can be created and deleted.


#### Flags:

`--cloud_spanner_config`: The config for the Cloud Spanner instance.
    (default: 'regional-us-central1')

`--cloud_spanner_nodes`: The number of nodes for the Cloud Spanner instance.
    (default: '1')
    (an integer)

`--cloud_spanner_project`: The project for the Cloud Spanner instance. Use
    default project if unset.

### [perfkitbenchmarker.providers.gcp.gcs ](../perfkitbenchmarker/providers/gcp/gcs.py)

#### Description:

Contains classes/functions related to Google Cloud Storage.

#### Flags:

`--google_cloud_sdk_version`: Use a particular version of the Google Cloud SDK,
    e.g.: 103.0.0

### [perfkitbenchmarker.providers.kubernetes.flags ](../perfkitbenchmarker/providers/kubernetes/flags.py)

#### Description:

No description available

#### Flags:

`--ceph_keyring`: Path to the Ceph keyring file.
    (default: '/etc/ceph/keyring')

`--ceph_monitors`: IP addresses and ports of Ceph Monitors. Must be provided
    when Ceph scratch disk is required. Example:
    "127.0.0.1:6789,192.168.1.1:6789"
    (default: '')
    (a comma separated list)

`--ceph_secret`: Name of the Ceph Secret used by Kubernetes in order to
    authenticate with Ceph. If provided, overrides keyring.

`--[no]docker_in_privileged_mode`: If set to True, will attempt to create Docker
    containers in a privileged mode. Note that some benchmarks execute commands
    which are only allowed in privileged mode.
    (default: 'true')

`--k8s_volume_parameters`: A colon separated key-value pair that will be added
    to Kubernetes storage class parameters.;
    repeat this option to specify a list of values

`--k8s_volume_provisioner`: <kubernetes.io/azure-disk|kubernetes.io/gce-
    pd|kubernetes.io/aws-ebs|kubernetes.io/glusterfs>: The name of the
    provisioner to use for K8s storage classes.

`--[no]kubernetes_anti_affinity`: If set to True, PKB pods will not be scheduled
    on the same nodes as other PKB pods.
    (default: 'true')

`--rbd_pool`: Name of RBD pool for Ceph volumes.
    (default: 'rbd')

`--rbd_user`: Name of RADOS user.
    (default: 'admin')

`--username`: User name that Perfkit will attempt to use in order to SSH into
    Docker instance.
    (default: 'root')

### [perfkitbenchmarker.providers.mesos.flags ](../perfkitbenchmarker/providers/mesos/flags.py)

#### Description:

No description available

#### Flags:

`--docker_cpus`: CPU limit for docker containers.
    (default: '1.0')
    (a number)

`--docker_memory_mb`: Memory limit for docker containers.
    (default: '2048')
    (an integer)

`--marathon_address`: Marathon IP address and port.
    (default: 'localhost:8080')

`--marathon_auth`: Marathon server basic authentication.
    (default: 'root:password')

`--[no]mesos_privileged_docker`: If set to True, will attempt to create Docker
    containers in a privileged mode. Note that some benchmarks execute commands
    which are only allowed in privileged mode.
    (default: 'false')

### [perfkitbenchmarker.providers.openstack.flags ](../perfkitbenchmarker/providers/openstack/flags.py)

#### Description:

No description available

#### Flags:

`--openstack_additional_flags`: Additional comma separated flags to pass to
    every OpenStack CLI command. See "openstack --help" for more.
    (default: '')
    (a comma separated list)

`--[no]openstack_boot_from_volume`: Boot from volume instead of an image
    (default: 'false')

`--openstack_cli_path`: The path to the OpenStack CLI binary.
    (default: 'openstack')

`--[no]openstack_config_drive`: Add possibilities to get metadata from external
    drive
    (default: 'false')

`--openstack_floating_ip_pool`: Name of OpenStack floating IP-address pool. If
    set, a floating-ip address from this pool will be associatedto each instance
    and will be used for communicating with it. To use this flag, an internally
    routable network must also be specified via the openstack_network flag.

`--openstack_image_username`: Ssh username for cloud image
    (default: 'ubuntu')

`--openstack_network`: Name of OpenStack network. This network provides
    automatically allocated fixed-IP addresses to attached instances. Typically,
    this network is used for internal communication between instances. If
    openstack_floating_ip_pool is not set then this network will be used to
    communicate with the instance.
    (default: 'private')

`--openstack_private_network`: (DEPRECATED: Use openstack_network) Name of
    OpenStack private network.
    (default: 'private')

`--openstack_public_network`: (DEPRECATED: Use openstack_floating_ip_pool) Name
    of OpenStack public network.

`--openstack_scheduler_policy`: <None|affinity|anti-affinity>: Add possibility
    to use affinity or anti-affinity policy in scheduling process
    (default: 'None')

`--openstack_volume_size`: (DEPRECATED: Use data_disk_size) Size of the volume
    (GB).
    (an integer)

`--openstack_volume_type`: Optional Cinder volume type to use.

### [perfkitbenchmarker.providers.profitbricks.flags ](../perfkitbenchmarker/providers/profitbricks/flags.py)

#### Description:

No description available

#### Flags:

`--availability_zone`: <AUTO|ZONE_1|ZONE_2|ZONE_3>: Direct a storage volume to
    be created in one of three zones per data center (AUTO, ZONE_1, ZONE_2,
    ZONE_3)
    (default: 'AUTO')

`--profitbricks_boot_volume_size`: Choose the boot volume size in GB.
    (default: '10')
    (an integer)

`--profitbricks_boot_volume_type`: <HDD|SSD>: Choose between HDD or SSD boot
    volume types.
    (default: 'HDD')

`--profitbricks_config`: Path to config file containing your email and password.
    Can also be set via $PROFITBRICKS_CONFIG environment variable.
    (File format: email:password)
    (default: '~/.config/profitbricks-auth.cfg')

`--profitbricks_image_alias`: An alias to a ProfitBricks public image. If given,
    it will be used instead of a default Ubuntu 14 image. E.g., "ubuntu:latest"
    indicates the latest version of Ubuntu is to be used to provision a volume.

`--profitbricks_location`: <us/las|us/ewr|de/fkb|de/fra>: Location of data
    center to be provisioned (us/las, us/ewr, de/fkb, de/fra)
    (default: 'us/las')

### [perfkitbenchmarker.providers.rackspace.flags ](../perfkitbenchmarker/providers/rackspace/flags.py)

#### Description:

No description available

#### Flags:

`--additional_rackspace_flags`: Additional global flags to pass to every RackCLI
    command. See "rack --help" for more.
    (default: '')
    (a comma separated list)

`--rack_path`: The path for the rack CLI binary.
    (default: 'rack')

`--rack_profile`: A string indicating which RackCLI profile to use. If none is
    specified default profile is used (see https://developer.rackspace.com/docs
    /rack-cli/configuration/#config-file)

`--[no]rackspace_boot_from_cbs_volume`: When flag is included the instance will
    use a remote disk as its boot disk, if machine_type supports it.
    (default: 'false')

`--rackspace_network_id`: (EXPERIMENTAL) The ID of an already created network to
    use instead of creating a new one. Must have a subnet already associated
    with the network.

`--rackspace_region`: A string indicating which Rackspace region to use.
    (default: 'IAD')

`--[no]rackspace_use_security_group`: (EXPERIMENTAL) A boolean indicating
    whether or not to create a security group for the new instance. Applies
    default security group rules (e.g. allow ingress TCP, and UDP traffic
    through port 22). If no security group is used, all incoming and outgoing
    traffic through TCP, UDP and ICMP is allowed, this is the default.
    (default: 'false')

### [perfkitbenchmarker.publisher ](../perfkitbenchmarker/publisher.py)

#### Description:

Classes to collect and publish performance samples to various sinks.

#### Flags:

`--bigquery_table`: The BigQuery table to publish results to. This should be of
    the form "[project_id:]dataset_name.table_name".

`--bq_path`: Path to the "bq" executable.
    (default: 'bq')

`--bq_project`: Project to use for authenticating with BigQuery.

`--cloud_storage_bucket`: GCS bucket to upload records to. Bucket must exist.

`--[no]collapse_labels`: Collapse entries in labels in JSON output.
    (default: 'true')

`--csv_path`: A path to write CSV-format results

`--es_index`: Elasticsearch index name to store documents
    (default: 'perfkit')

`--es_type`: Elasticsearch document type
    (default: 'result')

`--es_uri`: The Elasticsearch address and port. e.g. http://localhost:9200

`--gsutil_path`: path to the "gsutil" executable
    (default: 'gsutil')

`--[no]hostname_metadata`: A boolean indicating whether to publish VM hostnames
    as part of sample metadata.
    (default: 'false')

`--influx_db_name`: Name of Influx DB database that you wish to publish to or
    create
    (default: 'perfkit')

`--influx_uri`: The Influx DB address and port. Expects the format
    hostname:portIf port is not passed in it assumes port 80. e.g.
    localhost:8086

`--json_path`: A path to write newline-delimited JSON results Default: write to
    a run-specific temporary directory

`--json_write_mode`: <w|a>: Open mode for file specified by --json_path.
    Default: overwrite file
    (default: 'w')

`--metadata`: A colon separated key-value pair that will be added to the labels
    field of all samples as metadata. Multiple key-value pairs may be specified
    by separating each pair by commas.;
    repeat this option to specify a list of values
    (default: '[]')

`--[no]official`: A boolean indicating whether results are official or not. The
    default is False. Official test results are treated and queried differently
    from non-official test results.
    (default: 'false')

`--product_name`: The product name to use when publishing results.
    (default: 'PerfKitBenchmarker')

`--service_account`: Service account to use to authenticate with BQ.

`--service_account_private_key`: Service private key for authenticating with BQ.

### [perfkitbenchmarker.smb_service ](../perfkitbenchmarker/smb_service.py)

#### Description:

Resource encapsulating provisioned cloud SMB services.

Defines a resource for use in other benchmarks such as SpecSFS2014 and FIO.

Example --benchmark_config_file:

smb_10_tb: &smb_10_tb
  Azure:
    disk_type: smb
    mount_point: /scratch

specsfs:
  name: specsfs2014
  flags:
    specsfs2014_num_runs: 1
    specsfs2014_load: 1
  vm_groups:
    clients:
      disk_spec: *smb_10_tb
      vm_count: 1
      os_type: rhel
    gluster_servers:
      vm_count: 0


#### Flags:

`--smb_tier`: SMB Mode
    (default: 'Standard')

### [perfkitbenchmarker.spark_service ](../perfkitbenchmarker/spark_service.py)

#### Description:

Benchmarking support for Apache Spark services.

In order to benchmark Apache Spark services such as Google Cloud
Platform's Dataproc or Amazon's EMR, we create a BaseSparkService
class.  Classes to wrap each provider's Apache Spark Service are
in the provider directory as a subclass of BaseSparkService.

Also in this module is a PkbSparkService, which builds a Spark
cluster by creating VMs and installing the necessary software.

For more on Apache Spark: http://spark.apache.org/


#### Flags:

`--spark_service_log_level`: <DEBUG|INFO|FATAL>: Supported log levels when
    submitting jobs to spark service clusters.
    (default: 'INFO')

`--spark_static_cluster_id`: If set, the name of the Spark cluster, assumed to
    be ready.

### [perfkitbenchmarker.stages ](../perfkitbenchmarker/stages.py)

#### Description:

Variables and classes related to the different stages of a PKB run.

#### Flags:

`--run_stage`: The stage or stages of perfkitbenchmarker to run.
    (default: 'provision,prepare,run,cleanup,teardown')
    (A complete benchmark execution consists of 5 stages: provision, prepare,
    run, cleanup, teardown. Possible flag values include an individual stage, a
    comma-separated list of stages, or 'all'. If a list of stages is provided,
    they must be in order without skipping any stage.)

### [perfkitbenchmarker.static_virtual_machine ](../perfkitbenchmarker/static_virtual_machine.py)

#### Description:

Class to represent a Static Virtual Machine object.

All static VMs provided in a given group will be used before any non-static
VMs are provisioned. For example, in a test that uses 4 VMs, if 3 static VMs
are provided, all of them will be used and one additional non-static VM
will be provisioned. The VM's should be set up with passwordless ssh and
passwordless sudo (neither sshing nor running a sudo command should prompt
the user for a password).

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.


#### Flags:

`--static_vm_tags`: The tags of static VMs for PKB to run with. Even if other
    VMs are specified in a config, if they aren't in this list they will be
    skipped during VM creation.
    (a comma separated list)

### [perfkitbenchmarker.temp_dir ](../perfkitbenchmarker/temp_dir.py)

#### Description:

Functions related to PerfKit Benchmarker's temporary directory.

PerfKit Benchmarker creates files under a temporary directory (typically in
/tmp/perfkitbenchmarker or C:\TEMP\perfkitbenchmarker - see tempfile.tempdir for
more information).


#### Flags:

`--temp_dir`: Temp directory PKB uses.
    (default: '/tmp/perfkitbenchmarker')

### [perfkitbenchmarker.timing_util ](../perfkitbenchmarker/timing_util.py)

#### Description:

Utilities for generating timing samples.

#### Flags:

`--timing_measurements`: Comma-separated list of values from
    <none|end_to_end_runtime|runtimes|timestamps> that selects which timing
    measurements to enable. Measurements will be included as samples in the
    benchmark results. none: No measurements included (same as providing an
    empty list, and cannot be combined with other options). end_to_end_runtime:
    Includes an end-to-end runtime measurement. runtimes: Includes runtimes of
    all measured intervals, including the end-to-end runtime, the time taken by
    the benchmark module Prepare, Run, and Cleanup functions, and other
    important intervals. timestamps: Includes start and stop timestamps of all
    measured intervals.
    (default: 'end_to_end_runtime')
    (a comma separated list)

### [perfkitbenchmarker.traces.collectd ](../perfkitbenchmarker/traces/collectd.py)

#### Description:

Records system performance counters during benchmark runs using collectd.

http://collectd.org


#### Flags:

`--[no]collectd`: Install and run collectd on the guest.
    (default: 'false')

`--collectd_output`: Path to store collectd results.

### [perfkitbenchmarker.traces.dstat ](../perfkitbenchmarker/traces/dstat.py)

#### Description:

Records system performance counters during benchmark runs using dstat.

http://dag.wiee.rs/home-made/dstat/


#### Flags:

`--[no]dstat`: Run dstat (http://dag.wiee.rs/home-made/dstat/) on each VM to
    collect system performance metrics during each benchmark run.
    (default: 'false')

`--dstat_interval`: dstat sample collection frequency, in seconds. Only
    applicable when --dstat is specified.
    (an integer)

`--dstat_output`: Output directory for dstat output. Only applicable when
    --dstat is specified. Default: run temporary directory.

`--[no]dstat_publish`: Whether to publish average dstat statistics.
    (default: 'false')

`--dstat_publish_regex`: Requires setting dstat_publish to true. If specified,
    any dstat statistic matching this regular expression will be published such
    that each individual statistic will be in a sample with the time since the
    epoch in the metadata. Examples. Use ".*" to record all samples. Use "net"
    to record networking statistics.

### [perfkitbenchmarker.traces.sar ](../perfkitbenchmarker/traces/sar.py)

#### Description:

Records cpu performance counters during benchmark runs using sar.

#### Flags:

`--[no]sar`: Run sar (https://linux.die.net/man/1/sar) on each VM to collect
    system performance metrics during each benchmark run.
    (default: 'false')

`--sar_interval`: sar sample collection frequency, in seconds. Only applicable
    when --sar is specified.
    (default: '5')
    (an integer)

`--sar_output`: Output directory for sar output. Only applicable when --sar is
    specified. Default: run temporary directory.

`--[no]sar_publish`: Whether to publish average sar statistics.
    (default: 'true')

`--sar_samples`: Number of sar samples to collect. When undefined sar is ran
    indefinitely. This must be set to record average statistics. Only applicable
    when --sar is specified.
    (an integer)

### [perfkitbenchmarker.virtual_machine ](../perfkitbenchmarker/virtual_machine.py)

#### Description:

Class to represent a Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.


#### Flags:

`--[no]dedicated_hosts`: If True, use hosts that only have VMs from the same
    benchmark running on them.
    (default: 'false')

`--num_cpus_override`: Rather than detecting the number of CPUs present on the
    machine, use this value if set. Some benchmarks will use this number to
    automatically scale their configurations; this can be used as a method to
    control benchmark scaling. It will also change the num_cpus metadata
    published along with the benchmark data.
    (an integer)

`--[no]skip_firewall_rules`: If set, this run will not create firewall rules.
    This is useful if the user project already has all of the firewall rules in
    place and/or creating new ones is expensive
    (default: 'false')

`--vm_metadata`: Metadata to add to the vm via the provider's AddMetadata
    function. It expectskey:value pairs
    (default: '')
    (a comma separated list)

### [perfkitbenchmarker.vm_util ](../perfkitbenchmarker/vm_util.py)

#### Description:

Set of utility functions for working with virtual machines.

#### Flags:

`--background_cpu_threads`: Number of threads of background cpu usage while
    running a benchmark
    (an integer)

`--background_network_ip_type`: <INTERNAL|EXTERNAL>: IP address type to use when
    generating background network traffic
    (default: 'EXTERNAL')

`--background_network_mbits_per_sec`: Number of megabits per second of
    background network traffic to generate during the run phase of the benchmark
    (an integer)

`--burn_cpu_seconds`: Amount of time in seconds to burn cpu on vm before
    starting benchmark
    (default: '0')
    (an integer)

`--burn_cpu_threads`: Number of threads to use to burn cpu before starting
    benchmark.
    (default: '1')
    (an integer)

`--default_timeout`: The default timeout for retryable commands in seconds.
    (default: '1200')
    (an integer)

`--ip_addresses`: <REACHABLE|BOTH|INTERNAL|EXTERNAL>: For networking tests: use
    both internal and external IP addresses (BOTH), external and internal only
    if the receiving VM is reachable by internal IP (REACHABLE), external IP
    only (EXTERNAL) or internal IP only (INTERNAL)
    (default: 'REACHABLE')

`--[no]simulate_maintenance`: Whether to simulate VM maintenance during the
    benchmark. This requires both benchmark and provider support.
    (default: 'false')

`--simulate_maintenance_delay`: The number of seconds to wait to start
    simulating maintenance.
    (default: '0')
    (an integer)

`--ssh_control_path`: Overrides the default ControlPath setting for ssh
    connections if --ssh_reuse_connections is set. This can be helpful on
    systems whose default temporary directory path is too long (sockets have a
    max path length) or a version of ssh that doesn't support the %C token. See
    ssh documentation on the ControlPath setting for more detailed information.

`--ssh_control_persist`: Setting applied to ssh connections if
    --ssh_reuse_connections is set. Sets how long the connections persist before
    they are removed. See ssh documentation about the ControlPersist setting for
    more detailed information.
    (default: '30m')

`--[no]ssh_reuse_connections`: Whether to reuse SSH connections rather than
    reestablishing a connection for each remote command.
    (default: 'true')

`--ssh_server_alive_count_max`: Value for ssh -o ServerAliveCountMax. Use with
    --ssh_server_alive_interval to configure how long to wait for unresponsive
    servers.
    (default: '10')
    (an integer)

`--ssh_server_alive_interval`: Value for ssh -o ServerAliveInterval. Use with
    --ssh_server_alive_count_max to configure how long to wait for unresponsive
    servers.
    (default: '30')
    (an integer)

### [perfkitbenchmarker.windows_benchmarks.fio_benchmark ](../perfkitbenchmarker/windows_benchmarks/fio_benchmark.py)

#### Description:

Runs fio benchmarks.



#### Flags:

`--fio_file_size`: "filesize" field of the global section of the fio config.
    This is the size of the individual files. Default is 4 * (System Memory) or
    100GB, whichever is smaller.
    (an integer)

`--fio_random_read_parallel_size`: "size" field of the random_read_parallel
    section of the fio config. This is the size of I/O for this job. fio will
    run until this many bytes have been transferred. The default is 2 * (System
    Memory) or 100GB, whichever is smaller.
    (an integer)

`--fio_random_read_size`: "size" field of the random_read section of the fio
    config. This is the size of I/O for this job. fio will run until this many
    bytes have been transferred. The default is 2 * (System Memory) or 100GB,
    whichever is smaller.
    (an integer)

`--fio_random_write_size`: "size" field of the random_write section of the fio
    config. This is the size of I/O for this job. fio will run until this many
    bytes have been transferred. The default is 2 * (System Memory) or 100GB,
    whichever is smaller.
    (an integer)

`--fio_sequential_read_size`: "size" field of the sequential_read section of the
    fio config. This is the size of I/O for this job. fio will run until this
    many bytes have been transferred. The default is 4 * (System Memory) or
    100GB, whichever is smaller.
    (an integer)

`--fio_sequential_write_size`: "size" field of the sequential_write section of
    the fio config. This is the size of I/O for this job. fio will run until
    this many bytes have been transferred. The default is 4 * (System Memory) or
    100GB, whichever is smaller.
    (an integer)

### [perfkitbenchmarker.windows_packages.diskspd ](../perfkitbenchmarker/windows_packages/diskspd.py)

#### Description:

Module containing DiskSpd installation and cleanup functions.

DiskSpd is a tool made for benchmarking Windows disk performance.

More information about DiskSpd may be found here:
https://gallery.technet.microsoft.com/DiskSpd-a-robust-storage-6cd2f223


#### Flags:

`--diskspd_access_pattern`: <s|r>: the access patten of the read and writethe
    performance will be downgrade a little bit if usedifferent hintsavailable
    option: r|s, r: random accesss: sequential access. Defaults: s.
    (default: 's')

`--diskspd_block_size`: The block size used when reading and writing data.
    Defaults: 64K. Unit: KB, can be set via --diskspd_block_unit
    (default: '64')
    (an integer)

`--diskspd_block_unit`: <K|M|G>: The unit of the block size, available option:
    K|M|G. Will be used as the unit for --diskspd_block_size Defaults: K.
    (default: 'K')

`--diskspd_config_list`: comma separated list of configs to run with diskspd.
    The format for a single config is RANDOM_ACCESS:IS_READ:BLOCK_SIZE, for
    example FALSE:TRUE:64. Default Behavior: diskspd benchmark test will try to
    combine--diskspd_access_pattern, --diskspd_write_read_ratio,
    --diskspd_block_size together and form a set a config to run.
    (a comma separated list)

`--diskspd_cooldown`: The cool down time for diskspd, the time to ensure
    thateach instance of diskspd is active during eachmeasurement period of each
    instance. Defaults: 5s. Unit: seconds
    (default: '5')
    (an integer)

`--[no]diskspd_disable_affinity`: Whether to diable the group affinity,group
    affinity is to round robin tasks. across processor group. Defaults: False
    (default: 'false')

`--diskspd_duration`: The number of seconds to run diskspd test.Defaults to 30s.
    Unit: seconds.
    (default: '20')
    (an integer)

`--diskspd_file_size`: The file size DiskSpd will create when testing. Defaults:
    819200. Unit: KB.
    (default: '819200')
    (an integer)

`--[no]diskspd_large_page`: Whether use large page for IO buffers. Defaults:
    False
    (default: 'false')

`--[no]diskspd_latency_stats`: Whether measure the latency statisticsDefaults:
    False
    (default: 'false')

`--diskspd_outstanding_io`: The number of outstanding I/O per thread per
    target.Defaults: 2.
    (default: '2')
    (an integer)

`--[no]diskspd_software_cache`: Whether to disable software cachingDefaults:
    True
    (default: 'true')

`--diskspd_stride_or_alignment`: If the access pattern is sequential, then this
    valuemeans the stride for the accessIf the access pattern is random, then
    this value meansthe specified number of bytes that random I/O aligns
    to.Defaults: 64K. Unit: KB, can be set
    (default: '64')
    (an integer)

`--diskspd_stride_or_alignment_unit`: <K|M|G|b>: The unit of the
    stride_or_alignment,available option: K|M|G|bDefaults: K.
    (default: 'K')

`--diskspd_thread_number_per_file`: The thread number created per file toperform
    read and write. Defaults: 1.
    (default: '1')
    (an integer)

`--diskspd_throughput_per_ms`: The throughput per thread per target. Defaults:
    None. Unit: bytes per ms.
    (an integer)

`--diskspd_warmup`: The warm up time for diskspd, the time needed to entersteady
    state of I/O operation. Defaults to 5s. Unit: seconds.
    (default: '5')
    (an integer)

`--diskspd_write_read_ratio`: The ratio of write workload to read
    workload.Example: 50 means 50%, and write and read each takes50% of the
    total I/O data.To test read speed, set this value to 0. To test write speed,
    set this value to 100. Defaults: 0. Unit: percent.
    (default: '0')
    (an integer)

`--[no]diskspd_write_through`: Whether to enable write through IO. Defaults:
    True
    (default: 'true')

### [perfkitbenchmarker.windows_packages.hammerdb ](../perfkitbenchmarker/windows_packages/hammerdb.py)

#### Description:

Module containing HammerDB installation and cleanup functions.

HammerDB is a tool made for benchmarking sql performance.

More information about HammerDB may be found here:
https://www.hammerdb.com/index.html


#### Flags:

`--[no]hammerdb_run_tpcc`: tpcc is a sql benchmark to measure transaction
    speed.Default: True
    (default: 'true')

`--[no]hammerdb_run_tpch`: tpch is a sql benchmark to calculate the query per
    hourperformance metrics.Default: True
    (default: 'true')

`--hammerdb_tpcc_runtime`: The running time for tpcc benchmark testDefault: 60.
    Unit: second
    (default: '60')
    (an integer)

`--hammerdb_tpcc_schema_virtual_user`: The number of virtual user used when
    building the schema. Default: 1
    (default: '1')
    (an integer)

`--hammerdb_tpcc_virtual_user_list`: The list of numbers of virtual users making
    transaction at the same time. Default: [1]
    (default: '1')
    (a comma separated list)

`--hammerdb_tpcc_warehouse`: The number of warehouse used in tpcc benchmarking.
    Default: 1
    (default: '1')
    (an integer)

`--hammerdb_tpch_scale_fact`: The running time for tpcc benchmark test. Default:
    60. Unit: second
    (default: '1')
    (an integer)

`--hammerdb_tpch_virtual_user`: The virtual user number to run tpch test.
    Default: 4.
    (default: '4')
    (an integer)

### [perfkitbenchmarker.windows_packages.iperf3 ](../perfkitbenchmarker/windows_packages/iperf3.py)

#### Description:

Module containing Iperf3 windows installation and cleanup functions.

#### Flags:

`--bandwidth_step_mb`: The amount of megabytes to increase bandwidth in each UDP
    stream test.
    (default: '100')
    (an integer)

`--max_bandwidth_mb`: The maximum bandwidth, in megabytes, to test in a UDP
    stream.
    (default: '500')
    (an integer)

`--min_bandwidth_mb`: The minimum bandwidth, in megabytes, to test in a UDP
    stream.
    (default: '100')
    (an integer)

`--[no]run_tcp`: setting to false will disable the run of the TCP test
    (default: 'true')

`--[no]run_udp`: setting to true will enable the run of the UDP test
    (default: 'false')

`--socket_buffer_size`: The socket buffer size in megabytes. If None is
    specified then the socket buffer size will not be set.
    (an integer)

`--tcp_number_of_streams`: The number of parrallel streams to run in the TCP
    test.
    (default: '10')
    (an integer)

`--tcp_stream_seconds`: The amount of time to run the TCP stream test.
    (default: '3')
    (an integer)

`--udp_buffer_len`: UDP packet size in bytes.
    (default: '100')
    (an integer)

`--udp_client_threads`: Number of parallel client threads to run.
    (default: '1')
    (an integer)

`--udp_stream_seconds`: The amount of time to run the UDP stream test.
    (default: '3')
    (an integer)

### [perfkitbenchmarker.windows_packages.ntttcp ](../perfkitbenchmarker/windows_packages/ntttcp.py)

#### Description:

Module containing NTttcp installation and cleanup functions.

NTttcp is a tool made for benchmarking Windows networking.

More information about NTttcp may be found here:
https://gallery.technet.microsoft.com/NTttcp-Version-528-Now-f8b12769


#### Flags:

`--ntttcp_config_list`: comma separated list of configs to run with ntttcp. The
    format for a single config is UDP:THREADS:RUNTIME_S:IP_TYPE:PACKET_SIZE, for
    example True:4:60:INTERNAL:0,False:8:60:EXTERNAL:150
    (default: '')
    (a comma separated list)

`--ntttcp_cooldown_time`: Time to wait between the test runs.
    (default: '60')
    (an integer)

`--ntttcp_packet_size`: The size of the packet being used in the test.
    (an integer)

`--ntttcp_receiver_rb`: The size of the receive buffer, in Kilo Bytes, on the
    receiving VM. The default is the OS default.
    (default: '-1')
    (an integer)

`--ntttcp_receiver_sb`: The size of the send buffer, in Kilo Bytes, on the
    receiving VM. The default is the OS default.
    (default: '-1')
    (an integer)

`--ntttcp_sender_rb`: The size of the receive buffer, in Kilo Bytes, on the
    sending VM. The default is the OS default.
    (default: '-1')
    (an integer)

`--ntttcp_sender_sb`: The size of the send buffer, in Kilo Bytes, on the sending
    VM. The default is the OS default.
    (default: '-1')
    (an integer)

`--ntttcp_threads`: The number of client and server threads for NTttcp to run
    with.
    (default: '1')
    (an integer)

`--ntttcp_time`: The number of seconds for NTttcp to run.
    (default: '60')
    (an integer)

`--[no]ntttcp_udp`: Whether to run a UDP test.
    (default: 'false')

### [perfkitbenchmarker.windows_packages.nuttcp ](../perfkitbenchmarker/windows_packages/nuttcp.py)

#### Description:

Module containing nuttcp installation and cleanup functions.

#### Flags:

`--nuttcp_bandwidth_step_mb`: The amount of megabytes to increase bandwidth in
    each UDP stream test.
    (default: '1000')
    (an integer)

`--nuttcp_cpu_sample_time`: Time, in seconds, to take the CPU usage sample.
    (default: '3')
    (an integer)

`--nuttcp_max_bandwidth_mb`: The maximum bandwidth, in megabytes, to test in a
    UDP stream.
    (default: '10000')
    (an integer)

`--nuttcp_min_bandwidth_mb`: The minimum bandwidth, in megabytes, to test in a
    UDP stream.
    (default: '100')
    (an integer)

`--nuttcp_udp_iterations`: The number of consecutive tests to run.
    (default: '1')
    (an integer)

`--nuttcp_udp_packet_size`: The size of each UDP packet sent in the UDP stream.
    (default: '1420')
    (an integer)

`--[no]nuttcp_udp_run_both_directions`: Run the test twice, using each VM as a
    source.
    (default: 'false')

`--nuttcp_udp_stream_seconds`: The amount of time to run the UDP stream test.
    (default: '60')
    (an integer)

`--[no]nuttcp_udp_unlimited_bandwidth`: Run an "unlimited bandwidth" test
    (default: 'false')

### [perfkitbenchmarker.windows_packages.psping ](../perfkitbenchmarker/windows_packages/psping.py)

#### Description:

Module containing psping installation and cleanup functions.

psping is a tool made for benchmarking Windows networking.



#### Flags:

`--psping_bucket_count`: For the results histogram, number of columns
    (default: '100')
    (an integer)

`--psping_packet_size`: The size of the packet to test the ping with.
    (default: '1')
    (an integer)

`--psping_rr_count`: The number of pings to attempt
    (default: '1000')
    (an integer)

`--psping_timeout`: The time to allow psping to run
    (default: '10')
    (an integer)

### [perfkitbenchmarker.windows_virtual_machine ](../perfkitbenchmarker/windows_virtual_machine.py)

#### Description:

Module containing mixin classes for Windows virtual machines.

#### Flags:

`--[no]log_windows_password`: Whether to log passwords for Windows machines.
    This can be useful in the event of needing to manually RDP to the instance.
    (default: 'false')

`--[no]set_cpu_priority_high`: Allows executables to be set to High (up from
    Normal) CPU priority through the SetProcessPriorityToHigh function.
    (default: 'false')

