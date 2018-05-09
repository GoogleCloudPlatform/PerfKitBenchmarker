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
    vms. If unset, this benchmark will use 2000000 * num_cpus on data nodes as
    the value.
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
    ent/com/google/cloud/bigtable/bigtable-hbase-1.1/0.9.0/bigtable-
    hbase-1.1-0.9.0.jar')

`--google_bigtable_instance_name`: Bigtable instance name.

`--google_bigtable_zone_name`: Bigtable zone.
    (default: 'us-central1-b')

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

`--redis_region`: The region to spin up cloud redis in
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

### [perfkitbenchmarker.linux_benchmarks.dacapo_benchmark ](../perfkitbenchmarker/linux_benchmarks/dacapo_benchmark.py)

#### Description:

Runs DaCapo benchmarks.

This benchmark runs the various DaCapo benchmarks. More information can be found
at: http://dacapobench.org/


#### Flags:

`--dacapo_benchmark`: <luindex|lusearch>: Name of specific DaCapo benchmark to
    execute.
    (default: 'luindex')

`--dacapo_jar_filename`: Filename of DaCapo jar file.
    (default: 'dacapo-9.12-bach.jar')

`--dacapo_num_iters`: Number of iterations to execute.
    (default: '1')
    (an integer)

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

`--edw_benchmark_scripts`: Comma separated list of scripts.
    (default: 'sample.sql')
    (a comma separated list)

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

`--fio_parameters`: Parameters to apply to all PKB generated fio jobs. Each
    member of the list should be of the form "param=value".
    (default: '')
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

### [perfkitbenchmarker.linux_benchmarks.hpcc_benchmark ](../perfkitbenchmarker/linux_benchmarks/hpcc_benchmark.py)

#### Description:

Runs HPC Challenge.

Homepage: http://icl.cs.utk.edu/hpcc/

Most of the configuration of the HPC-Challenge revolves around HPL, the rest of
the HPCC piggybacks upon the HPL configration.

Homepage: http://www.netlib.org/benchmark/hpl/

HPL requires a BLAS library (Basic Linear Algebra Subprograms)
OpenBlas: http://www.openblas.net/

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

`--hpcc_mpi_env`: Comma seperated list containing environment variables to use
    with mpirun command. e.g.
    MKL_DEBUG_CPU_TYPE=7,MKL_ENABLE_INSTRUCTIONS=AVX512
    (default: '')
    (a comma separated list)

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

`--[no]hpcg_run_as_root`: If true, pass --allow-run-as-root to mpirun.
    (default: 'false')

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

`--inception3_data_dir`: Directory where input data is stored
    (default: 'gs://cloud-tpu-test-datasets/fake_imagenet')

`--inception3_eval_batch_size`: Global (not per-shard) batch size for evaluation
    (default: '1024')
    (an integer)

`--inception3_iterations`: Number of iterations per TPU training loop.
    (default: '100')
    (an integer)

`--inception3_learning_rate`: Learning rate.
    (default: '0.165')
    (a number)

`--inception3_mode`: <train|eval|train_and_eval>: Mode to run: train, eval,
    train_and_eval
    (default: 'train')

`--inception3_model_dir`: Directory where model output is stored

`--inception3_save_checkpoints_secs`: Interval (in seconds) at which the model
    data should be checkpointed. Set to 0 to disable.
    (default: '0')
    (an integer)

`--inception3_train_batch_size`: Global (not per-shard) batch size for training
    (default: '1024')
    (an integer)

`--inception3_train_steps`: Number of steps use for training.
    (default: '250000')
    (an integer)

`--inception3_train_steps_per_eval`: Number of training steps to run between
    evaluations.
    (default: '2000')
    (an integer)

`--inception3_use_data`: <real|fake>: Whether to use real or fake data. If real,
    the data is downloaded from inception3_data_dir. Otherwise, synthetic data
    is generated.
    (default: 'real')

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

### [perfkitbenchmarker.linux_benchmarks.iperf_vpn_benchmark ](../perfkitbenchmarker/linux_benchmarks/iperf_vpn_benchmark.py)

#### Description:

Runs plain Iperf over vpn.

Docs:
http://iperf.fr/

Runs Iperf to collect network throughput.


#### Flags:

`--iperf_vpn_runtime_in_seconds`: Number of seconds to run iperf.
    (default: '60')
    (a positive integer)

`--iperf_vpn_sending_thread_count`: Number of connections to make to the server
    for sending traffic.
    (default: '1')
    (a positive integer)

`--iperf_vpn_timeout`: Number of seconds to wait in addition to iperf runtime
    before killing iperf client command.
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

### [perfkitbenchmarker.linux_benchmarks.mnist_benchmark ](../perfkitbenchmarker/linux_benchmarks/mnist_benchmark.py)

#### Description:

Run MNIST benchmarks.

#### Flags:

`--mnist_model_dir`: Estimator model directory

`--mnist_train_file`: mnist train file for tensorflow
    (default: 'gs://tfrc-test-bucket/mnist-records/train.tfrecords')

`--mnist_train_steps`: Total number of training steps
    (default: '2000')
    (an integer)

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

`--multichase_taskset_options`: If provided, taskset is used to limit the cores
    available to multichase. The value of this flag contains the options to
    provide to taskset. Examples: '0x00001FE5' or '-c 0,2,5-12'.

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

For Amazon's EMR service, if the the provided jar file has a main class, you
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

`--runspec_build_tool_version`: Version of gcc/g++/gfortran. This should match
    runspec_config. Note, if neither runspec_config and
    runspec_build_tool_version is set, the test install gcc/g++/gfortran-4.7,
    since that matches default config version. If runspec_config is set, but not
    runspec_build_tool_version, default version of build tools will be
    installed. Also this flag only works with debian.

`--runspec_config`: Used by the PKB speccpu2006 benchmark. Name of the cfg file
    to use as the SPEC CPU2006 config file provided to the runspec binary via
    its --config flag. If the benchmark is run using the cpu2006-1.2.iso file,
    then the cfg file must be placed in the local PKB data directory and will be
    copied to the remote machine prior to executing runspec. See README.md for
    instructions if running with a repackaged cpu2006v1.2.tgz file.
    (default: 'linux64-x64-gcc47.cfg')

`--runspec_define`: Used by the PKB speccpu2006 benchmark. Optional comma-
    separated list of SYMBOL[=VALUE] preprocessor macros provided to the runspec
    binary via repeated --define flags. Example: numa,smt,sse=SSE4.2
    (default: '')

`--[no]runspec_enable_32bit`: Used by the PKB speccpu2006 benchmark. If set,
    multilib packages will be installed on the remote machine to enable use of
    32-bit SPEC CPU2006 binaries. This may be useful when running on memory-
    constrained instance types (i.e. less than 2 GiB memory/core), where 64-bit
    execution may be problematic.
    (default: 'false')

`--[no]runspec_estimate_spec`: Used by the PKB speccpu2006 benchmark. If set,
    the benchmark will report an estimated aggregate score even if SPEC CPU2006
    did not compute one. This usually occurs when --runspec_iterations is less
    than 3.  --runspec_keep_partial_results is also required to be set. Samples
    will becreated as estimated_SPECint(R)_rate_base2006 and
    estimated_SPECfp(R)_rate_base2006.  Available results will be saved, and PKB
    samples will be marked with a metadata value of partial=true. If unset,
    SPECint(R)_rate_base2006 and SPECfp(R)_rate_base2006 are listed in the
    metadata under missing_results.
    (default: 'false')

`--runspec_iterations`: Used by the PKB speccpu2006 benchmark. The number of
    benchmark iterations to execute, provided to the runspec binary via its
    --iterations flag.
    (default: '3')
    (an integer)

`--[no]runspec_keep_partial_results`: Used by the PKB speccpu2006 benchmark. If
    set, the benchmark will report an aggregate score even if some of the SPEC
    CPU2006 component tests failed with status "NR". Available results will be
    saved, and PKB samples will be marked with a metadata value of partial=true.
    If unset, partial failures are treated as errors.
    (default: 'false')

`--runspec_metric`: <rate|speed>: SPEC test to run. Speed is time-based metric,
    rate is throughput-based metric.
    (default: 'rate')

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

`--specsfs2014_benchmark`: <VDI|DATABASE|SWBUILD|VDA>: The SPEC SFS 2014
    benchmark to run.
    (default: 'VDI')

`--specsfs2014_config`: This flag can be used to specify an alternate SPEC
    config file to use. If this option is specified, none of the other benchmark
    specific flags which operate on the config file will be used (since the
    default config file will be replaced by this one).

`--specsfs2014_incr_load`: The amount to increment "load" by for each run.
    (default: '1')
    (a positive integer)

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

`--sysbench_report_interval`: The interval, in seconds, we ask sysbench to
    report results.
    (default: '2')
    (an integer)

`--sysbench_run_seconds`: The duration of the actual run in which results are
    collected, in seconds.
    (default: '480')
    (an integer)

`--sysbench_table_size`: The number of rows of each table used in the oltp tests
    (default: '100000')
    (an integer)

`--sysbench_tables`: The number of tables used in sysbench oltp.lua tests
    (default: '4')
    (an integer)

`--sysbench_testname`: The built in oltp lua script to run
    (default: 'oltp_read_write')

`--sysbench_warmup_seconds`: The duration of the warmup run in which results are
    discarded, in seconds.
    (default: '120')
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

`--tf_data_format`: <NCHW|NHWC>: Data layout to
    use: NHWC (TF native) or NCHW (cuDNN native).
    (default: 'NCHW')

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

`--tf_precision`: <float16|float32>: Use 16-bit floats for certain tensors
    instead of 32-bit floats. This is currently experimental.
    (default: 'float32')

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
Tensorflow Serving is built from source, which takes
a significant amount of time (45 minutes on an n1-standard-8).
Note that both client and server VMs build the code.

Once the code is built, the server prepares an inception model
for serving. It prepares a pre-trained inception model using a publicly
available checkpoint file. This model has been trained to ~75% accuracy
on the imagenet 2012 dataset, so is relatively useless; however, measuring
the accuracy of the model is beyond the scope of this benchmark.
The server then starts the standard tensorflow_model_server binary using
the prepared model.

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

