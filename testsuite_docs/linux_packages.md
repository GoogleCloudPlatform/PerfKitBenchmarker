### [perfkitbenchmarker.linux_packages.aerospike_server ](perfkitbenchmarker/linux_packages/aerospike_server.py)

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

### [perfkitbenchmarker.linux_packages.azure_sdk ](perfkitbenchmarker/linux_packages/azure_sdk.py)

#### Description:

Package for installing the Azure SDK.

#### Flags:

`--azure_lib_version`: Use a particular version of azure client lib, e.g.: 1.0.2
    (default: '1.0.3')

### [perfkitbenchmarker.linux_packages.cassandra ](perfkitbenchmarker/linux_packages/cassandra.py)

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

### [perfkitbenchmarker.linux_packages.ch_block_storage ](perfkitbenchmarker/linux_packages/ch_block_storage.py)

#### Description:

Contains cloudharmony block storage benchmark installation functions.

#### Flags:

`--ch_params`: A list of comma seperated "key=value" parameters passed into
    cloud harmony benchmarks.
    (default: '')
    (a comma separated list)

### [perfkitbenchmarker.linux_packages.cloud_tpu_models ](perfkitbenchmarker/linux_packages/cloud_tpu_models.py)

#### Description:

Module containing cloud TPU models installation and cleanup functions.

#### Flags:

`--cloud_tpu_commit_hash`: git commit hash of desired cloud TPU models commit.
    (default: 'c44f52634e007694e7ccad1cffdf63f05b90c80e')

### [perfkitbenchmarker.linux_packages.cuda_toolkit ](perfkitbenchmarker/linux_packages/cuda_toolkit.py)

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

`--cuda_toolkit_version`: <8.0|9.0>: Version of CUDA Toolkit to install
    (default: '9.0')

`--[no]gpu_autoboost_enabled`: whether gpu autoboost is enabled

### [perfkitbenchmarker.linux_packages.cudnn ](perfkitbenchmarker/linux_packages/cudnn.py)

#### Description:

Module containing CUDA Deep Neural Network library installation functions.

#### Flags:

`--cudnn`: The NVIDIA CUDA Deep Neural Network library. Please put in data
    directory and specify the name
    (default: 'libcudnn7_7.0.5.15-1+cuda9.0_amd64.deb')

### [perfkitbenchmarker.linux_packages.gluster ](perfkitbenchmarker/linux_packages/gluster.py)

#### Description:

Module containing GlusterFS installation and cleanup functions.

#### Flags:

`--gluster_replicas`: The number of Gluster replicas.
    (default: '3')
    (an integer)

`--gluster_stripes`: The number of Gluster stripes.
    (default: '1')
    (an integer)

### [perfkitbenchmarker.linux_packages.memcached_server ](perfkitbenchmarker/linux_packages/memcached_server.py)

#### Description:

Module containing memcached server installation and cleanup functions.

#### Flags:

`--memcached_size_mb`: Size of memcached cache in megabytes.
    (default: '64')
    (an integer)

### [perfkitbenchmarker.linux_packages.mxnet_cnn ](perfkitbenchmarker/linux_packages/mxnet_cnn.py)

#### Description:

Module containing MXNet CNN installation and cleanup functions.

#### Flags:

`--mxnet_commit_hash`: git commit hash of desired mxnet commit.
    (default: '2700ddbbeef212879802f7f0c0812192ec5c2b77')

### [perfkitbenchmarker.linux_packages.netperf ](perfkitbenchmarker/linux_packages/netperf.py)

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

### [perfkitbenchmarker.linux_packages.openjdk ](perfkitbenchmarker/linux_packages/openjdk.py)

#### Description:

Module containing OpenJDK installation and cleanup functions.

#### Flags:

`--openjdk_version`: Version of openjdk to use. By default, the version of
    openjdk is automatically detected.

### [perfkitbenchmarker.linux_packages.redis_server ](perfkitbenchmarker/linux_packages/redis_server.py)

#### Description:

Module containing redis installation and cleanup functions.

#### Flags:

`--[no]redis_enable_aof`: Enable append-only file (AOF) with appendfsync always.
    (default: 'false')

`--redis_total_num_processes`: Total number of redis server processes.
    (default: '1')
    (a positive integer)

### [perfkitbenchmarker.linux_packages.tensorflow ](perfkitbenchmarker/linux_packages/tensorflow.py)

#### Description:

Module containing TensorFlow installation and cleanup functions.

#### Flags:

`--tf_benchmarks_commit_hash`: git commit hash of desired tensorflow benchmark
    commit.
    (default: 'bab8a61aaca3d2b94072ae2b87f0aafe1797b165')

`--tf_cpu_pip_package`: TensorFlow CPU pip package to install. By default, PKB
    will install an Intel-optimized CPU build when using CPUs.
    (default:
    'https://anaconda.org/intel/tensorflow/1.4.0/download/tensorflow-1.4.0-cp27
    -cp27mu-linux_x86_64.whl')

`--tf_gpu_pip_package`: TensorFlow GPU pip package to install. By default, PKB
    will install tensorflow-gpu==1.7 when using GPUs.
    (default: 'tensorflow-gpu==1.7')

### [perfkitbenchmarker.linux_packages.tomcat ](perfkitbenchmarker/linux_packages/tomcat.py)

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

### [perfkitbenchmarker.linux_packages.ycsb ](perfkitbenchmarker/linux_packages/ycsb.py)

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

`--ycsb_measurement_type`: <histogram|hdrhistogram|timeseries>: Measurement type
    to use for ycsb. Defaults to histogram.
    (default: 'histogram')

`--ycsb_operation_count`: Number of operations *per client VM*.
    (default: '1000000')
    (an integer)

`--ycsb_preload_threads`: Number of threads per loader during the initial data
    population stage. Default value depends on the target DB.
    (an integer)

`--ycsb_record_count`: Pre-load with a total dataset of records total. Overrides
    recordcount value in all workloads of this run. Defaults to None, where
    recordcount value in each workload is used. If neither is not set, ycsb
    default of 0 is used.
    (an integer)

`--[no]ycsb_reload_database`: Reload database, othewise skip load stage. Note,
    this flag is only used if the database is already loaded.
    (default: 'true')

`--ycsb_run_parameters`: Passed to YCSB during the load stage. Comma-separated
    list of "key=value" pairs.
    (default: '')
    (a comma separated list)

`--ycsb_threads_per_client`: Number of threads per loader during the benchmark
    run. Specify a list to vary the number of clients.
    (default: '32')
    (a comma separated list)

`--ycsb_timelimit`: Maximum amount of time to run each workload / client count
    combination. Set to 0 for unlimited time.
    (default: '1800')
    (an integer)

`--ycsb_version`: YCSB version to use. Defaults to version 0.9.0.
    (default: '0.9.0')

`--ycsb_workload_files`: Path to YCSB workload file to use during *run* stage
    only. Comma-separated list
    (default: 'workloada,workloadb')
    (a comma separated list)

