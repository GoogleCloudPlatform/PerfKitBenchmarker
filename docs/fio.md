# Fio Test

## How the test works
The fio benchmark in the perfkitbenchmarker repository is a tool used to evaluate the performance of storage devices and systems by simulating various types of I/O workloads. It allows for precise control over the I/O patterns, including sequential and random reads/writes, as well as different block sizes and access patterns. The benchmark measures performance metrics such as throughput, latency, and IOPS (Input/Output Operations Per Second) to assess the storage system's capabilities.

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=fio```  

perfkitbenchmarker.linux_benchmarks.fio_benchmark:
```
  --[no]fio_bw_log: Whether to collect a bandwidth log of the fio jobs.
    (default: 'false')
  --fio_command_timeout_sec: Timeout for fio commands in seconds.
    (an integer)
  --[no]fio_direct: Whether to use O_DIRECT to bypass OS cache. This is strongly recommended, but not supported by all files.
    (default: 'true')
  --fio_fill_size: The amount of device to fill in prepare stage. The valid value can either be an integer, which represents
    the number of bytes to fill or a percentage, which represents the percentage of the device. A filesystem will be
    unmounted before filling and remounted afterwards. Only valid when --fio_target_mode is against_device_with_fill or
    against_file_with_fill.
    (default: '100%')
  --fio_generate_scenarios: Generate a job file with the given scenarios. Special scenario 'all' generates all scenarios.
    Available scenarios are sequential_write, sequential_read, random_write, and random_read. Cannot use with --fio_jobfile.
    You can also specify a scenario in the format accesspattern_blocksize_operation_workingset for a custom workload.
    (default: '')
    (a comma separated list)
  --[no]fio_hist_log: Whether to collect clat histogram.
    (default: 'false')
  --fio_io_depths: IO queue depths to run on. Can specify a single number, like --fio_io_depths=1, a range, like
    --fio_io_depths=1-4, or a list, like --fio_io_depths=1-4,6-8
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read as -1,3,5,6,7.)
  --fio_ioengine: <libaio|windowsaio>: Defines how the job issues I/O to the file
    (default: 'libaio')
  --[no]fio_iops_log: Whether to collect an IOPS log of the fio jobs.
    (default: 'false')
  --fio_jobfile: Job file that fio will use. If not given, use a job file bundled with PKB. Cannot use with
    --fio_generate_scenarios.
  --[no]fio_lat_log: Whether to collect a latency log of the fio jobs.
    (default: 'false')
  --fio_log_avg_msec: By default, this will average each log entry in the fio latency, bandwidth, and iops logs over the
    specified period of time in milliseconds. If set to 0, fio will log an entry for every IO that completes, this can grow
    very quickly in size and can cause performance overhead.
    (default: '1000')
    (a non-negative integer)
  --fio_log_hist_msec: Same as fio_log_avg_msec, but logs entries for completion latency histograms. If set to 0, histogram
    logging is disabled.
    (default: '1000')
    (an integer)
  --fio_num_jobs: Number of concurrent fio jobs to run.
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read as -1,3,5,6,7.)
  --fio_parameters: Parameters to apply to all PKB generated fio jobs. Each member of the list should be of the form
    "param=value".
    (default: 'randrepeat=0')
    (a comma separated list)
  --fio_rng: <tausworthe|lfsr|tausworthe64>: Which RNG to use for 4k Random IOPS.
    (default: 'tausworthe64')
  --fio_runtime: The number of seconds to run each fio job for.
    (default: '600')
    (a positive integer)
  --fio_target_mode: <against_device_with_fill|against_device_without_fill|against_file_with_fill|against_file_without_fill>:
    Whether to run against a raw device or a file, and whether to prefill.
    (default: 'against_file_without_fill')
  --[no]fio_use_default_scenarios: Use the legacy scenario tables defined in fio_benchmark.py to resolve the scenario name in
    generate scenarios
    (default: 'true')
  --fio_working_set_size: The size of the working set, in GB. If not given, use the full size of the device. If using
    --fio_generate_scenarios and not running against a raw device, you must pass --fio_working_set_size.
    (a non-negative integer)
```
### See example configuration here: 


## Metrics captured
sequential_write:write:iops (#):  
The number of sequential write I/O operations per second.

sequential_read:read:iops (#):  
The number of sequential read I/O operations per second.

random_write_test:write:iops (#):  
The number of random write I/O operations per second.

random_read_test:read:iops (#):  
The number of random read I/O operations per second.

random_read_test_parallel:read:iops (#):  
The number of parallel random read I/O operations per second.

sequential_write:write:bandwidth (KB/s):  
The sequential write bandwidth measured in kilobytes per second.

sequential_read:read:bandwidth (KB/s):  
The sequential read bandwidth measured in kilobytes per second.

random_write_test:write:bandwidth (KB/s):  
The random write bandwidth measured in kilobytes per second.

random_read_test:read:bandwidth (KB/s):  
The random read bandwidth measured in kilobytes per second.

random_read_test_parallel:read:bandwidth (KB/s):  
The parallel random read bandwidth measured in kilobytes per second.

sequential_write:write:latency (usec):  
The latency of sequential write operations measured in microseconds.

sequential_read:read:latency (usec):  
The latency of sequential read operations measured in microseconds.

random_write_test:write:latency (usec):  
The latency of random write operations measured in microseconds.

random_read_test:read:latency (usec):  
The latency of random read operations measured in microseconds.

random_read_test_parallel:read:latency (usec):  
The latency of parallel random read operations measured in microseconds.

End to end Runtime (seconds): 
The total runtime of the test from initiation to (teardown complete?)
