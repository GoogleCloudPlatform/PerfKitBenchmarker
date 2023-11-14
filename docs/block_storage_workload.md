# Block Storage Workload Test

## How the test works
The block_storage_workload benchmark in the perfkitbenchmarker repository is a tool designed to evaluate the performance of block storage devices or volumes. It generates a workload that simulates real-world block storage usage patterns, including read and write operations. The benchmark measures performance metrics such as throughput, latency, and IOPS (Input/Output Operations Per Second) to assess the performance of the block storage system. It allows customization of parameters such as block size, number of threads, and duration of the benchmark run. 

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=block_storage_workload```  

perfkitbenchmarker.linux_benchmarks.block_storage_workloads_benchmark:
```
  --iodepth_list: A list of iodepth parameter used by fio command in simulated database and streaming scenarios only.
    (default: '')
    (a comma separated list)
  --maxjobs: The maximum allowed number of jobs to support.
    (default: '0')
    (an integer)
  --workload_mode: <logging|database|streaming>: Simulate a logging, database or streaming scenario.
    (default: 'logging')
```
### See example configuration here: 


## Metrics captured

sequential_write:write:iops (#):  
The number of sequential write Input/Output Operations Per Second (IOPS) achieved during the benchmark.

random_read:read:iops (#):  
The number of random read IOPS achieved during the benchmark.

sequential_read:read:iops (#):  
The number of sequential read IOPS achieved during the benchmark.

sequential_write:write:bandwidth (KB/s):  
The sequential write bandwidth (data transfer rate) achieved during the benchmark, measured in kilobytes per second.

random_read:read:bandwidth (KB/s):  
The random read bandwidth achieved during the benchmark, measured in kilobytes per second.

sequential_read:read:bandwidth (KB/s):  
The sequential read bandwidth achieved during the benchmark, measured in kilobytes per second.

sequential_write:write:latency (usec):  
The average latency (time delay) for sequential write operations.
random_read:read:latency (usec):  
The average latency for random read operations.

sequential_read:read:latency (usec):  
The average latency for sequential read operations.

End to end Runtime (seconds): 
The total runtime of the test from initiation to (teardown complete?)

