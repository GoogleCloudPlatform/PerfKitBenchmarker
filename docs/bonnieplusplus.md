# Bonnie Plus Plus Test

## How the test works
The bonnieplusplus benchmark in the perfkitbenchmarker repository is a tool used to assess the performance and capabilities of file systems and storage devices. It performs a series of tests to measure metrics such as sequential and random file I/O performance, file creation and deletion speed, as well as metadata operations. The benchmark generates synthetic workloads that simulate real-world file system usage scenarios. By executing the bonnieplusplus benchmark, it is possible to evaluate the read and write throughput, latency, and scalability of file systems and storage devices.

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=bonnieplusplus```  

perfkitbenchmarker.linux_benchmarks.bonnieplusplus:
```
  N/A
```
### See example configuration here: 


## Metrics captured
put_block_cpu (%s):  
CPU utilization percentage during the put_block operation.

rewrite_cpu (%s):  
CPU utilization percentage during the rewrite operation.

get_block_cpu (%s):  
CPU utilization percentage during the get_block operation.

seeks_cpu (%s):  
CPU utilization percentage during the seeks operation.

seq_create_cpu (%s):  
CPU utilization percentage during the sequential create operation.

seq_del_cpu (%s):  
CPU utilization percentage during the sequential delete operation.

ran_create_cpu (%s):  
CPU utilization percentage during the random create operation.

ran_del_cpu (%s):  
 utilization percentage during the random delete operation.

put_block_latency (ms):  
Latency (response time) for put_block operation, measured in milliseconds.

rewrite_latency (ms):  
Latency for rewrite operation, measured in milliseconds.

get_block_latency (ms):  
Latency for get_block operation, measured in milliseconds.

seeks_latency (ms):  
 for seeks operation, measured in milliseconds.

seq_create_latency (us):  
Latency for sequential create operation, measured in microseconds.

seq_stat_latency (us):  
Latency for sequential stat operation, measured in microseconds.

seq_del_latency (us):  
Latency for sequential delete operation, measured in microseconds.

ran_create_latency (us):  
Latency for random create operation, measured in microseconds.

ran_stat_latency (us):  
Latency for random stat operation, measured in microseconds.

ran_del_latency (us):  
Latency for random delete operation, measured in microseconds.

put_block (K/sec):  
Throughput (data transfer rate) for put_block operation, measured in kilobytes per second.

rewrite (K/sec):  
Throughput for rewrite operation, measured in kilobytes per second.

get_block (K/sec):  
Throughput for get_block operation, measured in kilobytes per second.

seeks (K/sec):  
Throughput for seeks operation, measured in kilobytes per second.

seq_create (K/sec):  
Throughput for sequential create operation, measured in kilobytes per second.

seq_del (K/sec):  
Throughput for sequential delete operation, measured in kilobytes per second.

ran_create (K/sec):  
Throughput for random create operation, measured in kilobytes per second.

ran_del (K/sec):  
Throughput for random delete operation, measured in kilobytes per second.

End to end Runtime (seconds): 
The total runtime of the test from initiation to (teardown complete?)
