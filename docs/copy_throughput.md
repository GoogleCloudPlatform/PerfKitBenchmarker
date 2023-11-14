# Copy Throughput Test

## How the test works
The copy_throughput benchmark in the perfkitbenchmarker repository is a tool used to measure the throughput performance of data copying operations. It evaluates the efficiency of copying data from one storage location to another within the same system or across a network. The benchmark measures the time taken to copy a specified amount of data and calculates the throughput in bytes per second. 

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=copy_throughput```  

perfkitbenchmarker.linux_benchmarks.copy_throughput_benchmark:
```
  --copy_benchmark_mode: <cp|dd|scp>: Runs either cp, dd or scp tests.
    (default: 'cp')
  --copy_benchmark_single_file_mb: If set, a single file of the specified number of MB is used instead of the normal cloud-
    storage-workload.sh basket of files.  Not supported when copy_benchmark_mode is dd
    (an integer)
```
### See example configuration here: 


## Metrics captured
cp throughput (MB/sec): The data transfer rate achieved during the copy operation, measured in megabytes per second. It indicates how quickly data is copied from the source to the destination.

End to end Runtime (seconds): 
The total runtime of the test from initiation to (teardown complete?)
