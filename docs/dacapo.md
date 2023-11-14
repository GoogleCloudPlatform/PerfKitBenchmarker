# Dacapo Test

## How the test works
The dacapo_benchmark in the perfkitbenchmarker repository is a benchmark specifically designed for evaluating the performance of Java Virtual Machines (JVMs) using the DaCapo benchmark suite. The DaCapo benchmark suite consists of a set of real-world Java applications that stress different aspects of JVM performance, such as CPU, memory, and I/O. The dacapo_benchmark executes these benchmark applications and measures metrics such as execution time, throughput, and latency.

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=coremark```  

perfkitbenchmarker.linux_benchmarks.coremark_benchmark:
```
  --coremark_parallelism_method: <PTHREAD|FORK|SOCKET>: Method to use for parallelism in the Coremark benchmark.
    (default: 'PTHREAD')
```
### See example configuration here: 


## Metrics captured
run_time (ms): The total execution time of the benchmark, measured in milliseconds. It represents the time taken to run the entire workload of the benchmark and provides an indication of the overall performance and efficiency of the system.

End to end Runtime (seconds): 
The total runtime of the test from initiation to (teardown complete?)
