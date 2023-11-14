# IPerf Test

## How the test works
The iperf test is a linux based test that uses the linux iperf library (Link to iperf lib) to measure network throughput performance between two devices.  This test creates two devices sized according to the test confiuration. By default these devices (device A and device B) are created in the same availablilty zone, the throughput test is run from A to B and then from B to A for both their public and private IP addresses, capturing 4 total network throughput measurements.

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=iperf```  

perfkitbenchmarker.linux_benchmarks.iperf_benchmark:  
```
  --iperf_benchmarks: Run TCP, UDP or both
    (default: 'TCP')
    (a comma separated list)
  --iperf_buffer_length: set read/write buffer size (TCP) or length (UDP) to n[kmKM]Bytes.1kB= 10^3, 1mB= 10^6, 1KB=2^10, 1MB=2^20
  --iperf_interval: The number of seconds between periodic bandwidth reports. Currently only for TCP tests
    (a number)
  --iperf_runtime_in_seconds: Number of seconds to run iperf.
    (default: '60')
    (a positive integer)
  --iperf_sending_thread_count: server for sending traffic. Iperfwill run once for each value in the list
    (default: '1')
    (A comma-separated list of integers or integer ranges. Ex: -1,3,5:7 is read as -1,3,5,6,7.)
  --iperf_sleep_time: number of seconds to sleep after each iperf test
    (default: '5')
    (an integer)
  --iperf_tcp_per_stream_bandwidth: In Mbits. Iperf will attempt to send at this bandwidth for TCP tests. If using multiple streams, each stream will attempt to send
    at this bandwidth
    (a number)
  --iperf_timeout: Number of seconds to wait in addition to iperf runtime before killing iperf client command.
    (a positive integer)
  --iperf_udp_per_stream_bandwidth: In Mbits. Iperf will attempt to send at this bandwidth for UDP tests. If using multiple streams, each stream will attempt to send
    at this bandwidth
    (a number)
```
### See example configuration here: 


## Metrics captured
Throughput (Mbits/sec): Data transfer speed from one point to another over the virtual cloud network. The details are configurable, but the default option measures the average throughput over a 60 second run of the test for each run.

End to end Runtime: 
The total runtime of the test from initiation to (teardown complete?)

