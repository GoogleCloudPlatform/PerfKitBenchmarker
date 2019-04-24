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

