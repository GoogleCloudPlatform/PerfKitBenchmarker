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

`--tcp_number_of_streams`: The number of parrallel streams to run in the TCP
    test
    (default: '10')
    (an integer)

`--tcp_stream_seconds`: The amount of time to run the TCP stream test.
    (default: '3')
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

`--ntttcp_threads`: The number of client and server threads for NTttcp to run
    with.
    (default: '1')
    (an integer)

`--ntttcp_time`: The number of seconds for NTttcp to run.
    (default: '60')
    (an integer)

### [perfkitbenchmarker.windows_packages.nuttcp ](../perfkitbenchmarker/windows_packages/nuttcp.py)

#### Description:

Module containing nuttcp installation and cleanup functions.

#### Flags:

`--nuttcp_bandwidth_step_mb`: The amount of megabytes to increase bandwidth in
    each UDP stream test.
    (default: '1000')
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
    (default: '10')
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

