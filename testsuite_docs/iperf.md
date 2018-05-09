
perfkitbenchmarker.linux_benchmarks.iperf_benchmark:
  --iperf_runtime_in_seconds: Number of seconds to run iperf.
    (default: '60')
    (a positive integer)
  --iperf_sending_thread_count: Number of connections to make to the server for
    sending traffic.
    (default: '1')
    (a positive integer)
  --iperf_timeout: Number of seconds to wait in addition to iperf runtime before
    killing iperf client command.
    (a positive integer)

perfkitbenchmarker.linux_benchmarks.iperf_vpn_benchmark:
  --iperf_vpn_runtime_in_seconds: Number of seconds to run iperf.
    (default: '60')
    (a positive integer)
  --iperf_vpn_sending_thread_count: Number of connections to make to the server
    for sending traffic.
    (default: '1')
    (a positive integer)
  --iperf_vpn_timeout: Number of seconds to wait in addition to iperf runtime
    before killing iperf client command.
    (a positive integer)

perfkitbenchmarker.windows_packages.iperf3:
  --bandwidth_step_mb: The amount of megabytes to increase bandwidth in each UDP
    stream test.
    (default: '100')
    (an integer)
  --max_bandwidth_mb: The maximum bandwidth, in megabytes, to test in a UDP
    stream.
    (default: '500')
    (an integer)
  --min_bandwidth_mb: The minimum bandwidth, in megabytes, to test in a UDP
    stream.
    (default: '100')
    (an integer)
  --[no]run_tcp: setting to false will disable the run of the TCP test
    (default: 'true')
  --[no]run_udp: setting to true will enable the run of the UDP test
    (default: 'false')
  --tcp_number_of_streams: The number of parrallel streams to run in the TCP
    test
    (default: '10')
    (an integer)
  --tcp_stream_seconds: The amount of time to run the TCP stream test.
    (default: '3')
    (an integer)
  --udp_stream_seconds: The amount of time to run the UDP stream test.
    (default: '3')
    (an integer)
