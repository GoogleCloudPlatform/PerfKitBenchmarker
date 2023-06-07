# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Runs plain netperf in a few modes.

docs:
https://hewlettpackard.github.io/netperf/doc/netperf.html
manpage: http://manpages.ubuntu.com/manpages/maverick/man1/netperf.1.html

Runs TCP_RR, TCP_CRR, and TCP_STREAM benchmarks from netperf across two
machines.
"""

import collections
import csv
import json
import logging
import os
import re
import time
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import netperf
import six
from six.moves import zip

flags.DEFINE_integer(
    'netperf_max_iter',
    None, 'Maximum number of iterations to run during '
    'confidence interval estimation. If unset, '
    'a single iteration will be run.',
    lower_bound=3,
    upper_bound=30)

flags.DEFINE_integer(
    'netperf_test_length', 60, 'netperf test length, in seconds', lower_bound=1)
flags.DEFINE_bool(
    'netperf_enable_histograms', True,
    'Determines whether latency histograms are '
    'collected/reported. Only for *RR benchmarks')
flag_util.DEFINE_integerlist(
    'netperf_num_streams',
    flag_util.IntegerList([1]), 'Number of netperf processes to run. Netperf '
    'will run once for each value in the list.',
    module_name=__name__)
flags.DEFINE_integer('netperf_thinktime', 0,
                     'Time in nanoseconds to do work for each request.')
flags.DEFINE_integer('netperf_thinktime_array_size', 0,
                     'The size of the array to traverse for thinktime.')
flags.DEFINE_integer(
    'netperf_thinktime_run_length', 0,
    'The number of contiguous numbers to sum at a time in the '
    'thinktime array.')
flags.DEFINE_integer(
    'netperf_udp_stream_send_size_in_bytes',
    1024,
    'Send size to use for UDP_STREAM tests (netperf -m flag)',
    lower_bound=1,
    upper_bound=65507)
# We set the default to 128KB (131072 bytes) to override the Linux default
# of 16K so that we can achieve the "link rate".
flags.DEFINE_integer('netperf_tcp_stream_send_size_in_bytes', 131072,
                     'Send size to use for TCP_STREAM tests (netperf -m flag)')
flags.DEFINE_integer(
    'netperf_mss', None,
    'Sets the Maximum Segment Size (in bytes) for netperf TCP tests to use. '
    'The effective MSS will be slightly smaller than the value specified here. '
    'If you try to set an MSS higher than the current MTU, '
    'the MSS will be set to the highest possible value for that MTU. '
    'If you try to set the MSS lower than 88 bytes, the default MSS will be '
    'used.')

TCP_RR = 'TCP_RR'
TCP_CRR = 'TCP_CRR'
TCP_STREAM = 'TCP_STREAM'
UDP_RR = 'UDP_RR'
UDP_STREAM = 'UDP_STREAM'

TCP_BENCHMARKS = [TCP_RR, TCP_CRR, TCP_STREAM]
ALL_BENCHMARKS = TCP_BENCHMARKS + [UDP_RR, UDP_STREAM]

flags.DEFINE_list('netperf_benchmarks', TCP_BENCHMARKS,
                  'The netperf benchmark(s) to run.')
flags.register_validator(
    'netperf_benchmarks',
    lambda benchmarks: benchmarks and set(benchmarks).issubset(ALL_BENCHMARKS))

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'netperf'
BENCHMARK_CONFIG = """
netperf:
  description: Run one or more of TCP_RR, TCP_CRR, UDP_RR, TCP_STREAM and
               UDP_STREAM
  vpc_peering: True
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
  flags:
    placement_group_style: closest_supported
"""

MBPS = 'Mbits/sec'
TRANSACTIONS_PER_SECOND = 'transactions_per_second'
# Specifies the keys and to include in the results for OMNI tests.
# Any user of ParseNetperfOutput() (e.g. container_netperf_benchmark), must
# specify these selectors to ensure the parsing doesn't break.
OUTPUT_SELECTOR = ('THROUGHPUT,THROUGHPUT_UNITS,P50_LATENCY,P90_LATENCY,'
                   'P99_LATENCY,STDDEV_LATENCY,MIN_LATENCY,MAX_LATENCY,'
                   'CONFIDENCE_ITERATION,THROUGHPUT_CONFID,'
                   'LOCAL_TRANSPORT_RETRANS,REMOTE_TRANSPORT_RETRANS,'
                   'TRANSPORT_MSS')

# Command ports are even (id*2), data ports are odd (id*2 + 1)
PORT_START = 20000

REMOTE_SCRIPTS_DIR = 'netperf_test_scripts'
REMOTE_SCRIPT = 'netperf_test.py'

PERCENTILES = [50, 90, 99]

# By default, Container-Optimized OS (COS) host firewall allows only
# outgoing connections and incoming SSH connections. To allow incoming
# connections from VMs running netperf, we need to add iptables rules
# on the VM running netserver.
_COS_RE = re.compile(r'\b(cos|gci)-')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def PrepareNetperf(vm):
  """Installs netperf on a single vm."""
  vm.Install('netperf')


def Prepare(benchmark_spec):
  """Extracts the first two VM's from benchmark_spec and prepares the VMs for executing netperf.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  client_vm, server_vm = benchmark_spec.vms[:2]
  background_tasks.RunThreaded(PrepareNetperf, [client_vm, server_vm])
  background_tasks.RunParallelThreads(
      [
          (PrepareClientVM, [client_vm], {}),
          (
              PrepareServerVM,
              [server_vm, client_vm.internal_ip, client_vm.ip_address],
              {},
          ),
      ],
      2,
  )


def PrepareClientVM(client_vm):
  """Install netperf and copy remote test script to client_vm.

  Args:
    client_vm: The VM that runs the netperf binary.
  """
  # Copy remote test script to client
  path = data.ResourcePath(os.path.join(REMOTE_SCRIPTS_DIR, REMOTE_SCRIPT))
  logging.info('Uploading %s to %s', path, client_vm)
  client_vm.PushFile(path, REMOTE_SCRIPT)
  client_vm.RemoteCommand(f'sudo chmod 755 {REMOTE_SCRIPT}')


def PrepareServerVM(server_vm, client_vm_internal_ip, client_vm_ip_address):
  """Install netperf and start netserver processes on server_vm.

  Args:
    server_vm: The VM that runs the netserver binary.
    client_vm_internal_ip: Internal IP address of client_vm.
    client_vm_ip_address: All IP addresses of client_vm.
  """
  num_streams = max(FLAGS.netperf_num_streams) * max(
      1, len(server_vm.GetInternalIPs())
  )

  # See comments where _COS_RE is defined.
  if server_vm.image and re.search(_COS_RE, server_vm.image):
    _SetupHostFirewall(server_vm, client_vm_internal_ip, client_vm_ip_address)

  # Start the netserver processes
  if vm_util.ShouldRunOnExternalIpAddress():
    # Open all of the command and data ports
    server_vm.AllowPort(PORT_START, PORT_START + num_streams * 2 - 1)

  port_end = PORT_START + num_streams * 2 - 1
  netserver_cmd = (f'for i in $(seq {PORT_START} 2 {port_end}); do '
                   f'{netperf.NETSERVER_PATH} -p $i & done')
  server_vm.RemoteCommand(netserver_cmd)


def _SetupHostFirewall(server_vm, client_vm_internal_ip, client_vm_ip_address):
  """Set up host firewall to allow incoming traffic.

  Args:
    server_vm: The VM that runs the netserver binary.
    client_vm_internal_ip: Internal IP address of client_vm.
    client_vm_ip_address: All IP addresses of client_vm.
  """
  ip_addrs = [client_vm_internal_ip]
  if vm_util.ShouldRunOnExternalIpAddress():
    ip_addrs.append(client_vm_ip_address)

  logging.info('setting up host firewall on %s running %s for client at %s',
               server_vm.name, server_vm.image, ip_addrs)
  cmd = 'sudo iptables -A INPUT -p %s -s %s -j ACCEPT'
  for protocol in 'tcp', 'udp':
    for ip_addr in ip_addrs:
      server_vm.RemoteHostCommand(cmd % (protocol, ip_addr))


def _HistogramStatsCalculator(histogram, percentiles=PERCENTILES):
  """Computes values at percentiles in a distribution as well as stddev.

  Args:
    histogram: A dict mapping values to the number of samples with that value.
    percentiles: An array of percentiles to calculate.

  Returns:
    A dict mapping stat names to their values.
  """
  stats = {}

  # Histogram data in list form sorted by key
  by_value = sorted([(value, count) for value, count in histogram.items()],
                    key=lambda x: x[0])
  total_count = sum(histogram.values())

  cur_value_index = 0  # Current index in by_value
  cur_index = 0  # Number of values we've passed so far
  for p in percentiles:
    index = int(float(total_count) * float(p) / 100.0)
    index = min(index, total_count - 1)  # Handle 100th percentile
    for value, count in by_value[cur_value_index:]:
      if cur_index + count > index:
        stats['p%s' % str(p)] = by_value[cur_value_index][0]
        break
      else:
        cur_index += count
        cur_value_index += 1

  # Compute stddev
  value_sum = float(sum([value * count for value, count in histogram.items()]))
  average = value_sum / float(total_count)
  if total_count > 1:
    total_of_squares = sum([
        (value - average)**2 * count for value, count in histogram.items()
    ])
    stats['stddev'] = (total_of_squares / (total_count - 1))**0.5
  else:
    stats['stddev'] = 0
  return stats


def ParseNetperfOutput(stdout, metadata, benchmark_name,
                       enable_latency_histograms):
  """Parses the stdout of a single netperf process.

  Args:
    stdout: the stdout of the netperf process
    metadata: metadata for any sample.Sample objects we create
    benchmark_name: the name of the netperf benchmark
    enable_latency_histograms: bool indicating if latency histograms are
      included in stdout

  Returns:
    A tuple containing (throughput_sample, latency_samples, latency_histogram)
  """
  # Don't modify the metadata dict that was passed in
  metadata = metadata.copy()

  # Extract stats from stdout
  # Sample output:
  #
  # "MIGRATED TCP REQUEST/RESPONSE TEST from 0.0.0.0 (0.0.0.0) port 20001
  # AF_INET to 104.154.50.86 () port 20001 AF_INET : +/-2.500% @ 99% conf.
  # : first burst 0",\n
  # Throughput,Throughput Units,Throughput Confidence Width (%),
  # Confidence Iterations Run,Stddev Latency Microseconds,
  # 50th Percentile Latency Microseconds,90th Percentile Latency Microseconds,
  # 99th Percentile Latency Microseconds,Minimum Latency Microseconds,
  # Maximum Latency Microseconds\n
  # 1405.50,Trans/s,2.522,4,783.80,683,735,841,600,900\n
  try:
    fp = six.StringIO(stdout)
    # "-o" flag above specifies CSV output, but there is one extra header line:
    banner = next(fp)
    assert banner.startswith('MIGRATED'), stdout
    r = csv.DictReader(fp)
    results = next(r)
    logging.info('Netperf Results: %s', results)
    assert 'Throughput' in results
  except (StopIteration, AssertionError):
    # The output returned by netperf was unparseable - usually due to a broken
    # connection or other error.  Raise KnownIntermittentError to signal the
    # benchmark can be retried.  Do not automatically retry as an immediate
    # retry on these VMs may be adveresly affected (e.g. burstable credits
    # partially used)
    message = 'Netperf ERROR: Failed to parse stdout. STDOUT: %s' % stdout
    logging.error(message)
    raise errors.Benchmarks.KnownIntermittentError(message)

  # Update the metadata with some additional infos
  meta_keys = [('Confidence Iterations Run', 'confidence_iter'),
               ('Throughput Confidence Width (%)', 'confidence_width_percent')]
  if 'TCP' in benchmark_name:
    meta_keys.extend([
        ('Local Transport Retransmissions', 'netperf_retransmissions'),
        ('Remote Transport Retransmissions', 'netserver_retransmissions'),
        ('Transport MSS bytes', 'netperf_mss')
    ])

  metadata.update(
      {meta_key: results[netperf_key] for netperf_key, meta_key in meta_keys})

  # Create the throughput sample
  throughput = float(results['Throughput'])
  throughput_units = results['Throughput Units']
  if throughput_units == '10^6bits/s':
    # TCP_STREAM benchmark
    unit = MBPS
    metric = '%s_Throughput' % benchmark_name
  elif throughput_units == 'Trans/s':
    # *RR benchmarks
    unit = TRANSACTIONS_PER_SECOND
    metric = '%s_Transaction_Rate' % benchmark_name
  else:
    raise ValueError(
        'Netperf output specifies unrecognized throughput units %s' %
        throughput_units)
  throughput_sample = sample.Sample(metric, throughput, unit, metadata)

  latency_hist = None
  latency_samples = []
  if enable_latency_histograms:
    # Parse the latency histogram. {latency: count} where "latency" is the
    # latency in microseconds with only 2 significant figures and "count" is the
    # number of response times that fell in that latency range.
    latency_hist = netperf.ParseHistogram(stdout)
    hist_metadata = {'histogram': json.dumps(latency_hist)}
    hist_metadata.update(metadata)
    latency_samples.append(
        sample.Sample('%s_Latency_Histogram' % benchmark_name, 0, 'us',
                      hist_metadata))
  if unit != MBPS:
    for metric_key, metric_name in [
        ('50th Percentile Latency Microseconds', 'p50'),
        ('90th Percentile Latency Microseconds', 'p90'),
        ('99th Percentile Latency Microseconds', 'p99'),
        ('Minimum Latency Microseconds', 'min'),
        ('Maximum Latency Microseconds', 'max'),
        ('Stddev Latency Microseconds', 'stddev')
    ]:
      if metric_key in results:
        latency_samples.append(
            sample.Sample('%s_Latency_%s' % (benchmark_name, metric_name),
                          float(results[metric_key]), 'us', metadata))

  return (throughput_sample, latency_samples, latency_hist)


def RunNetperf(vm, benchmark_name, server_ips, num_streams):
  """Spawns netperf on a remote VM, parses results.

  Args:
    vm: The VM that the netperf TCP_RR benchmark will be run upon.
    benchmark_name: The netperf benchmark to run, see the documentation.
    server_ips: A list of ips for a machine that is running netserver.
    num_streams: The number of netperf client threads to run.

  Returns:
    A sample.Sample object with the result.
  """
  enable_latency_histograms = FLAGS.netperf_enable_histograms or num_streams > 1
  # Throughput benchmarks don't have latency histograms
  enable_latency_histograms = (
      enable_latency_histograms and
      (benchmark_name not in ['TCP_STREAM', 'UDP_STREAM']))
  # Flags:
  # -o specifies keys to include in CSV output.
  # -j keeps additional latency numbers
  # -v sets the verbosity level so that netperf will print out histograms
  # -I specifies the confidence % and width - here 99% confidence that the true
  #    value is within +/- 2.5% of the reported value
  # -i specifies the maximum and minimum number of iterations.
  confidence = (f'-I 99,5 -i {FLAGS.netperf_max_iter},3'
                if FLAGS.netperf_max_iter else '')
  verbosity = '-v2 ' if enable_latency_histograms else ''

  remote_cmd_timeout = (
      FLAGS.netperf_test_length * (FLAGS.netperf_max_iter or 1) + 300)

  metadata = {
      'netperf_test_length': FLAGS.netperf_test_length,
      'sending_thread_count': num_streams,
      'max_iter': FLAGS.netperf_max_iter or 1
  }

  remote_cmd_list = []
  assert server_ips, ('Server VM does not have an IP to use for netperf.')
  for server_ip_idx, server_ip in enumerate(server_ips):
    netperf_cmd = (
        f'{netperf.NETPERF_PATH} '
        '-p {command_port} '
        f'-j {verbosity} '
        f'-t {benchmark_name} '
        f'-H {server_ip} '
        f'-l {FLAGS.netperf_test_length} {confidence}'
        ' -- '
        '-P ,{data_port} '
        f'-o {OUTPUT_SELECTOR}'
    )

    if benchmark_name.upper() == 'UDP_STREAM':
      send_size = FLAGS.netperf_udp_stream_send_size_in_bytes
      netperf_cmd += f' -R 1 -m {send_size} -M {send_size} '
      metadata['netperf_send_size_in_bytes'] = (
          FLAGS.netperf_udp_stream_send_size_in_bytes
      )

    elif benchmark_name.upper() == 'TCP_STREAM':
      send_size = FLAGS.netperf_tcp_stream_send_size_in_bytes
      netperf_cmd += f' -m {send_size} -M {send_size} '
      metadata['netperf_send_size_in_bytes'] = (
          FLAGS.netperf_tcp_stream_send_size_in_bytes
      )

    if FLAGS.netperf_thinktime != 0:
      netperf_cmd += (
          ' -X '
          f'{FLAGS.netperf_thinktime},'
          f'{FLAGS.netperf_thinktime_array_size},'
          f'{FLAGS.netperf_thinktime_run_length} '
      )

    if FLAGS.netperf_mss and 'TCP' in benchmark_name.upper():
      netperf_cmd += f' -G {FLAGS.netperf_mss}b'
      metadata['netperf_mss_requested'] = FLAGS.netperf_mss

    # Run all of the netperf processes and collect their stdout
    # TODO(dlott): Analyze process start delta of netperf processes on the
    # remote machine.

    remote_cmd = (
        f'./{REMOTE_SCRIPT} --netperf_cmd="{netperf_cmd}" '
        f'--num_streams={num_streams} --port_start={PORT_START+(server_ip_idx*2*num_streams)}'
    )
    remote_cmd_list.append(remote_cmd)

  # Give the remote script the max possible test length plus 5 minutes to
  # complete
  remote_cmd_timeout = (
      FLAGS.netperf_test_length * (FLAGS.netperf_max_iter or 1) + 300
  )
  start_time = time.time()
  remote_stdout_stderr_threads = background_tasks.RunThreaded(
      lambda cmd: vm.RobustRemoteCommand(cmd, timeout=remote_cmd_timeout),
      remote_cmd_list,
  )
  end_time = time.time()
  start_time_sample = sample.Sample('start_time', start_time, 'sec', metadata)
  end_time_sample = sample.Sample('end_time', end_time, 'sec', metadata)

  # Decode stdouts, stderrs, and return codes from remote command's stdout
  json_outs = [json.loads(remote_stdout_stderr[0]) for remote_stdout_stderr in
               remote_stdout_stderr_threads]
  stdouts_list = [json_out[0] for json_out in json_outs]

  parsed_output = []
  for stdouts in stdouts_list:
    for stdout in stdouts:
      parsed_output.append(ParseNetperfOutput(
          stdout, metadata, benchmark_name, enable_latency_histograms
      ))

  samples = [start_time_sample, end_time_sample]
  if len(parsed_output) == 1:
    # Only 1 netperf thread
    throughput_sample, latency_samples, histogram = parsed_output[0]
    output_samples = samples + [throughput_sample] + latency_samples
    # Create formatted output for TCP stream throughput metrics
    if benchmark_name.upper() == 'TCP_STREAM':
      output_samples.append(
          sample.Sample(
              throughput_sample.metric + '_1stream',
              throughput_sample.value,
              throughput_sample.unit,
              throughput_sample.metadata,
          )
      )
    return output_samples
  else:
    # Multiple netperf threads
    # Unzip parsed output
    # Note that latency_samples are invalid with multiple threads because stats
    # are computed per-thread by netperf, so we don't use them here.
    throughput_samples, _, latency_histograms = [
        list(t) for t in zip(*parsed_output)
    ]
    # They should all have the same units
    throughput_unit = throughput_samples[0].unit
    # Extract the throughput values from the samples
    throughputs = [s.value for s in throughput_samples]
    # Compute some stats on the throughput values
    throughput_stats = sample.PercentileCalculator(throughputs, [50, 90, 99])
    throughput_stats['min'] = min(throughputs)
    throughput_stats['max'] = max(throughputs)
    # Calculate aggregate throughput
    throughput_stats['total'] = throughput_stats['average'] * len(throughputs)
    # Create samples for throughput stats
    for stat, value in throughput_stats.items():
      samples.append(
          sample.Sample(f'{benchmark_name}_Throughput_{stat}', float(value),
                        throughput_unit, metadata))
    # Create formatted output, following {benchmark_name}_Throughput_Xstream(s)
    # for TCP stream throughput metrics
    if benchmark_name.upper() == 'TCP_STREAM':
      samples.append(
          sample.Sample(
              f'{benchmark_name}_Throughput_{len(parsed_output)}streams',
              throughput_stats['total'],
              throughput_unit,
              metadata,
          )
      )
    if enable_latency_histograms:
      # Combine all of the latency histogram dictionaries
      latency_histogram = collections.Counter()
      for histogram in latency_histograms:
        latency_histogram.update(histogram)
      # Create a sample for the aggregate latency histogram
      hist_metadata = {'histogram': json.dumps(latency_histogram)}
      hist_metadata.update(metadata)
      samples.append(
          sample.Sample(f'{benchmark_name}_Latency_Histogram', 0, 'us',
                        hist_metadata))
      # Calculate stats on aggregate latency histogram
      latency_stats = _HistogramStatsCalculator(latency_histogram, [50, 90, 99])
      # Create samples for the latency stats
      for stat, value in latency_stats.items():
        samples.append(
            sample.Sample(f'{benchmark_name}_Latency_{stat}', float(value),
                          'us', metadata))
    return samples


def Run(benchmark_spec):
  """Extracts the first two VM's from benchmark_spec and runs netperf.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  client_vm, server_vm = benchmark_spec.vms[:2]
  return RunClientServerVMs(client_vm, server_vm)


def RunClientServerVMs(client_vm, server_vm):
  """Runs a netperf process between client_vm and server_vm.

  Args:
    client_vm: The VM that runs the netperf binary.
    server_vm: The VM that runs the netserver binary.

  Returns:
    A list of sample.Sample objects.
  """
  logging.info('netperf running on %s', client_vm)
  results = []
  metadata = {
      'sending_zone': client_vm.zone,
      'sending_machine_type': client_vm.machine_type,
      'receiving_zone': server_vm.zone,
      'receiving_machine_type': server_vm.machine_type
  }

  for num_streams in FLAGS.netperf_num_streams:
    assert num_streams >= 1

    for netperf_benchmark in FLAGS.netperf_benchmarks:
      if vm_util.ShouldRunOnExternalIpAddress():
        external_ip_results = RunNetperf(
            client_vm, netperf_benchmark, [server_vm.ip_address], num_streams
        )
        for external_ip_result in external_ip_results:
          external_ip_result.metadata['ip_type'] = (
              vm_util.IpAddressMetadata.EXTERNAL
          )
          external_ip_result.metadata.update(metadata)
        results.extend(external_ip_results)

      if vm_util.ShouldRunOnInternalIpAddress(client_vm, server_vm):
        internal_ip_results = RunNetperf(
            client_vm,
            netperf_benchmark,
            server_vm.GetInternalIPs(),
            num_streams,
        )
        for internal_ip_result in internal_ip_results:
          internal_ip_result.metadata.update(metadata)
          internal_ip_result.metadata['ip_type'] = (
              vm_util.IpAddressMetadata.INTERNAL
          )
        results.extend(internal_ip_results)

  return results


def Cleanup(benchmark_spec):
  """Extracts the first two VM's from benchmark_spec and calls RunVM(vms).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  client_vm, server_vm = benchmark_spec.vms[:2]
  CleanupClientServerVMs(client_vm, server_vm)


def CleanupClientServerVMs(client_vm, server_vm):
  """Cleanup netperf on the target vm (by uninstalling).

  Args:
    client_vm: The VM that runs the netperf binary.
    server_vm: The VM that runs the netserver binary.
  """
  server_vm.RemoteCommand('sudo killall netserver')
  client_vm.RemoteCommand(f'sudo rm -rf {REMOTE_SCRIPT}')
