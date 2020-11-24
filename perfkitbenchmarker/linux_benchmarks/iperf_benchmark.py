# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs plain Iperf.

Docs:
http://iperf.fr/

Runs Iperf to collect network throughput.
"""

import logging
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flag_util.DEFINE_integerlist(
    'iperf_sending_thread_count',
    flag_util.IntegerList([1]), 'server for sending traffic. Iperf'
    'will run once for each value in the list',
    module_name=__name__)
flags.DEFINE_integer(
    'iperf_runtime_in_seconds',
    60,
    'Number of seconds to run iperf.',
    lower_bound=1)
flags.DEFINE_integer(
    'iperf_timeout',
    None, 'Number of seconds to wait in '
    'addition to iperf runtime before '
    'killing iperf client command.',
    lower_bound=1)
flags.DEFINE_float(
    'iperf_udp_per_stream_bandwidth', None,
    'In Mbits. Iperf will attempt to send at this bandwidth for UDP tests. '
    'If using multiple streams, each stream will '
    'attempt to send at this bandwidth')
flags.DEFINE_float(
    'iperf_tcp_per_stream_bandwidth', None,
    'In Mbits. Iperf will attempt to send at this bandwidth for TCP tests. '
    'If using multiple streams, each stream will '
    'attempt to send at this bandwidth')

TCP = 'TCP'
UDP = 'UDP'
IPERF_BENCHMARKS = [TCP, UDP]

flags.DEFINE_list('iperf_benchmarks', [TCP], 'Run TCP, UDP or both')

flags.register_validator(
    'iperf_benchmarks',
    lambda benchmarks: benchmarks and set(benchmarks).issubset(IPERF_BENCHMARKS)
    )

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'iperf'
BENCHMARK_CONFIG = """
iperf:
  description: Run iperf
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
"""

IPERF_PORT = 20000
IPERF_UDP_PORT = 25000
IPERF_RETRIES = 5


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install iperf and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  if len(vms) != 2:
    raise ValueError(
        f'iperf benchmark requires exactly two machines, found {len(vms)}')

  for vm in vms:
    vm.Install('iperf')
    if vm_util.ShouldRunOnExternalIpAddress():
      if TCP in FLAGS.iperf_benchmarks:
        vm.AllowPort(IPERF_PORT)
      if UDP in FLAGS.iperf_benchmarks:
        vm.AllowPort(IPERF_UDP_PORT)
    if TCP in FLAGS.iperf_benchmarks:
      stdout, _ = vm.RemoteCommand(f'nohup iperf --server --port {IPERF_PORT}'
                                   ' &> /dev/null & echo $!')

      # TODO(ssabhaya): store this in a better place once we have a better place
      vm.iperf_tcp_server_pid = stdout.strip()
    if UDP in FLAGS.iperf_benchmarks:
      stdout, _ = vm.RemoteCommand(
          f'nohup iperf --server --udp --port {IPERF_UDP_PORT}'
          ' &> /dev/null & echo $!')
      # TODO(ssabhaya): store this in a better place once we have a better place
      vm.iperf_udp_server_pid = stdout.strip()


@vm_util.Retry(max_retries=IPERF_RETRIES)
def _RunIperf(sending_vm, receiving_vm, receiving_ip_address, thread_count,
              ip_type, protocol):
  """Run iperf using sending 'vm' to connect to 'ip_address'.

  Args:
    sending_vm: The VM sending traffic.
    receiving_vm: The VM receiving traffic.
    receiving_ip_address: The IP address of the iperf server (ie the receiver).
    thread_count: The number of threads the server will use.
    ip_type: The IP type of 'ip_address' (e.g. 'internal', 'external')
    protocol: The protocol for Iperf to use. Either 'TCP' or 'UDP'

  Returns:
    A Sample.
  """

  metadata = {
      # The meta data defining the environment
      'receiving_machine_type': receiving_vm.machine_type,
      'receiving_zone': receiving_vm.zone,
      'sending_machine_type': sending_vm.machine_type,
      'sending_thread_count': thread_count,
      'sending_zone': sending_vm.zone,
      'runtime_in_seconds': FLAGS.iperf_runtime_in_seconds,
      'ip_type': ip_type,
  }

  if protocol == TCP:

    iperf_cmd = (
        f'iperf --enhancedreports --client {receiving_ip_address} --port '
        f'{IPERF_PORT} --format m --time {FLAGS.iperf_runtime_in_seconds} '
        f'--parallel {thread_count}')

    if FLAGS.iperf_tcp_per_stream_bandwidth:
      iperf_cmd += f' --bandwidth {FLAGS.iperf_tcp_per_stream_bandwidth}M'

    # the additional time on top of the iperf runtime is to account for the
    # time it takes for the iperf process to start and exit
    timeout_buffer = FLAGS.iperf_timeout or 30 + thread_count
    stdout, _ = sending_vm.RemoteCommand(
        iperf_cmd,
        should_log=True,
        timeout=FLAGS.iperf_runtime_in_seconds + timeout_buffer)

    window_size_match = re.search(
        r'TCP window size: (?P<size>\d+\.?\d+) (?P<units>\S+)', stdout)
    window_size = float(window_size_match.group('size'))

    buffer_size = float(
        re.search(r'Write buffer size: (?P<buffer_size>\d+\.\d+) \S+',
                  stdout).group('buffer_size'))

    multi_thread = re.search((
        r'\[SUM\]\s+\d+\.\d+-\d+\.\d+\s\w+\s+(?P<transfer>\d+)\s\w+\s+(?P<throughput>\d+)'
        r'\s\w+/\w+\s+(?P<write>\d+)/(?P<err>\d+)\s+(?P<retry>\d+)\s*'), stdout)
    # Iperf output is formatted differently when running with multiple threads
    # vs a single thread
    if multi_thread:
      # Write, error, retry
      write = int(multi_thread.group('write'))
      err = int(multi_thread.group('err'))
      retry = int(multi_thread.group('retry'))

    # if single thread
    else:
      # Write, error, retry
      match = re.search(
          r'\d+ Mbits/sec\s+(?P<write>\d+)/(?P<err>\d+)\s+(?P<retry>\d+)',
          stdout)
      write = int(match.group('write'))
      err = int(match.group('err'))
      retry = int(match.group('retry'))

    r = re.compile((
        r'\d+ Mbits\/sec\s+ \d+\/\d+\s+\d+\s+(?P<cwnd>-*\d+)(?P<cwnd_unit>\w+)\/(?P<rtt>\d+)'
        r'\s+(?P<rtt_unit>\w+)\s+(?P<netpwr>\d+\.\d+)'))
    match = [m.groupdict() for m in r.finditer(stdout)]

    cwnd = sum(float(i['cwnd']) for i in match) / len(match)
    rtt = round(sum(float(i['rtt']) for i in match) / len(match), 2)
    netpwr = round(sum(float(i['netpwr']) for i in match) / len(match), 2)

    rtt_unit = match[0]['rtt_unit']

    thread_values = re.findall(r'\[SUM].*\s+(\d+\.?\d*).Mbits/sec', stdout)
    if not thread_values:
      # If there is no sum you have try and figure out an estimate
      # which happens when threads start at different times.  The code
      # below will tend to overestimate a bit.
      thread_values = re.findall(r'\[.*\d+\].*\s+(\d+\.?\d*).Mbits/sec', stdout)

      if len(thread_values) != thread_count:
        raise ValueError(f'Only {len(thread_values)} out of {thread_count}'
                         ' iperf threads reported a throughput value.')

    total_throughput = sum(float(value) for value in thread_values)

    tcp_metadata = {
        'buffer_size': buffer_size,
        'tcp_window_size': window_size,
        'write_packet_count': write,
        'err_packet_count': err,
        'retry_packet_count': retry,
        'congestion_window': cwnd,
        'rtt': rtt,
        'rtt_unit': rtt_unit,
        'netpwr': netpwr
    }
    metadata.update(tcp_metadata)
    return sample.Sample('Throughput', total_throughput, 'Mbits/sec', metadata)

  elif protocol == UDP:

    iperf_cmd = (
        f'iperf --enhancedreports --udp --client {receiving_ip_address} --port'
        f' {IPERF_UDP_PORT} --format m --time {FLAGS.iperf_runtime_in_seconds}'
        f' --parallel {thread_count}')

    if FLAGS.iperf_udp_per_stream_bandwidth:
      iperf_cmd += f' --bandwidth {FLAGS.iperf_udp_per_stream_bandwidth}M'

    # the additional time on top of the iperf runtime is to account for the
    # time it takes for the iperf process to start and exit
    timeout_buffer = FLAGS.iperf_timeout or 30 + thread_count
    stdout, _ = sending_vm.RemoteCommand(
        iperf_cmd,
        should_log=True,
        timeout=FLAGS.iperf_runtime_in_seconds + timeout_buffer)

    match = re.search(
        r'UDP buffer size: (?P<buffer_size>\d+\.\d+)\s+(?P<buffer_unit>\w+)',
        stdout)
    buffer_size = float(match.group('buffer_size'))
    datagram_size = int(
        re.findall(r'(?P<datagram_size>\d+)\sbyte\sdatagrams', stdout)[0])
    ipg_target = float(re.findall(r'IPG\starget:\s(\d+.?\d+)', stdout)[0])
    ipg_target_unit = str(
        re.findall(r'IPG\starget:\s\d+.?\d+\s(\S+)\s', stdout)[0])

    multi_thread = re.search(
        (r'\[SUM\]\s\d+\.?\d+-\d+\.?\d+\ssec\s+\d+\.?\d+\s+MBytes\s+\d+\.?\d+'
         r'\s+Mbits/sec\s+(?P<write>\d+)/(?P<err>\d+)\s+(?P<pps>\d+)\s+pps'),
        stdout)
    if multi_thread:
      # Write, Err, PPS
      write = int(multi_thread.group('write'))
      err = int(multi_thread.group('err'))
      pps = int(multi_thread.group('pps'))

    else:
      # Write, Err, PPS
      match = re.search(
          r'\d+\s+Mbits/sec\s+(?P<write>\d+)/(?P<err>\d+)\s+(?P<pps>\d+)\s+pps',
          stdout)
      write = int(match.group('write'))
      err = int(match.group('err'))
      pps = int(match.group('pps'))

    # Jitter
    jitter_array = re.findall(r'Mbits/sec\s+(?P<jitter>\d+\.?\d+)\s+[a-zA-Z]+',
                              stdout)
    jitter_avg = sum(float(x) for x in jitter_array) / len(jitter_array)

    jitter_unit = str(
        re.search(r'Mbits/sec\s+\d+\.?\d+\s+(?P<jitter_unit>[a-zA-Z]+)',
                  stdout).group('jitter_unit'))

    # total and lost datagrams
    match = re.findall(
        r'(?P<lost_datagrams>\d+)/\s*(?P<total_datagrams>\d+)\s+\(', stdout)
    lost_datagrams_sum = sum(float(i[0]) for i in match)
    total_datagrams_sum = sum(float(i[1]) for i in match)

    # out of order datagrams
    out_of_order_array = re.findall(
        r'(\d+)\s+datagrams\sreceived\sout-of-order', stdout)
    out_of_order_sum = sum(int(x) for x in out_of_order_array)

    thread_values = re.findall(r'\[SUM].*\s+(\d+\.?\d*).Mbits/sec', stdout)
    if not thread_values:
      # If there is no sum you have try and figure out an estimate
      # which happens when threads start at different times.  The code
      # below will tend to overestimate a bit.
      thread_values = re.findall(
          r'\[.*\d+\].*\s+(\d+\.?\d*).Mbits/sec\s+\d+/\d+', stdout)

      if len(thread_values) != thread_count:
        raise ValueError(
            f'Only {len(thread_values)} out of {thread_count} iperf threads reported a'
            ' throughput value.')

    total_throughput = sum(float(value) for value in thread_values)

    udp_metadata = {
        'buffer_size': buffer_size,
        'datagram_size_bytes': datagram_size,
        'write_packet_count': write,
        'err_packet_count': err,
        'pps': pps,
        'ipg_target': ipg_target,
        'ipg_target_unit': ipg_target_unit,
        'jitter': jitter_avg,
        'jitter_unit': jitter_unit,
        'lost_datagrams': lost_datagrams_sum,
        'total_datagrams': total_datagrams_sum,
        'out_of_order_datagrams': out_of_order_sum
    }
    metadata.update(udp_metadata)
    return sample.Sample('UDP Throughput', total_throughput, 'Mbits/sec',
                         metadata)


def Run(benchmark_spec):
  """Run iperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  results = []

  logging.info('Iperf Results:')

  for protocol in FLAGS.iperf_benchmarks:
    for thread_count in FLAGS.iperf_sending_thread_count:
      # Send traffic in both directions
      for sending_vm, receiving_vm in vms, reversed(vms):
        # Send using external IP addresses
        if vm_util.ShouldRunOnExternalIpAddress():
          results.append(
              _RunIperf(sending_vm,
                        receiving_vm,
                        receiving_vm.ip_address,
                        thread_count,
                        vm_util.IpAddressMetadata.EXTERNAL,
                        protocol))

        # Send using internal IP addresses
        if vm_util.ShouldRunOnInternalIpAddress(sending_vm, receiving_vm):
          results.append(
              _RunIperf(sending_vm,
                        receiving_vm,
                        receiving_vm.internal_ip,
                        thread_count,
                        vm_util.IpAddressMetadata.INTERNAL,
                        protocol))

  return results


def Cleanup(benchmark_spec):
  """Cleanup iperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms:
    if TCP in FLAGS.iperf_benchmarks:
      vm.RemoteCommand(
          f'kill -9 {vm.iperf_tcp_server_pid}', ignore_failure=True)
    if UDP in FLAGS.iperf_benchmarks:
      vm.RemoteCommand(
          f'kill -9 {vm.iperf_udp_server_pid}', ignore_failure=True)
