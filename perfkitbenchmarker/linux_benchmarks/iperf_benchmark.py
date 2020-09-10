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

flag_util.DEFINE_integerlist('iperf_sending_thread_count',
                             flag_util.IntegerList([1]),
                             'server for sending traffic. Iperf'
                             'will run once for each value in the list',
                             module_name=__name__)
flags.DEFINE_integer('iperf_runtime_in_seconds', 60,
                     'Number of seconds to run iperf.',
                     lower_bound=1)
flags.DEFINE_integer('iperf_timeout', None,
                     'Number of seconds to wait in '
                     'addition to iperf runtime before '
                     'killing iperf client command.',
                     lower_bound=1)
flags.DEFINE_float('iperf_udp_per_stream_bandwidth', None,
                   'In Mbits. Iperf will attempt to send at this bandwidth for UDP tests. '
                   'If using multiple streams, each stream will '
                   'attempt to send at this bandwidth')
flags.DEFINE_float('iperf_tcp_per_stream_bandwidth', None,
                   'In Mbits. Iperf will attempt to send at this bandwidth for TCP tests. '
                   'If using multiple streams, each stream will '
                   'attempt to send at this bandwidth')

TCP = 'TCP'
UDP = 'UDP'
IPERF_BENCHMARKS = [TCP, UDP]

flags.DEFINE_list('iperf_benchmarks', [TCP],
                  'Run TCP, UDP or both')

flags.register_validator(
    'iperf_benchmarks',
    lambda benchmarks: benchmarks and set(benchmarks).issubset(IPERF_BENCHMARKS))

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
        'iperf benchmark requires exactly two machines, found {0}'.format(len(
            vms)))

  for vm in vms:
    vm.Install('iperf')
    if vm_util.ShouldRunOnExternalIpAddress():
      if TCP in FLAGS.iperf_benchmarks:
        vm.AllowPort(IPERF_PORT)
      if UDP in FLAGS.iperf_benchmarks:
        vm.AllowPort(IPERF_UDP_PORT)
    if TCP in FLAGS.iperf_benchmarks:
      stdout, _ = vm.RemoteCommand(('nohup iperf --server --port %s &> /dev/null'
                                    '& echo $!') % IPERF_PORT)
      # TODO store this in a better place once we have a better place
      vm.iperf_tcp_server_pid = stdout.strip()
    if UDP in FLAGS.iperf_benchmarks:
      stdout, _ = vm.RemoteCommand(('nohup iperf --server -u --port %s &> /dev/null'
                                    '& echo $!') % IPERF_UDP_PORT)
      # TODO store this in a better place once we have a better place
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

  if protocol == TCP:

    iperf_cmd = ('iperf -e --client %s --port %s --format m --time %s -P %s' %
                 (receiving_ip_address, IPERF_PORT,
                  FLAGS.iperf_runtime_in_seconds,
                  thread_count))
    if FLAGS.iperf_tcp_per_stream_bandwidth:
      iperf_cmd = iperf_cmd + (' -b %dM' % FLAGS.iperf_tcp_per_stream_bandwidth)
    # the additional time on top of the iperf runtime is to account for the
    # time it takes for the iperf process to start and exit
    timeout_buffer = FLAGS.iperf_timeout or 30 + thread_count
    stdout, _ = sending_vm.RemoteCommand(iperf_cmd, should_log=True,
                                         timeout=FLAGS.iperf_runtime_in_seconds + timeout_buffer)

    # Example output from iperf that needs to be parsed
    # STDOUT: ------------------------------------------------------------
    # Client connecting to 10.237.229.201, TCP port 5001
    # TCP window size: 0.04 MByte (default)
    # ------------------------------------------------------------
    # [  6] local 10.76.234.115 port 53527 connected with 10.237.229.201 port 5001
    # [  3] local 10.76.234.115 port 53524 connected with 10.237.229.201 port 5001
    # [  4] local 10.76.234.115 port 53525 connected with 10.237.229.201 port 5001
    # [  5] local 10.76.234.115 port 53526 connected with 10.237.229.201 port 5001
    # [ ID] Interval       Transfer     Bandwidth
    # [  4]  0.0-60.0 sec  3730 MBytes  521.1 Mbits/sec
    # [  5]  0.0-60.0 sec  3499 MBytes   489 Mbits/sec
    # [  6]  0.0-60.0 sec  3044 MBytes   425 Mbits/sec
    # [  3]  0.0-60.0 sec  3738 MBytes   522 Mbits/sec
    # [SUM]  0.0-60.0 sec  14010 MBytes  1957 Mbits/sec

    # NEW OUTPUT
    #   ------------------------------------------------------------
    # Client connecting to 172.17.0.5, TCP port 20000 with pid 4167
    # Write buffer size: 0.12 MByte
    # TCP window size: 1.42 MByte (default)
    # ------------------------------------------------------------
    # [  3] local 172.17.0.6 port 45518 connected with 172.17.0.5 port 20000 (ct=0.08 ms)
    # [ ID] Interval        Transfer    Bandwidth       Write/Err  Rtry     Cwnd/RTT        NetPwr
    # [  3] 0.00-60.00 sec  236112 MBytes  33011 Mbits/sec  1888894/0          0       -1K/25 us  165054051.49

    multi_thread = re.findall(r'\[SUM\]\s+\d+\.\d+-\d+\.\d+\s\w+\s+\d+\s\w+\s+\d+\s\w+\/\w+\s+\d+\/\d+\s+\d+\s+', stdout)
    window_size = re.findall(r'TCP window size: \d+\.\d+ \S+', stdout)
    buffer_size_re = re.findall(r'Write buffer size: \d+\.\d+ \S+', stdout)
    buffer_size = re.findall(r'\d+\.\d+', str(buffer_size_re))
    buffer_size_num = float(buffer_size[0])
    # buffer_size_unit = re.findall(r'\d+\.\d+ (\S+)', buffer_size_re[0])
    window_size_num = (re.findall(r'\d+\.\d+', str(window_size)))
    window_size_num = float(window_size_num[0])
    window_size_measurement = re.findall(r'\d+\.\d+ (\S+)', window_size[0])
    window_size_measurement = window_size_measurement[0]

    # Iperf output is formatted differently when running with multiple threads vs a single thread
    if multi_thread:
      # Write and Err
      write_err = re.findall(r'\d+ Mbits\/sec\s+(\d+\/\d+)', str(multi_thread))
      write_re = re.findall(r'\d+', str(write_err))
      write = int(write_re[0])
      err = int(write_re[1])
      # Retry
      retry_re = re.findall(r'\d+ Mbits\/sec\s+ \d+\/\d+\s+(\d+)', str(multi_thread))
      retry = int(retry_re[0])

      # Cwnd
      cwnd_rtt = re.findall(r'\d+ Mbits\/sec\s+ \d+\/\d+\s+\d+\s+(-*\d+\w+\-*/\d+\s+\w+)', stdout)
      rtt = 0
      for i in cwnd_rtt:
        rtt_part = re.findall(r'\/(-*\d+)', i)
        rtt = rtt + float(rtt_part[0])
      # calculating average
      rtt = round(float(rtt) / len(cwnd_rtt), 2)

      cwnd_re = re.findall(r'-*\d+\s*', cwnd_rtt[0])
      cwnd = float(cwnd_re[0])
      cwnd_unit_re = re.findall(r'-*\d+\s*(\w+)', cwnd_rtt[0])
      # cwnd_unit = cwnd_unit_re[0]
      rtt_unit = cwnd_unit_re[1]

      # Netpwr
      netpwr_re = re.findall(r'\d+ Mbits\/sec\s+ \d+\/\d+\s+\d+\s+-*\d+\w+\/\d+\s+\w+\s+(\d+\.\d+)', stdout)
      netpwr = 0
      for i in netpwr_re:
        netpwr = netpwr + float(i)
      netpwr = netpwr / len(netpwr_re)
      netpwr = round(float(netpwr), 2)

    # if single thread
    else:

      # Write and Err
      write_err = re.findall(r'\d+ Mbits\/sec\s+(\d+\/\d+)', str(stdout))
      write_re = re.findall(r'\d+', str(write_err))
      write = int(write_re[0])
      err = int(write_re[1])

      # Retry
      retry_re = re.findall(r'\d+ Mbits\/sec\s+ \d+\/\d+\s+(\d+)', str(stdout))
      retry = int(retry_re[0])

      # Cwnd
      cwnd_rtt = re.findall(r'\d+ Mbits\/sec\s+ \d+\/\d+\s+\d+\s+(-*\d+\w+\-*/\d+\s+\w+)', stdout)
      cwnd_re = re.findall(r'-*\d+\s*', cwnd_rtt[0])
      cwnd = float(cwnd_re[0])
      cwnd_unit_re = re.findall(r'-*\d+\s*(\w+)', cwnd_rtt[0])
      # cwnd_unit = cwnd_unit_re[0]
      rtt = float(cwnd_re[1])
      rtt_unit = cwnd_unit_re[1]

      # Netpwr
      netpwr = re.findall(r'\d+ Mbits\/sec\s+ \d+\/\d+\s+\d+\s+-*\d+\w+\/\d+\s+\w+\s+(\d+\.\d+)', stdout)
      netpwr = float(netpwr[0])

    thread_values = re.findall(r'\[SUM].*\s+(\d+\.?\d*).Mbits/sec', stdout)
    if not thread_values:
      # If there is no sum you have try and figure out an estimate
      # which happens when threads start at different times.  The code
      # below will tend to overestimate a bit.
      thread_values = re.findall(r'\[.*\d+\].*\s+(\d+\.?\d*).Mbits/sec', stdout)

      if len(thread_values) != thread_count:
        raise ValueError('Only %s out of %s iperf threads reported a'
                         ' throughput value.' %
                         (len(thread_values), thread_count))

    total_throughput = 0.0
    for value in thread_values:
      total_throughput += float(value)

    metadata = {
        # The meta data defining the environment
        'receiving_machine_type': receiving_vm.machine_type,
        'receiving_zone': receiving_vm.zone,
        'sending_machine_type': sending_vm.machine_type,
        'sending_thread_count': thread_count,
        'sending_zone': sending_vm.zone,
        'runtime_in_seconds': FLAGS.iperf_runtime_in_seconds,
        'ip_type': ip_type,
        'buffer_size': buffer_size_num,
        'tcp_window_size': window_size_num,
        'write': write,
        'err': err,
        'retry': retry,
        'cwnd': cwnd,
        'rtt': rtt,
        'rtt_unit': rtt_unit,
        'netpwr': netpwr
    }
    return sample.Sample('Throughput', total_throughput, 'Mbits/sec', metadata)


  elif protocol == UDP:

    iperf_cmd = ('iperf -e -u --client %s --port %s --format m --time %s -P %s' %
                 (receiving_ip_address, IPERF_UDP_PORT,
                  FLAGS.iperf_runtime_in_seconds,
                  thread_count))

    if FLAGS.iperf_udp_per_stream_bandwidth:
      iperf_cmd = iperf_cmd + (' -b %dM' % FLAGS.iperf_udp_per_stream_bandwidth)
      
    # the additional time on top of the iperf runtime is to account for the
    # time it takes for the iperf process to start and exit
    timeout_buffer = FLAGS.iperf_timeout or 30 + thread_count
    stdout, _ = sending_vm.RemoteCommand(iperf_cmd, should_log=True,
                                         timeout=FLAGS.iperf_runtime_in_seconds +
                                         timeout_buffer)

    # Client connecting to 35.245.145.216, UDP port 25000 with pid 10286
    # Sending 1470 byte datagrams, IPG target: 11215.21 us (kalman adjust)
    # UDP buffer size: 0.20 MByte (default)
    # ------------------------------------------------------------
    # [  3] local 10.128.0.2 port 41821 connected with 35.245.145.216 port 25000
    # [ ID] Interval        Transfer     Bandwidth      Write/Err  PPS
    # [  3] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
    # [  3] Sent 5350 datagrams
    # [  3] Server Report:
    # [  3]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.289 ms    0/ 5350 (0%)


    # ------------------------------------------------------------
    # Client connecting to 35.245.176.237, UDP port 25000 with pid 10171
    # Sending 1470 byte datagrams, IPG target: 11215.21 us (kalman adjust)
    # UDP buffer size: 0.20 MByte (default)
    # ------------------------------------------------------------
    # [  5] local 10.128.0.2 port 52277 connected with 35.245.176.237 port 25000
    # [  3] local 10.128.0.2 port 51332 connected with 35.245.176.237 port 25000
    # [  4] local 10.128.0.2 port 53360 connected with 35.245.176.237 port 25000
    # [ ID] Interval        Transfer     Bandwidth      Write/Err  PPS
    # [  5] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
    # [  5] Sent 5350 datagrams
    # [  3] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
    # [  3] Sent 5350 datagrams
    # [  4] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
    # [  4] Sent 5350 datagrams
    # [SUM] 0.00-60.00 sec  22.5 MBytes  3.15 Mbits/sec  16050/0      267 pps
    # [SUM] Sent 16050 datagrams
    # [  5] Server Report:
    # [  5]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.087 ms    0/ 5350 (0%)
    # [  3] Server Report:
    # [  3]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.085 ms    1/ 5350 (0.019%)
    # [  4] Server Report:
    # [  4]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.058 ms    0/ 5350 (0%)
    # [  4] 0.00-60.00 sec  1 datagrams received out-of-order

    multi_thread = re.findall(r'\[SUM\]\s\d+\.?\d+-\d+\.?\d+\ssec\s+\d+\.?\d+\s+MBytes\s+\d+\.?\d+\s+Mbits\/sec\s+\d+\/\d+\s+\d+\s+pps', stdout)

    buffer_size_re = re.findall(r'UDP buffer size: \d+\.\d+ \S+', stdout)
    buffer_size = re.findall(r'(\d+\.\d+)', buffer_size_re[0])
    buffer_size_num = float(buffer_size[0])
    # buffer_size_unit = re.findall(r'\d+\.\d+ (\S+)', buffer_size_re[0])
    datagram_size = int(re.findall(r'(\d+)\sbyte\sdatagrams', stdout)[0])
    ipg_target = float(re.findall(r'IPG\starget:\s(\d+.?\d+)', stdout)[0])
    ipg_target_unit = str(re.findall(r'IPG\starget:\s\d+.?\d+\s(\S+)\s', stdout)[0])

    if multi_thread:
      # Write and Err
      write_err = re.findall(r'\d+\s+Mbits\/sec\s+(\d+\/\d+)', str(multi_thread))
      write_re = re.findall(r'\d+', str(write_err))
      write = int(write_re[0])
      err = int(write_re[1])

      # pps
      pps = re.findall(r'(\d+)\s+pps', str(multi_thread))
      pps = int(pps[0])

    else:
      # Write and Err
      write_err = re.findall(r'\d+\s+Mbits\/sec\s+(\d+\/\d+)', str(stdout))
      write_re = re.findall(r'\d+', str(write_err))
      write = int(write_re[0])
      err = int(write_re[1])

      # pps
      pps = re.findall(r'(\d+)\s+pps', str(stdout))
      pps = int(pps[0])

    # Jitter
    jitter_array = re.findall(r'Mbits\/sec\s+(\d+\.?\d+)\s+[a-zA-Z]+', stdout)
    jitter_avg = sum(float(x) for x in jitter_array) / len(jitter_array)

    jitter_unit = str(re.findall(r'Mbits\/sec\s+\d+\.?\d+\s+([a-zA-Z]+)', stdout)[0])

    # total and lost datagrams
    lost_datagrams_array = re.findall(r'Mbits\/sec\s+\d+\.?\d+\s+[a-zA-Z]+\s+(\d+)\/\s+\d+\s+\(', stdout)
    total_datagrams_array = re.findall(r'Mbits\/sec\s+\d+\.?\d+\s+[a-zA-Z]+\s+\d+\/\s+(\d+)+\s+\(', stdout)

    lost_datagrams_sum = sum(int(x) for x in lost_datagrams_array)
    total_datagrams_sum = sum(int(x) for x in total_datagrams_array)

    # out of order datagrams
    out_of_order_sum = 0
    out_of_order_array = re.findall(r'(\d+)\s+datagrams\sreceived\sout-of-order', stdout)
    if out_of_order_array:
      out_of_order_sum = sum(int(x) for x in out_of_order_array)

    thread_values = re.findall(r'\[SUM].*\s+(\d+\.?\d*).Mbits/sec', stdout)
    if not thread_values:
      # If there is no sum you have try and figure out an estimate
      # which happens when threads start at different times.  The code
      # below will tend to overestimate a bit.
      thread_values = re.findall(r'\[.*\d+\].*\s+(\d+\.?\d*).Mbits/sec\s+\d+\/\d+', stdout)

      if len(thread_values) != thread_count:
        raise ValueError('Only %s out of %s iperf threads reported a'
                         ' throughput value.' %
                         (len(thread_values), thread_count))

    total_throughput = 0.0
    for value in thread_values:
      total_throughput += float(value)

    metadata = {
        # The meta data defining the environment
        'receiving_machine_type': receiving_vm.machine_type,
        'receiving_zone': receiving_vm.zone,
        'sending_machine_type': sending_vm.machine_type,
        'sending_thread_count': thread_count,
        'sending_zone': sending_vm.zone,
        'runtime_in_seconds': FLAGS.iperf_runtime_in_seconds,
        'ip_type': ip_type,
        'buffer_size': buffer_size_num,
        'datagram_size_bytes': datagram_size,
        'write': write,
        'err': err,
        'pps': pps,
        'ipg_target': ipg_target,
        'ipg_target_unit': ipg_target_unit,
        'jitter': jitter_avg,
        'jitter_unit': jitter_unit,
        'lost_datagrams': lost_datagrams_sum,
        'total_datagrams': total_datagrams_sum,
        'out_of_order_datagrams': out_of_order_sum
    }
    return sample.Sample('UDP Throughput', total_throughput, 'Mbits/sec', metadata)


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
          results.append(_RunIperf(sending_vm,
                                   receiving_vm,
                                   receiving_vm.ip_address,
                                   thread_count,
                                   'external',
                                   protocol))

        # Send using internal IP addresses
        if vm_util.ShouldRunOnInternalIpAddress(sending_vm,
                                                receiving_vm):
          results.append(_RunIperf(sending_vm,
                                   receiving_vm,
                                   receiving_vm.internal_ip,
                                   thread_count,
                                   'internal',
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
      vm.RemoteCommand('kill -9 ' + vm.iperf_tcp_server_pid, ignore_failure=True)
    if UDP in FLAGS.iperf_benchmarks:
      vm.RemoteCommand('kill -9 ' + vm.iperf_udp_server_pid, ignore_failure=True)
