# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing Iperf3 windows installation and cleanup functions."""

import ntpath

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_integer('max_bandwidth_mb', 500,
                     'The maximum bandwidth, in megabytes, to test in a '
                     'UDP stream.')

flags.DEFINE_integer('min_bandwidth_mb', 100,
                     'The minimum bandwidth, in megabytes, to test in a '
                     'UDP stream.')

flags.DEFINE_integer('bandwidth_step_mb', 100,
                     'The amount of megabytes to increase bandwidth in each '
                     'UDP stream test.')

flags.DEFINE_integer('udp_stream_seconds', 3,
                     'The amount of time to run the UDP stream test.')

flags.DEFINE_integer('tcp_stream_seconds', 3,
                     'The amount of time to run the TCP stream test.')

flags.DEFINE_integer('tcp_number_of_streams', 10,
                     'The number of parrallel streams to run in the TCP test')

flags.DEFINE_bool('run_tcp', True,
                  'setting to false will disable the run of the TCP test')

flags.DEFINE_bool('run_udp', False,
                  'setting to true will enable the run of the UDP test')

IPERF3_DIR = 'iperf-3.1.3-win64'
IPERF3_ZIP = IPERF3_DIR + '.zip'
IPERF3_URL = 'http://iperf.fr/download/windows/' + IPERF3_ZIP

IPERF3_OUT_FILE = 'iperf_results'
IPERF3_UDP_PORT = 5201
IPERF3_TCP_PORT = IPERF3_UDP_PORT


def Install(vm):
  zip_path = ntpath.join(vm.temp_dir, IPERF3_ZIP)
  vm.DownloadFile(IPERF3_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def RunIperf3TCPMultiStream(sending_vm, receiving_vm, use_internal_ip=True):
  """Run a multi-stream TCP bandwidth between two VMs.

  Args:
    sending_vm: The client VM that will send the TCP packets.
    receiving_vm: The server VM that will receive the UDP packets.
    use_internal_ip: if true, the private network will be used for the test.
                     if false, the external network will be used for the test.

  Returns:
    List of sample objects each representing a single metric on a single run.
  """

  receiver_ip = (
      receiving_vm.internal_ip if use_internal_ip else receiving_vm.ip_address)

  sender_args = ('--client {ip} --port {port} -t {time} -P {num_streams} -f M '
                 '> {out_file}').format(
                     ip=receiver_ip,
                     port=IPERF3_TCP_PORT,
                     time=FLAGS.tcp_stream_seconds,
                     num_streams=FLAGS.tcp_number_of_streams,
                     out_file=IPERF3_OUT_FILE)

  output = _RunIperf3ServerClientPair(sending_vm, sender_args, receiving_vm)

  return ParseTCPMultiStreamOutput(output, sending_vm, receiving_vm,
                                   FLAGS.tcp_number_of_streams, use_internal_ip)


def _RunIperf3ServerClientPair(sending_vm, sender_args, receiving_vm):
  """Create a server-client iperf3 pair.

  The server exits after the client completes its request.

  Args:
    sending_vm: The client VM that will send the UDP/TCP packets.
    sender_args: the client VM iperf3 args.
    receiving_vm: The server VM that will receive the UDP packets.

  Returns:
    output from the client iperf3 process.
  """

  iperf3_exec_dir = ntpath.join(sending_vm.temp_dir, IPERF3_DIR)

  def _RunIperf3(vm, options, is_client):
    # to ensure that the server is up before the client, we wait for 5 second
    # when executing the client command
    command = ('cd {iperf3_exec_dir}; '
               'sleep {delay_time}; '
               '.\\iperf3.exe {options}').format(
                   iperf3_exec_dir=iperf3_exec_dir,
                   delay_time=(5 if is_client else 0),
                   options=options)
    vm.RemoteCommand(command)

  receiver_args = '--server -1'

  threaded_args = [((receiving_vm, receiver_args, False), {}),
                   ((sending_vm, sender_args, True), {})]

  vm_util.RunThreaded(_RunIperf3, threaded_args)

  cat_command = 'cd {iperf3_exec_dir}; cat {out_file}'.format(
      iperf3_exec_dir=iperf3_exec_dir, out_file=IPERF3_OUT_FILE)
  command_out, _ = sending_vm.RemoteCommand(cat_command)

  return command_out


def RunIperf3UDPStream(sending_vm, receiving_vm, use_internal_ip=True):
  """Runs the Iperf3 UDP stream test.

  Args:
    sending_vm: The client VM that will send the UDP packets.
    receiving_vm: The server VM that will receive the UDP packets.
    use_internal_ip: if true, the private network will be used for the test.
                     if false, the external network will be used for the test.

  Returns:
    List of sample objects each representing a single metric on a single run.
  """
  iperf3_exec_dir = ntpath.join(sending_vm.temp_dir, IPERF3_DIR)

  def _RunIperf3(vm, options):
    command = 'cd {iperf3_exec_dir}; .\\iperf3.exe {options}'.format(
        iperf3_exec_dir=iperf3_exec_dir,
        options=options)
    vm.RemoteCommand(command)

  receiver_ip = (receiving_vm.internal_ip if use_internal_ip
                 else receiving_vm.ip_address)

  samples = []

  for bandwidth in xrange(FLAGS.min_bandwidth_mb,
                          FLAGS.max_bandwidth_mb,
                          FLAGS.bandwidth_step_mb):
    sender_args = ('--client {server_ip} --udp -t {num_tries} '
                   '-b {bandwidth}M -w 32M > {out_file}'.format(
                       server_ip=receiver_ip,
                       num_tries=FLAGS.udp_stream_seconds,
                       bandwidth=bandwidth,
                       out_file=IPERF3_OUT_FILE))

    # the "-1" flag will cause the server to exit after performing a single
    # test. This is necessary because the RemoteCommand call will not return
    # until the command completes, even if it is run as a daemon.
    receiver_args = '--server -1'

    threaded_args = [((receiving_vm, receiver_args), {}),
                     ((sending_vm, sender_args), {})]

    vm_util.RunThreaded(_RunIperf3, threaded_args)

    # retrieve the results and parse them
    cat_command = 'cd {iperf3_exec_dir}; cat {out_file}'.format(
        iperf3_exec_dir=iperf3_exec_dir,
        out_file=IPERF3_OUT_FILE)
    command_out, _ = sending_vm.RemoteCommand(cat_command)
    samples.extend(
        GetUDPStreamSamples(sending_vm, receiving_vm, command_out, bandwidth,
                            use_internal_ip))

  return samples


# Connecting to host 127.0.0.1, port 5201
# [  4] local 127.0.0.1 port 53966 connected to 127.0.0.1 port 5201
# [  6] local 127.0.0.1 port 53967 connected to 127.0.0.1 port 5201
# [  8] local 127.0.0.1 port 53968 connected to 127.0.0.1 port 5201
# [ 10] local 127.0.0.1 port 53969 connected to 127.0.0.1 port 5201
# [ 12] local 127.0.0.1 port 53970 connected to 127.0.0.1 port 5201
# [ ID] Interval           Transfer     Bandwidth
# [  4]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
# [  6]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
# [  8]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
# [ 10]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
# [ 12]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
# [SUM]   0.00-1.01   sec   512 MBytes  4.27 Gbits/sec
# - - - - - - - - - - - - - - - - - - - - - - - - -
# [  4]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
# [  6]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
# [  8]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
# [ 10]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
# [ 12]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
# [SUM]   1.01-2.00   sec   531 MBytes  4.48 Gbits/sec
# - - - - - - - - - - - - - - - - - - - - - - - - -
# [  4]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
# [  6]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
# [  8]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
# [ 10]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
# [ 12]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
# [SUM]   2.00-3.01   sec   631 MBytes  5.27 Gbits/sec
# - - - - - - - - - - - - - - - - - - - - - - - - -
# [ ID] Interval           Transfer     Bandwidth
# [  4]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
# [  4]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
# [  6]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
# [  6]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
# [  8]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
# [  8]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
# [ 10]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
# [ 10]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
# [ 12]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
# [ 12]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
# [SUM]   0.00-3.01   sec  1.64 GBytes  4.67 Gbits/sec                  sender
# [SUM]   0.00-3.01   sec  1.64 GBytes  4.67 Gbits/sec                  receiver
#
# iperf Done.


def ParseTCPMultiStreamOutput(results, sending_vm, receiving_vm, num_streams,
                              internal_ip_used):
  """Turns the 'results' into a list of samples.

  Args:
    results: string output of iperf3 TCP multi stream output.
    sending_vm: vm where the client is run.
    receiving_vm: vm where the server is run.
    num_streams: number of TCP streams.
    internal_ip_used: for the metadata, lets the user know if it was the
                      internal or external IP used in the test.

  Returns:
    List of samples representing the results.
  """
  data_lines = [line.rstrip('\r') for line in results.split('\n')]
  data_lines = [line for line in data_lines if 'receiver' in line]

  samples = []
  for line in data_lines:
    line_data = [val for val in line.split(' ') if val]
    if line_data[0] is '[':
      line_data = line_data[1:]

    thread_id = line_data[0].rstrip(']').lstrip('[')
    metadata = {
        'protocol': 'TCP',
        'num_threads': num_streams,
        'receiving_machine_type': receiving_vm.machine_type,
        'receiving_zone': receiving_vm.zone,
        'sending_machine_type': sending_vm.machine_type,
        'sending_zone': sending_vm.zone,
        'thread_id': thread_id,
        'internal_ip_used': internal_ip_used
    }
    bandwidth = line_data[5]
    units = line_data[6]
    samples.append(
        sample.Sample('Bandwidth', float(bandwidth), units, metadata))

  return samples


# Example output from iperf3
# Connecting to host 10.129.0.3, port 5201
# [  4] local 10.129.0.4 port 49526 connected to 10.129.0.3 port 5201
# [ ID] Interval           Transfer     Bandwidth       Total Datagrams
# [  4]   0.00-1.00   sec   159 MBytes  1.34 Gbits/sec  20398
# [  4]   1.00-2.00   sec   166 MBytes  1.40 Gbits/sec  21292
# [  4]   2.00-3.00   sec   167 MBytes  1.40 Gbits/sec  21323
# - - - - - - - - - - - - - - - - - - - - - - - - -
# [ ID] Interval           Transfer     Bandwidth       Jitter    Lost/Total Dat
# [  4]   0.00-3.00   sec   492 MBytes  1.38 Gbits/sec  0.072 ms  35148/62949 (5
# [  4] Sent 62949 datagrams
#
# iperf Done.


def GetUDPStreamSamples(sending_vm, receiving_vm, results, bandwidth,
                        internal_ip_used):
  """Parses Iperf3 results and outputs samples for PKB.

  Args:
    results: string containing iperf3 output.
    bandwidth: the bandwidth used in the test
    internal_ip_used: for the metadata, lets the user know if it was the
                      internal or external IP used in the test.

  Returns:
    List of samples.
  """
  # 2 header lines, list of test results, then the 3 summary header lines
  data_line_number = 2 + FLAGS.udp_stream_seconds + 3
  data_line = results.split('\n')[data_line_number].split(' ')
  data_line = [val for val in data_line if val]
  # The data line should look like
  # [  4]   0.00-3.00   sec   492 MBytes  1.38 Gbits/sec  0.072 ms  35148/62949
  jitter = float(data_line[8])
  bandwidth_achieved = float(data_line[6])
  bandwidth_achieved_unit = data_line[7].split('/')[0]
  if bandwidth_achieved_unit == 'Gbits':
    bandwidth_achieved *= 1000.0
  if bandwidth_achieved_unit == 'Kbits':
    bandwidth_achieved /= 1000.0
  lost = int(data_line[10].split('/')[0])
  total = int(data_line[10].split('/')[1])

  metadata = {
      'protocol': 'UDP',
      'total_lost': lost,
      'total_sent': total,
      'bandwidth': bandwidth,
      'receiving_machine_type': receiving_vm.machine_type,
      'receiving_zone': receiving_vm.zone,
      'sending_machine_type': sending_vm.machine_type,
      'sending_zone': sending_vm.zone,
      'internal_ip_used': internal_ip_used
  }

  # Get the percentage of packets lost.
  loss_rate = round(lost * 100.0 / total, 3)
  samples = [
      sample.Sample('Loss Rate', loss_rate, 'Percent',
                    metadata),
      sample.Sample('Bandwidth Achieved', bandwidth_achieved, 'Mbits/sec',
                    metadata),
      sample.Sample('Jitter', jitter, 'ms',
                    metadata),
  ]
  return samples
