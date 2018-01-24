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

flags.DEFINE_integer('max_bandwidth_mb', 5000,
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

IPERF3_DIR = 'iperf-3.1.3-win64'
IPERF3_ZIP = IPERF3_DIR + '.zip'
IPERF3_URL = 'http://iperf.fr/download/windows/' + IPERF3_ZIP

IPERF3_OUT_FILE = 'iperf_results'
IPERF3_UDP_PORT = 5201


def Install(vm):
  zip_path = ntpath.join(vm.temp_dir, IPERF3_ZIP)
  vm.DownloadFile(IPERF3_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


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
    samples.extend(GetUDPStreamSamples(command_out, bandwidth, use_internal_ip))

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


def GetUDPStreamSamples(results, bandwidth, internal_ip_used):
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
      'total_lost': lost,
      'total_sent': total,
      'bandwidth': bandwidth,
      'internal_ip_used': internal_ip_used
  }

  # Get the percentage of packets lost.
  loss_rate = round(lost*100.0/total, 3)
  samples = [
      sample.Sample('Loss Rate', loss_rate, 'Percent',
                    metadata),
      sample.Sample('Bandwidth Achieved', bandwidth_achieved, 'Mbits/sec',
                    metadata),
      sample.Sample('Jitter', jitter, 'ms',
                    metadata),
  ]
  return samples
