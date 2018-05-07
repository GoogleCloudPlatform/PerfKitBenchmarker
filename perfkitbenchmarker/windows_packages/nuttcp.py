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

"""Module containing nuttcp installation and cleanup functions."""

import ntpath

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

CONTROL_PORT = 5000
UDP_PORT = 5001
NUTTCP_OUT_FILE = 'nuttcp_results'

flags.DEFINE_integer('nuttcp_max_bandwidth_mb', 10000,
                     'The maximum bandwidth, in megabytes, to test in a '
                     'UDP stream.')

flags.DEFINE_integer('nuttcp_min_bandwidth_mb', 100,
                     'The minimum bandwidth, in megabytes, to test in a '
                     'UDP stream.')

flags.DEFINE_integer('nuttcp_bandwidth_step_mb', 1000,
                     'The amount of megabytes to increase bandwidth in each '
                     'UDP stream test.')

flags.DEFINE_integer('nuttcp_udp_stream_seconds', 10,
                     'The amount of time to run the UDP stream test.')

flags.DEFINE_integer('nuttcp_udp_packet_size', 1420,
                     'The size of each UDP packet sent in the UDP stream.')

flags.DEFINE_bool('nuttcp_udp_run_both_directions', False,
                  'Run the test twice, using each VM as a source.')

flags.DEFINE_integer('nuttcp_udp_iterations', 1,
                     'The number of consecutive tests to run.')

flags.DEFINE_bool('nuttcp_udp_unlimited_bandwidth', False,
                  'Run an "unlimited bandwidth" test')

NUTTCP_DIR = 'nuttcp-8.1.4.win64'
NUTTCP_ZIP = NUTTCP_DIR + '.zip'
NUTTCP_URL = 'http://nuttcp.net/nuttcp/nuttcp-8.1.4/binaries/' + NUTTCP_ZIP


def Install(vm):
  """Installs the nuttcp package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, NUTTCP_ZIP)
  vm.DownloadFile(NUTTCP_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def GetExecPath():
  return 'nuttcp-8.1.4.exe'


def RunNuttcp(sending_vm, receiving_vm, exec_path, dest_ip, network_type,
              iteration):
  """Run nuttcp tests.

  Args:
    sending_vm: vm sending the UDP packets.
    receiving_vm: vm receiving the UDP packets.
    exec_path: path to the nuttcp executable.
    dest_ip: the IP of the receiver.
    network_type: string representing the type of the network.
    iteration: the run number of the test.

  Returns:
    list of samples from the results of the nuttcp tests.
  """

  def _RunNuttcp(vm, options):
    command = 'cd {exec_dir}; .\\{exec_path} {options}'.format(
        exec_dir=vm.temp_dir,
        exec_path=exec_path,
        options=options)
    vm.RemoteCommand(command)

  samples = []

  bandwidths = [
      '{b}m'.format(b=b)
      for b in xrange(FLAGS.nuttcp_min_bandwidth_mb, FLAGS.
                      nuttcp_max_bandwidth_mb, FLAGS.nuttcp_bandwidth_step_mb)
  ]

  if FLAGS.nuttcp_udp_unlimited_bandwidth:
    bandwidths.append('u')

  for bandwidth in bandwidths:

    sender_args = ('-u -p{data_port} -P{control_port} -R{bandwidth} '
                   '-T{time} -l{packet_size} {dest_ip} > {out_file}').format(
                       data_port=UDP_PORT,
                       control_port=CONTROL_PORT,
                       bandwidth=bandwidth,
                       time=FLAGS.nuttcp_udp_stream_seconds,
                       packet_size=FLAGS.nuttcp_udp_packet_size,
                       dest_ip=dest_ip,
                       out_file=NUTTCP_OUT_FILE)

    receiver_args = '-p{data_port} -P{control_port} -1'.format(
        data_port=UDP_PORT,
        control_port=CONTROL_PORT)

    threaded_args = [((receiving_vm, receiver_args), {}),
                     ((sending_vm, sender_args), {})]

    vm_util.RunThreaded(_RunNuttcp, threaded_args)

    # retrieve the results and parse them
    cat_command = 'cd {nuttcp_exec_dir}; cat {out_file}'.format(
        nuttcp_exec_dir=sending_vm.temp_dir,
        out_file=NUTTCP_OUT_FILE)
    command_out, _ = sending_vm.RemoteCommand(cat_command)
    samples.append(
        GetUDPStreamSample(command_out, sending_vm, receiving_vm, bandwidth,
                           network_type, iteration))
  return samples

# 1416.3418 MB /  10.00 sec = 1188.1121 Mbps 85 %TX 26 %RX 104429 / 1554763
#  drop/pkt 6.72 %loss


def GetUDPStreamSample(command_out, sending_vm, receiving_vm, bandwidth,
                       network_type, iteration):
  """Get a sample from the nuttcp string results.

  Args:
    command_out: the nuttcp output.
    sending_vm: vm sending the UDP packets.
    receiving_vm: vm receiving the UDP packets.
    bandwidth: the requested bandwidth in the nuttcp sample.
    network_type: the type of the network, external or internal.
    iteration: the run number of the test.

  Returns:
    sample from the results of the nuttcp tests.
  """
  data_line = command_out.split('\n')[0].split(' ')
  data_line = [val for val in data_line if val]

  bandwidth = float(data_line[6])
  units = data_line[7]
  packet_loss = data_line[16]

  metadata = {
      'receiving_machine_type': receiving_vm.machine_type,
      'receiving_zone': receiving_vm.zone,
      'sending_machine_type': sending_vm.machine_type,
      'sending_zone': sending_vm.zone,
      'packet_loss': packet_loss,
      'bandwidth_requested': bandwidth,
      'network_type': network_type,
      'iteration': iteration
  }

  return sample.Sample('bandwidth', bandwidth, units, metadata)
