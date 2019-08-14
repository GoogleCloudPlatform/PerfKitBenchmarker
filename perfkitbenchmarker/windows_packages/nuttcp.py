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

import multiprocessing
import ntpath

from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

CONTROL_PORT = 5000
UDP_PORT = 5001
NUTTCP_OUT_FILE = 'nuttcp_results'
CPU_OUT_FILE = 'cpu_results'

flags.DEFINE_integer('nuttcp_max_bandwidth_mb', 10000,
                     'The maximum bandwidth, in megabytes, to test in a '
                     'UDP stream.')

flags.DEFINE_integer('nuttcp_min_bandwidth_mb', 100,
                     'The minimum bandwidth, in megabytes, to test in a '
                     'UDP stream.')

flags.DEFINE_integer('nuttcp_bandwidth_step_mb', 1000,
                     'The amount of megabytes to increase bandwidth in each '
                     'UDP stream test.')

flags.DEFINE_integer('nuttcp_udp_stream_seconds', 60,
                     'The amount of time to run the UDP stream test.')

flags.DEFINE_integer('nuttcp_udp_packet_size', 1420,
                     'The size of each UDP packet sent in the UDP stream.')

flags.DEFINE_bool('nuttcp_udp_run_both_directions', False,
                  'Run the test twice, using each VM as a source.')

flags.DEFINE_integer('nuttcp_udp_iterations', 1,
                     'The number of consecutive tests to run.')

flags.DEFINE_bool('nuttcp_udp_unlimited_bandwidth', False,
                  'Run an "unlimited bandwidth" test')

flags.DEFINE_integer('nuttcp_cpu_sample_time', 3,
                     'Time, in seconds, to take the CPU usage sample.')

NUTTCP_DIR = 'nuttcp-8.1.4.win64'
NUTTCP_ZIP = NUTTCP_DIR + '.zip'
NUTTCP_URL = 'https://nuttcp.net/nuttcp/nuttcp-8.1.4/binaries/' + NUTTCP_ZIP

_COMMAND_TIMEOUT_BUFFER = 30


class NuttcpNotRunningError(Exception):
  """Raised when nuttcp is not running at a time that it is expected to be."""


def Install(vm):
  """Installs the nuttcp package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, NUTTCP_ZIP)
  vm.DownloadFile(NUTTCP_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def CheckPrerequisites():
  if FLAGS.nuttcp_udp_stream_seconds <= FLAGS.nuttcp_cpu_sample_time:
    raise errors.Config.InvalidValue(
        'nuttcp_udp_stream_seconds must be greater than nuttcp_cpu_sample_time')


def GetExecPath():
  return 'nuttcp-8.1.4.exe'


def _RunNuttcp(vm, options, exec_path):
  """Run nuttcp, server or client depending on options.

  Args:
    vm: vm to run nuttcp on
    options: string of options to pass to nuttcp
    exec_path: string path to the nuttcp executable
  """
  command = 'cd {exec_dir}; .\\{exec_path} {options}'.format(
      exec_dir=vm.temp_dir,
      exec_path=exec_path,
      options=options)
  # Timeout after expected duration, 5sec server wait plus 25sec buffer
  timeout_duration = FLAGS.nuttcp_udp_stream_seconds + _COMMAND_TIMEOUT_BUFFER
  vm.RobustRemoteCommand(command, timeout=timeout_duration)


def _GetCpuUsage(vm):
  """Gather CPU usage data.

  Args:
    vm: the vm to gather cpu usage data on.

  Raises:
    NuttcpNotRunningError: raised if nuttcp is not running when the CPU usage
                           data gathering has finished.
  """
  command = ('cd {exec_path}; '
             "Get-Counter -Counter '\\Processor(*)\\% Processor Time' "
             '-SampleInterval {sample_time} | '
             'select -ExpandProperty CounterSamples | '
             'select InstanceName,CookedValue > {out_file};'
             'if ((ps | select-string nuttcp | measure-object).Count -eq 0) '
             '{{echo "FAIL"}}').format(
                 exec_path=vm.temp_dir,
                 sample_time=FLAGS.nuttcp_cpu_sample_time,
                 out_file=CPU_OUT_FILE)
  # returning from the command should never take longer than 30 seconds over
  # the actual sample time. If it does, it is hung.
  timeout_duration = FLAGS.nuttcp_cpu_sample_time + _COMMAND_TIMEOUT_BUFFER
  stdout, _ = vm.RemoteCommand(command, timeout=timeout_duration)
  if 'FAIL' in stdout:
    raise NuttcpNotRunningError('nuttcp not running after getting CPU usage.')


@vm_util.Retry(max_retries=3)
def RunSingleBandwidth(bandwidth, sending_vm, receiving_vm, dest_ip, exec_path):
  """Create a server-client nuttcp pair.

  The server exits after the client completes its request.

  Args:
    bandwidth: the requested transmission bandwidth
    sending_vm: vm sending the UDP packets.
    receiving_vm: vm receiving the UDP packets.
    dest_ip: the IP of the receiver.
    exec_path: path to the nuttcp executable.

  Returns:
    output from the client nuttcp process.
  """
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

  # Process to run the nuttcp server
  server_process = multiprocessing.Process(
      name='server',
      target=_RunNuttcp,
      args=(receiving_vm, receiver_args, exec_path))
  server_process.start()

  receiving_vm.WaitForProcessRunning('nuttcp', 30)

  # Process to run the nuttcp client
  client_process = multiprocessing.Process(
      name='client',
      target=_RunNuttcp,
      args=(sending_vm, sender_args, exec_path))
  client_process.start()

  sending_vm.WaitForProcessRunning('nuttcp', 30)

  process_args = [
      (_GetCpuUsage, (receiving_vm,), {}),
      (_GetCpuUsage, (sending_vm,), {})]

  background_tasks.RunParallelProcesses(process_args, 200)

  server_process.join()
  client_process.join()


@vm_util.Retry(max_retries=3)
def GatherResults(vm, out_file):
  """Gets the contents of out_file from vm.

  Args:
    vm: the VM to get the results from.
    out_file: the name of the file that contains results.

  Returns:
    The contents of 'out_file' as a string.
  """
  cat_command = 'cd {results_dir}; cat {out_file}'
  results_command = cat_command.format(results_dir=vm.temp_dir,
                                       out_file=out_file)
  results, _ = vm.RemoteCommand(results_command)
  return results


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

  samples = []

  bandwidths = [
      '{b}m'.format(b=b)
      for b in xrange(
          FLAGS.nuttcp_min_bandwidth_mb,
          FLAGS.nuttcp_max_bandwidth_mb,
          FLAGS.nuttcp_bandwidth_step_mb)
  ]

  if FLAGS.nuttcp_udp_unlimited_bandwidth:
    bandwidths.append('u')

  for bandwidth in bandwidths:

    RunSingleBandwidth(bandwidth, sending_vm, receiving_vm, dest_ip, exec_path)

    # retrieve the results and parse them
    udp_results = GatherResults(sending_vm, NUTTCP_OUT_FILE)

    # get the cpu usage for the sender
    sender_cpu_results = GatherResults(sending_vm, CPU_OUT_FILE)

    # get the cpu usage for the receiver
    receiving_cpu_results = GatherResults(receiving_vm, CPU_OUT_FILE)

    samples.append(
        GetUDPStreamSample(udp_results, sender_cpu_results,
                           receiving_cpu_results, sending_vm, receiving_vm,
                           bandwidth, network_type, iteration))

  return samples


def _GetCpuResults(cpu_results):
  r"""Transforms the string output of the cpu results.

  Sample output:
  '\r\n
  InstanceName      CookedValue\r\n
  ------------      -----------\r\n
  0            22.7976893740141\r\n
  1            32.6422793196096\r\n
  2            18.6525988706054\r\n
  3            44.5594145169094\r\n
  _total       29.6629938622484\r\n
  \r\n
  \r\n'

  Args:
    cpu_results: string of the output of the cpu usage command.
  Returns:
    Array of (cpu_num, percentage)
  """
  results = []
  for entry in (line for line in cpu_results.splitlines()[3:] if line):
    cpu_num, cpu_usage = entry.split()
    results.append((cpu_num, float(cpu_usage)))
  return results


# 1416.3418 MB /  10.00 sec = 1188.1121 Mbps 85 %TX 26 %RX 104429 / 1554763
#  drop/pkt 6.72 %loss


def GetUDPStreamSample(command_out, sender_cpu_results, receiving_cpu_results,
                       sending_vm, receiving_vm, request_bandwidth,
                       network_type, iteration):
  """Get a sample from the nuttcp string results.

  Args:
    command_out: the nuttcp output.
    sender_cpu_results: the cpu usage of the sender VM
    receiving_cpu_results: the cpu usage of the sender VM
    sending_vm: vm sending the UDP packets.
    receiving_vm: vm receiving the UDP packets.
    request_bandwidth: the requested bandwidth in the nuttcp sample.
    network_type: the type of the network, external or internal.
    iteration: the run number of the test.

  Returns:
    sample from the results of the nuttcp tests.
  """

  data_line = command_out.split('\n')[0].split(' ')
  data_line = [val for val in data_line if val]

  actual_bandwidth = float(data_line[6])
  units = data_line[7]
  packet_loss = data_line[16]

  metadata = {
      'receiving_machine_type': receiving_vm.machine_type,
      'receiving_zone': receiving_vm.zone,
      'sending_machine_type': sending_vm.machine_type,
      'sending_zone': sending_vm.zone,
      'packet_loss': packet_loss,
      'bandwidth_requested': request_bandwidth,
      'network_type': network_type,
      'packet_size': FLAGS.nuttcp_udp_packet_size,
      'sample_time': FLAGS.nuttcp_udp_stream_seconds,
      'iteration': iteration,
  }

  for cpu_usage in _GetCpuResults(sender_cpu_results):
    metadata['sender cpu %s' % cpu_usage[0]] = cpu_usage[1]

  for cpu_usage in _GetCpuResults(receiving_cpu_results):
    metadata['receiver cpu %s' % cpu_usage[0]] = cpu_usage[1]

  return sample.Sample('bandwidth', actual_bandwidth, units, metadata)
