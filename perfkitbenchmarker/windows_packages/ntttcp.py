# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing NTttcp installation and cleanup functions.

NTttcp is a tool made for benchmarking Windows networking.

More information about NTttcp may be found here:
https://gallery.technet.microsoft.com/NTttcp-Version-528-Now-f8b12769
"""

import collections
import ntpath
import time
import xml.etree.ElementTree

from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_integer('ntttcp_threads', 1,
                     'The number of client and server threads for NTttcp '
                     'to run with.')

flags.DEFINE_integer('ntttcp_time', 60,
                     'The number of seconds for NTttcp to run.')

flags.DEFINE_bool('ntttcp_udp', False, 'Whether to run a UDP test.')

flags.DEFINE_integer('ntttcp_cooldown_time', 60,
                     'Time to wait between the test runs.')

flags.DEFINE_integer('ntttcp_packet_size', None,
                     'The size of the packet being used in the test.')

flags.DEFINE_integer('ntttcp_sender_sb', -1,
                     'The size of the send buffer, in Kilo Bytes, on the '
                     'sending VM. The default is the OS default.')

flags.DEFINE_integer('ntttcp_sender_rb', -1,
                     'The size of the receive buffer, in Kilo Bytes, on the '
                     'sending VM. The default is the OS default.')

flags.DEFINE_integer('ntttcp_receiver_sb', -1,
                     'The size of the send buffer, in Kilo Bytes, on the '
                     'receiving VM. The default is the OS default.')

flags.DEFINE_integer('ntttcp_receiver_rb', -1,
                     'The size of the receive buffer, in Kilo Bytes, on the '
                     'receiving VM. The default is the OS default.')

flags.DEFINE_list(
    'ntttcp_config_list', '',
    'comma separated list of configs to run with ntttcp. The '
    'format for a single config is UDP:THREADS:RUNTIME_S:IP_TYPE:PACKET_SIZE, '
    'for example True:4:60:INTERNAL:0,False:8:60:EXTERNAL:150')

# When adding new configs to ntttcp_config_list, increase this value
_NUM_PARAMS_IN_CONFIG = 5

CONTROL_PORT = 6001
BASE_DATA_PORT = 5001
NTTTCP_RETRIES = 10
NTTTCP_DIR = 'NTttcp-v5.33'
NTTTCP_ZIP = NTTTCP_DIR + '.zip'
NTTTCP_URL = ('https://gallery.technet.microsoft.com/NTttcp-Version-528-'
              'Now-f8b12769/file/159655/1/' + NTTTCP_ZIP)

TRUE_VALS = ['True', 'true', 't']
FALSE_VALS = ['False', 'false', 'f']

# named tuple used in passing configs around
NtttcpConf = collections.namedtuple('NtttcpConf',
                                    'udp threads time_s ip_type packet_size')


def NtttcpConfigListValidator(value):
  """Returns whether or not the config list flag is valid."""
  if len(value) == 1 and not value[0]:
    # not using the config list here
    return True
  for config in value:
    config_vals = config.split(':')
    if len(config_vals) < _NUM_PARAMS_IN_CONFIG:
      return False

    try:
      udp = config_vals[0]
      threads = int(config_vals[1])
      time_s = int(config_vals[2])
      ip_type = config_vals[3]
      packet_size = int(config_vals[4])
    except ValueError:
      return False

    if udp not in TRUE_VALS + FALSE_VALS:
      return False

    if threads < 1:
      return False

    if time_s < 1:
      return False

    if packet_size < 0:
      return False

    # verify the ip type
    if ip_type not in [
        vm_util.IpAddressSubset.EXTERNAL, vm_util.IpAddressSubset.INTERNAL
    ]:
      return False

  return True


flags.register_validator('ntttcp_config_list', NtttcpConfigListValidator,
                         'malformed config list')


def ParseConfigList():
  """Get the list of configs for the test from the flags."""
  if not FLAGS.ntttcp_config_list:
    # config is the empty string.
    return [
        NtttcpConf(
            udp=FLAGS.ntttcp_udp,
            threads=FLAGS.ntttcp_threads,
            time_s=FLAGS.ntttcp_time,
            ip_type=FLAGS.ip_addresses,
            packet_size=FLAGS.ntttcp_packet_size)
    ]

  conf_list = []
  for config in FLAGS.ntttcp_config_list:
    confs = config.split(':')

    conf_list.append(
        NtttcpConf(
            udp=(confs[0] in TRUE_VALS),
            threads=int(confs[1]),
            time_s=int(confs[2]),
            ip_type=confs[3],
            packet_size=int(confs[4])))

  return conf_list


def Install(vm):
  """Installs the NTttcp package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, NTTTCP_ZIP)
  vm.DownloadFile(NTTTCP_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


@vm_util.Retry(poll_interval=60, fuzz=1, max_retries=NTTTCP_RETRIES)
def _TaskKillNtttcp(vm):
  kill_command = 'taskkill /IM ntttcp /F'
  vm.RemoteCommand(kill_command, ignore_failure=True, suppress_warning=True)


def _RunNtttcp(vm, options):
  timeout_duration = 3 * FLAGS.ntttcp_time
  ntttcp_exe_dir = ntpath.join(vm.temp_dir, 'x86fre')
  command = 'cd {ntttcp_exe_dir}; .\\NTttcp.exe {ntttcp_options}'.format(
      ntttcp_exe_dir=ntttcp_exe_dir, ntttcp_options=options)
  vm.RobustRemoteCommand(command, timeout=timeout_duration)


def _RemoveXml(vm):
  ntttcp_exe_dir = ntpath.join(vm.temp_dir, 'x86fre')
  rm_command = 'cd {ntttcp_exe_dir}; rm xml.txt'.format(
      ntttcp_exe_dir=ntttcp_exe_dir)
  vm.RemoteCommand(rm_command, ignore_failure=True, suppress_warning=True)


def _CatXml(vm):
  ntttcp_exe_dir = ntpath.join(vm.temp_dir, 'x86fre')
  cat_command = 'cd {ntttcp_exe_dir}; cat xml.txt'.format(
      ntttcp_exe_dir=ntttcp_exe_dir)
  ntttcp_xml, _ = vm.RemoteCommand(cat_command)
  return ntttcp_xml


def _GetSockBufferSize(sock_buff_size):
  return '%dK' % sock_buff_size if sock_buff_size != -1 else sock_buff_size


@vm_util.Retry(max_retries=NTTTCP_RETRIES)
def RunNtttcp(sending_vm, receiving_vm, receiving_ip_address, ip_type, udp,
              threads, time_s, packet_size, cooldown):
  """Run NTttcp and return the samples collected from the run."""

  if cooldown:
    time.sleep(FLAGS.ntttcp_cooldown_time)

  # Clean up any stray ntttcp processes in case this is retry.
  _TaskKillNtttcp(sending_vm)
  _TaskKillNtttcp(receiving_vm)

  packet_size_string = ''
  if packet_size:
    packet_size_string = ' -l %d ' % packet_size

  shared_options = '-xml -t {time} -p {port} {packet_size}'.format(
      time=time_s, port=BASE_DATA_PORT, packet_size=packet_size_string)

  udp_string = '-u' if udp else ''
  sending_options = shared_options + (
      '-s {udp} -m \'{threads},*,{ip}\' -rb {rb} -sb {sb}').format(
          udp=udp_string,
          threads=threads,
          ip=receiving_ip_address,
          rb=_GetSockBufferSize(FLAGS.ntttcp_sender_rb),
          sb=_GetSockBufferSize(FLAGS.ntttcp_sender_sb))
  receiving_options = shared_options + (
      '-r {udp} -m \'{threads},*,0.0.0.0\' -rb {rb} -sb {sb}').format(
          udp=udp_string,
          threads=threads,
          rb=_GetSockBufferSize(FLAGS.ntttcp_receiver_rb),
          sb=_GetSockBufferSize(FLAGS.ntttcp_receiver_sb))

  # NTttcp will append to the xml file when it runs, which causes parsing
  # to fail if there was a preexisting xml file. To be safe, try deleting
  # the xml file.
  _RemoveXml(sending_vm)
  _RemoveXml(receiving_vm)

  process_args = [(_RunNtttcp, (sending_vm, sending_options), {}),
                  (_RunNtttcp, (receiving_vm, receiving_options), {})]

  background_tasks.RunParallelProcesses(process_args, 200)

  sender_xml = _CatXml(sending_vm)
  receiver_xml = _CatXml(receiving_vm)

  metadata = {'ip_type': ip_type}
  for vm_specifier, vm in ('receiving', receiving_vm), ('sending', sending_vm):
    for k, v in vm.GetResourceMetadata().iteritems():
      metadata['{0}_{1}'.format(vm_specifier, k)] = v

  return ParseNtttcpResults(sender_xml, receiver_xml, metadata)


def ParseNtttcpResults(sender_xml_results, receiver_xml_results, metadata):
  """Parses the xml output from NTttcp and returns a list of samples.

  The list of samples contains total throughput and per thread throughput
  metrics (if there is more than a single thread).

  Args:
    sender_xml_results: ntttcp test output from the sender.
    receiver_xml_results: ntttcp test output from the receiver.
    metadata: metadata to be included as part of the samples.

  Returns:
    list of samples from the results of the ntttcp tests.
  """
  sender_xml_root = xml.etree.ElementTree.fromstring(sender_xml_results)
  receiver_xml_root = xml.etree.ElementTree.fromstring(receiver_xml_results)
  samples = []
  metadata = metadata.copy()

  # Get the parameters from the sender XML output, but skip the throughput and
  # thread info. Those will be used in the samples, not the metadata.
  for item in list(sender_xml_root):
    if item.tag == 'parameters':
      for param in list(item):
        metadata[param.tag] = param.text
    elif item.tag == 'throughput' or item.tag == 'thread':
      continue
    else:
      metadata['sender %s' % item.tag] = item.text

  # We do not want the parameters from the receiver (we already got those
  # from the sender), but we do want everything else and have it marked as
  # coming from the receiver.
  for item in list(receiver_xml_root):
    if item.tag == 'parameters' or item.tag == 'thread':
      continue
    elif item.tag == 'throughput':
      if item.attrib['metric'] == 'mbps':
        metadata['receiver throughput'] = item.text
    else:
      metadata['receiver %s' % item.tag] = item.text

  metadata['sender rb'] = FLAGS.ntttcp_sender_rb
  metadata['sender sb'] = FLAGS.ntttcp_sender_rb
  metadata['receiver rb'] = FLAGS.ntttcp_receiver_rb
  metadata['receiver sb'] = FLAGS.ntttcp_receiver_sb

  throughput_element = sender_xml_root.find('./throughput[@metric="mbps"]')
  samples.append(
      sample.Sample('Total Throughput', float(throughput_element.text), 'Mbps',
                    metadata))

  thread_elements = sender_xml_root.findall('./thread')
  if len(thread_elements) > 1:
    for element in thread_elements:
      throughput_element = element.find('./throughput[@metric="mbps"]')
      metadata = metadata.copy()
      metadata['thread_index'] = element.attrib['index']
      samples.append(sample.Sample('Thread Throughput',
                                   float(throughput_element.text),
                                   'Mbps',
                                   metadata))
  return samples
