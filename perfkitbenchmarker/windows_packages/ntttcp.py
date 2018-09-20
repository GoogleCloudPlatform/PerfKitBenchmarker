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

import ntpath
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

flags.DEFINE_integer('ntttcp_packet_size', None,
                     'The size of the packet being used in the test.')

CONTROL_PORT = 6001
BASE_DATA_PORT = 5001
NTTTCP_RETRIES = 10
NTTTCP_DIR = 'NTttcp-v5.33'
NTTTCP_ZIP = NTTTCP_DIR + '.zip'
NTTTCP_URL = ('https://gallery.technet.microsoft.com/NTttcp-Version-528-'
              'Now-f8b12769/file/159655/1/' + NTTTCP_ZIP)


def Install(vm):
  """Installs the NTttcp package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, NTTTCP_ZIP)
  vm.DownloadFile(NTTTCP_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def _RunNtttcp(vm, options):
  timeout_duration = 3 * FLAGS.ntttcp_time
  ntttcp_exe_dir = ntpath.join(vm.temp_dir, 'x86fre')
  command = 'cd {ntttcp_exe_dir}; .\\NTttcp.exe {ntttcp_options}'.format(
      ntttcp_exe_dir=ntttcp_exe_dir, ntttcp_options=options)
  vm.RemoteCommand(command, timeout=timeout_duration)


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


@vm_util.Retry(max_retries=NTTTCP_RETRIES)
def RunNtttcp(sending_vm, receiving_vm, receiving_ip_address, ip_type):
  """Run NTttcp and return the samples collected from the run."""

  packet_size_string = ''
  if FLAGS.ntttcp_packet_size:
    packet_size_string = ' -l %d ' % FLAGS.ntttcp_packet_size

  shared_options = '-xml -t {time} -p {port} {packet_size}'.format(
      time=FLAGS.ntttcp_time,
      port=BASE_DATA_PORT,
      packet_size=packet_size_string)

  udp_string = '-u' if FLAGS.ntttcp_udp else ''
  sending_options = shared_options + '-s {udp} -m \'{threads},*,{ip}\''.format(
      udp=udp_string, threads=FLAGS.ntttcp_threads, ip=receiving_ip_address)
  receiving_options = shared_options + (
      '-r {udp} -m \'{threads},*,0.0.0.0\'').format(
          udp=udp_string, threads=FLAGS.ntttcp_threads)

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
