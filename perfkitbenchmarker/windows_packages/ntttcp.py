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

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_integer('ntttcp_threads', 1,
                     'The number of client and server threads for NTttcp '
                     'to run with.')

flags.DEFINE_integer('ntttcp_time', 60,
                     'The number of seconds for NTttcp to run.')

CONTROL_PORT = 6001
BASE_DATA_PORT = 5001
NTTTCP_RETRIES = 5
NTTTCP_DIR = 'NTttcp-v5.33'
NTTTCP_ZIP = NTTTCP_DIR + '.zip'
NTTTCP_URL = ('https://gallery.technet.microsoft.com/NTttcp-Version-528-'
              'Now-f8b12769/file/159655/1/' + NTTTCP_ZIP)


def Install(vm):
  """Installs the NTttcp package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, NTTTCP_ZIP)
  vm.DownloadFile(NTTTCP_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


@vm_util.Retry(max_retries=NTTTCP_RETRIES)
def RunNtttcp(sending_vm, receiving_vm, receiving_ip_address, ip_type):
  """Run NTttcp and return the samples collected from the run."""

  shared_options = '-xml -t {time} -p {port} '.format(time=FLAGS.ntttcp_time,
                                                      port=BASE_DATA_PORT)

  client_options = '-s -m \'{threads},*,{ip}\''.format(
      threads=FLAGS.ntttcp_threads, ip=receiving_ip_address)
  server_options = '-r -m \'{threads},*,0.0.0.0\''.format(
      threads=FLAGS.ntttcp_threads)

  ntttcp_exe_dir = ntpath.join(sending_vm.temp_dir, 'x86fre')

  # NTttcp will append to the xml file when it runs, which causes parsing
  # to fail if there was a preexisting xml file. To be safe, try deleting
  # the xml file.
  rm_command = 'cd {ntttcp_exe_dir}; rm xml.txt'.format(
      ntttcp_exe_dir=ntttcp_exe_dir)
  sending_vm.RemoteCommand(
      rm_command, ignore_failure=True, suppress_warning=True)

  def _RunNtttcp(vm, options):
    command = 'cd {ntttcp_exe_dir}; .\\NTttcp.exe {ntttcp_options}'.format(
        ntttcp_exe_dir=ntttcp_exe_dir, ntttcp_options=options)
    vm.RemoteCommand(command)

  args = [((vm, shared_options + options), {}) for vm, options in
          zip([sending_vm, receiving_vm], [client_options, server_options])]
  vm_util.RunThreaded(_RunNtttcp, args)

  cat_command = 'cd {ntttcp_exe_dir}; cat xml.txt'.format(
      ntttcp_exe_dir=ntttcp_exe_dir)
  stdout, _ = sending_vm.RemoteCommand(cat_command)

  metadata = {'ip_type': ip_type}
  for vm_specifier, vm in ('receiving', receiving_vm), ('sending', sending_vm):
    for k, v in vm.GetResourceMetadata().iteritems():
      metadata['{0}_{1}'.format(vm_specifier, k)] = v

  return ParseNtttcpResults(stdout, metadata)


def ParseNtttcpResults(xml_results, metadata):
  """Parses the xml output from NTttcp and returns a list of samples.

  The list of samples contains total throughput and per thread throughput
  metrics (if there is more than a single thread).
  """
  root = xml.etree.ElementTree.fromstring(xml_results)
  samples = []
  metadata = metadata.copy()

  for element in root.findall('parameters/*'):
    if element.tag and element.text:
      metadata[element.tag] = element.text

  # TODO(ehankland): There is more interesting metadata that we can
  # extract from the xml file such as the number of retransmits and
  # cpu usage.

  throughput_element = root.find('./throughput[@metric="mbps"]')
  samples.append(sample.Sample(
      'Total Throughput', float(throughput_element.text), 'Mbps', metadata))

  thread_elements = root.findall('./thread')
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
