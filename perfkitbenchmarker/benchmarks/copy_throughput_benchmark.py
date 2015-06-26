# Copyright 2014 Google Inc. All rights reserved.
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

"""Runs copy benchmarks.

cp and dd between two attached disks on same vm.
scp copy across different vms using external networks.
"""
import logging
import posixpath

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flags.DEFINE_enum('copy_benchmark_mode', 'cp', ['cp', 'dd', 'scp'],
                  'Runs either cp, dd or scp tests.')

FLAGS = flags.FLAGS

# 'num_machines' set in GetInfo, as it depends on 'copy_benchmark_mode'.
BENCHMARK_INFO = {'name': 'copy_throughput',
                  'description': 'Get cp and scp performance between vms.',
                  'scratch_disk': True,
                  'num_machines': None}

CIPHER = 'aes128-cbc'

DATA_FILE = 'cloud-storage-workload.sh'
# size of all data
DATA_SIZE_IN_MB = 256.1
# size of scratch disk
DISK_SIZE_IN_GB = 500
# Unit for all benchmarks
UNIT = 'MB/sec'


def GetInfo():
  """Decide number of vms needed and return infomation for copy benchmark."""
  info = BENCHMARK_INFO.copy()
  if FLAGS.copy_benchmark_mode != 'scp':
    info['num_machines'] = 1
    info['scratch_disk'] = 2
  else:
    info['num_machines'] = 2
  return info


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(DATA_FILE)


def PrepareDataFile(vm):
  """Generate data file on vm to destination directory.

  Args:
    vm: The VM needs data file.
  """
  file_path = data.ResourcePath(DATA_FILE)
  vm.PushFile(file_path, '%s/' % vm.GetScratchDir(0))
  vm.RemoteCommand('cd %s/; bash cloud-storage-workload.sh'
                   % vm.GetScratchDir(0))


def PreparePrivateKey(vm):
  if not vm.has_private_key:
    vm.PushFile(vm_util.GetPrivateKeyPath(),
                linux_virtual_machine.REMOTE_KEY_PATH)
    vm.has_private_key = True


def Prepare(benchmark_spec):
  """Prepare vms with additional scratch disks and create vms for scp.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(PreparePrivateKey, vms)
  vm_util.RunThreaded(PrepareDataFile, vms)


def RunCp(vms):
  """Runs cp benchmarks and parses results.

  Args:
    vms: The VMs running cp benchmarks.

  Returns:
    A list of sample.Sample objects.
  """
  cmd = ('rm %s/*; sudo sync; sudo sysctl vm.drop_caches=3; '
         'time cp %s/data/* %s/; ' %
         (vms[0].GetScratchDir(1), vms[0].GetScratchDir(0),
          vms[0].GetScratchDir(1)))
  _, res = vms[0].RemoteCommand(cmd)
  logging.info(res)
  time_used = vm_util.ParseTimeCommandResult(res)
  return [sample.Sample('cp throughput', DATA_SIZE_IN_MB / time_used, UNIT)]


def RunDd(vms):
  """Run dd benchmark and parses results.

  Args:
    vms: The VMs running dd benchmarks.

  Returns:
    A list of samples. Each sample is a 4-tuple of (benchmark_name, value, unit,
    metadata), as accepted by PerfKitBenchmarkerPublisher.AddSamples.
  """
  vm = vms[0]
  cmd = ('rm -rf %s/*; sudo sync; sudo sysctl vm.drop_caches=3; '
         'time for i in {0..99}; do dd if=%s/data/file-$i.dat '
         'of=%s/file-$i.dat bs=262144; done' %
         (vm.GetScratchDir(1), vm.GetScratchDir(0),
          vm.GetScratchDir(1)))
  _, res = vm.RemoteCommand(cmd)
  logging.info(res)
  time_used = vm_util.ParseTimeCommandResult(res)
  return [sample.Sample('dd throughput', DATA_SIZE_IN_MB / time_used, UNIT)]


def RunScp(vms):
  """Run scp benchmark.

  Args:
    vms: The vms running scp commands.

  Returns:
    A list of samples. Each sample is a 4-tuple of (benchmark_name, value, unit,
    metadata), as accepted by PerfKitBenchmarkerPublisher.AddSamples.
  """
  result = RunScpSingleDirection(vms[0], vms[1])
  result += RunScpSingleDirection(vms[1], vms[0])
  return result


MODE_FUNCTION_DICTIONARY = {
    'cp': RunCp,
    'dd': RunDd,
    'scp': RunScp}


def RunScpSingleDirection(sending_vm, receiving_vm):
  """Run scp from sending_vm to receiving_vm and parse results.

  If 'receiving_vm' is accessible via internal IP from 'sending_vm', throughput
  over internal IP addresses will be tested in addition to external IP
  addresses.

  Args:
    sending_vm: The originating VM for the scp command.
    receiving_vm: The destination VM for the scp command.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  metadata = {
      'sending_zone': sending_vm.zone,
      'receiving_zone': receiving_vm.zone,
      'server_machine_type': receiving_vm.machine_type,
      'client_machine_type': sending_vm.machine_type,
  }

  cmd_template = ('sudo sync; sudo sysctl vm.drop_caches=3; '
                  'time /usr/bin/scp -o StrictHostKeyChecking=no -i %s -c %s '
                  '%s %s@%%s:%%s/;') % (
                      linux_virtual_machine.REMOTE_KEY_PATH, CIPHER,
                      '%s/data/*' % sending_vm.GetScratchDir(0),
                      receiving_vm.user_name)

  def RunForIpAddress(ip_address, ip_type):
    """Run SCP benchmark against a destination IP address."""
    target_dir = posixpath.join(receiving_vm.GetScratchDir(0), ip_type)
    cmd = cmd_template % (ip_address, target_dir)
    receiving_vm.RemoteCommand('mkdir %s' % target_dir)
    meta = metadata.copy()
    meta['ip_type'] = ip_type
    _, res = sending_vm.RemoteCommand(cmd)
    time_used = vm_util.ParseTimeCommandResult(res)
    result = DATA_SIZE_IN_MB / time_used
    receiving_vm.RemoteCommand('rm -rf %s' % target_dir)
    return sample.Sample('scp throughput', result, UNIT, meta)

  if vm_util.ShouldRunOnExternalIpAddress():
    results.append(RunForIpAddress(receiving_vm.ip_address, 'external'))

  if vm_util.ShouldRunOnInternalIpAddress(sending_vm, receiving_vm):
    results.append(RunForIpAddress(receiving_vm.internal_ip, 'internal'))

  return results


def Run(benchmark_spec):
  """Run cp/scp on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of throughput samples. Each sample contains
      the sample metric (string), value (float), unit (string), and metadata
      (dict).
  """
  vms = benchmark_spec.vms

  results = MODE_FUNCTION_DICTIONARY[FLAGS.copy_benchmark_mode](vms)

  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
