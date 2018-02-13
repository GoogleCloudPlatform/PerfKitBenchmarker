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

"""Runs copy benchmarks.

cp and dd between two attached disks on same vm.
scp copy across different vms using external networks.
"""
import logging
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.configs import option_decoders


class CopyThroughputBenchmarkSpec(
    benchmark_config_spec.BenchmarkConfigSpec):

  def __init__(self, component_full_name, **kwargs):
    self.data_size_in_mb = None
    super(CopyThroughputBenchmarkSpec, self).__init__(
        component_full_name, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(CopyThroughputBenchmarkSpec,
                   cls)._GetOptionDecoderConstructions()
    result.update({
        'data_size_in_mb': (option_decoders.FloatDecoder, {'default': None}),
    })
    return result

flags.DEFINE_enum('copy_benchmark_mode', 'cp', ['cp', 'dd', 'scp'],
                  'Runs either cp, dd or scp tests.')
flags.DEFINE_integer('copy_benchmark_single_file_mb', None, 'If set, a '
                     'single file of the specified number of MB is used '
                     'instead of the normal cloud-storage-workload.sh basket '
                     'of files.  Not supported when copy_benchmark_mode is dd')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'copy_throughput'
BENCHMARK_CONFIG = """
copy_throughput:
  description: Get cp and scp performance between vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      disk_count: 2
      vm_count: 1
"""

BENCHMARK_CONFIG_SPEC_CLASS = CopyThroughputBenchmarkSpec

# Preferred SCP ciphers, in order of preference:
CIPHERS = ['aes128-cbc', 'aes128-ctr']

DATA_FILE = 'cloud-storage-workload.sh'
# size of default data
DEFAULT_DATA_SIZE_IN_MB = 256.1
# Unit for all benchmarks
UNIT = 'MB/sec'


def GetConfig(user_config):
  """Decide number of vms needed and return infomation for copy benchmark."""

  if FLAGS.copy_benchmark_mode == 'dd' and FLAGS.copy_benchmark_single_file_mb:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Flag copy_benchmark_single_file_mb is not supported when flag '
        'copy_benchmark_mode is dd.')

  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.copy_benchmark_mode == 'scp':
    config['vm_groups']['default']['vm_count'] = 2
    config['vm_groups']['default']['disk_count'] = 1
  if FLAGS.copy_benchmark_single_file_mb:
    config['data_size_in_mb'] = FLAGS.copy_benchmark_single_file_mb
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
   benchmark_config: Unused
  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config  # unused
  data.ResourcePath(DATA_FILE)


def PrepareDataFile(vm, data_size_in_mb):
  """Generate data file on vm to destination directory.

  Args:
    vm: The VM which needs the data file.
    data_size_in_mb: The size of the data file in MB.
  """
  file_path = data.ResourcePath(DATA_FILE)
  vm.PushFile(file_path, '%s/' % vm.GetScratchDir(0))
  if data_size_in_mb:
    vm.RemoteCommand('cd %s/; bash cloud-storage-workload.sh single_file %s'
                     % (vm.GetScratchDir(0), data_size_in_mb))
  else:
    vm.RemoteCommand('cd %s/; bash cloud-storage-workload.sh'
                     % vm.GetScratchDir(0))


def PreparePrivateKey(vm):
  vm.AuthenticateVm()


def Prepare(benchmark_spec):
  """Prepare vms with additional scratch disks and create vms for scp.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(PreparePrivateKey, vms)

  args = [((vm, benchmark_spec.config.data_size_in_mb), {})
          for vm in benchmark_spec.vms]
  vm_util.RunThreaded(PrepareDataFile, args)


def RunCp(vms, data_size_in_mb, metadata):
  """Runs cp benchmarks and parses results.

  Args:
    vms: The VMs running cp benchmarks.
    data_size_in_mb: The size of the data file in MB.
    metadata: The base metadata to attach to the sample.

  Returns:
    A list of sample.Sample objects.
  """
  cmd = ('rm -rf %s/*; sudo sync; sudo sysctl vm.drop_caches=3; '
         'time cp %s/data/* %s/; ' %
         (vms[0].GetScratchDir(1), vms[0].GetScratchDir(0),
          vms[0].GetScratchDir(1)))
  _, res = vms[0].RemoteCommand(cmd)
  logging.info(res)
  time_used = vm_util.ParseTimeCommandResult(res)
  return [sample.Sample('cp throughput', data_size_in_mb / time_used, UNIT,
                        metadata=metadata)]


def RunDd(vms, data_size_in_mb, metadata):
  """Run dd benchmark and parses results.

  Args:
    vms: The VMs running dd benchmarks.
    data_size_in_mb: The size of the data file in MB.
    metadata: The metadata to attach to the sample.

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
  return [sample.Sample('dd throughput', data_size_in_mb / time_used, UNIT,
                        metadata=metadata)]


def AvailableCiphers(vm):
  """Returns the set of ciphers accepted by the vm's SSH server."""
  ciphers, _ = vm.RemoteCommand('sshd -T | grep ^ciphers ')
  return set(ciphers.split()[1].split(','))


def ChooseSshCipher(vms):
  """Returns the most-preferred cipher that's available to all vms."""
  available = reduce(lambda a, b: a & b, [AvailableCiphers(vm) for vm in vms])
  for cipher in CIPHERS:
    if cipher in available:
      return cipher
  raise Exception('None of the preferred ciphers (%s) are available (%s).'
                  % (CIPHERS, available))


def RunScp(vms, data_size_in_mb, metadata):
  """Run scp benchmark.

  Args:
    vms: The vms running scp commands.
    data_size_in_mb: The size of the data file in MB.
    metadata: The metadata to attach to the sample.

  Returns:
    A list of samples. Each sample is a 4-tuple of (benchmark_name, value, unit,
    metadata), as accepted by PerfKitBenchmarkerPublisher.AddSamples.
  """
  cipher = ChooseSshCipher(vms)
  result = RunScpSingleDirection(
      vms[0], vms[1], cipher, data_size_in_mb, metadata)
  result += RunScpSingleDirection(
      vms[1], vms[0], cipher, data_size_in_mb, metadata)
  return result


MODE_FUNCTION_DICTIONARY = {
    'cp': RunCp,
    'dd': RunDd,
    'scp': RunScp}


def RunScpSingleDirection(sending_vm, receiving_vm, cipher,
                          data_size_in_mb, base_metadata):
  """Run scp from sending_vm to receiving_vm and parse results.

  If 'receiving_vm' is accessible via internal IP from 'sending_vm', throughput
  over internal IP addresses will be tested in addition to external IP
  addresses.

  Args:
    sending_vm: The originating VM for the scp command.
    receiving_vm: The destination VM for the scp command.
    cipher: Name of the SSH cipher to use.
    data_size_in_mb: The size of the data file in MB.
    base_metadata: The base metadata to attach to the sample.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  metadata = base_metadata.copy()
  for vm_specifier, vm in ('receiving', receiving_vm), ('sending', sending_vm):
    for k, v in vm.GetResourceMetadata().iteritems():
      metadata['{0}_{1}'.format(vm_specifier, k)] = v

  cmd_template = ('sudo sync; sudo sysctl vm.drop_caches=3; '
                  'time /usr/bin/scp -o StrictHostKeyChecking=no -i %s -c %s '
                  '%s %s@%%s:%%s/;') % (
                      linux_virtual_machine.REMOTE_KEY_PATH, cipher,
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
    result = data_size_in_mb / time_used
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

  data_size_for_calculation = DEFAULT_DATA_SIZE_IN_MB
  if benchmark_spec.config.data_size_in_mb:
    data_size_for_calculation = benchmark_spec.config.data_size_in_mb

  metadata = {'copy_benchmark_single_file_mb':
              benchmark_spec.config.data_size_in_mb}

  results = MODE_FUNCTION_DICTIONARY[FLAGS.copy_benchmark_mode](
      vms, data_size_for_calculation, metadata)

  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
