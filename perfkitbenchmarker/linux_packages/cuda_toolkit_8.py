# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing CUDA toolkit 8 installation and cleanup functions.

This module installs cuda toolkit 8 from NVIDIA, configures gpu clock speeds
and autoboost settings, and exposes a method to collect gpu metadata. Currently
Tesla K80 and P100 gpus are supported, provided that there is only a single
type of gpu per system.
"""

import posixpath
import re

from perfkitbenchmarker import regex_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import vm_util


NVIDIA_TESLA_K80 = 'k80'
NVIDIA_TESLA_P100 = 'p100'
NVIDIA_TESLA_V100 = 'v100'
GPU_DEFAULTS = {
    NVIDIA_TESLA_K80: {
        'base_clock': [2505, 562],
        'max_clock': [2505, 875],
        'autoboost_enabled': True,
    },
    NVIDIA_TESLA_P100: {
        'base_clock': [715, 1189],
        'max_clock': [715, 1328],
        'autoboost_enabled': None,
    },
    NVIDIA_TESLA_V100: {
        'base_clock': [877, 1312],
        'max_clock': [877, 1530],
        'autoboost_enabled': None,
    },
}

flag_util.DEFINE_integerlist('gpu_clock_speeds',
                             None,
                             'desired gpu clock speeds in the form '
                             '[memory clock, graphics clock]')
flags.DEFINE_boolean('gpu_autoboost_enabled', None,
                     'whether gpu autoboost is enabled')

flags.DEFINE_string('cuda_toolkit_installation_dir', '/usr/local/cuda',
                    'installation directory to use for CUDA toolkit. '
                    'If the toolkit is not installed, it will be installed '
                    'here. If it is already installed, the installation at '
                    'this path will be used.')

FLAGS = flags.FLAGS


# TODO: Test the CUDA Ubuntu 14.04 installer, and if everything works ok,
# automatically install the correct package depending on the OS image.
CUDA_TOOLKIT_UBUNTU = 'cuda-repo-ubuntu1604_8.0.61-1_amd64.deb'
CUDA_TOOLKIT_UBUNTU_URL = (
    'http://developer.download.nvidia.com/compute/cuda'
    '/repos/ubuntu1604/x86_64/%s' % CUDA_TOOLKIT_UBUNTU)
EXTRACT_CLOCK_SPEEDS_REGEX = r'(\d*).*,\s*(\d*)'


class UnsupportedClockSpeedException(Exception):
  pass


class NvidiaSmiParseOutputException(Exception):
  pass


class HeterogeneousGpuTypesException(Exception):
  pass


class UnsupportedGpuTypeException(Exception):
  pass


def SmiPath():
  return posixpath.join(flags.cuda_toolkit_installation_dir,
                        'nvidia-smi')


def GetMetadata(vm):
  """Returns gpu-specific metadata as a dict.

  Returns:
    A dict of gpu-specific metadata.
  """
  metadata = {}
  clock_speeds = QueryGpuClockSpeed(vm, 0)
  autoboost_policy = QueryAutoboostPolicy(vm, 0)
  metadata['gpu_memory_clock'] = clock_speeds[0]
  metadata['gpu_graphics_clock'] = clock_speeds[1]
  metadata['gpu_autoboost'] = autoboost_policy['autoboost']
  metadata['gpu_autoboost_default'] = autoboost_policy['autoboost_default']
  metadata['nvidia_driver_version'] = GetDriverVersion(vm)
  metadata['gpu_type'] = GetGpuType(vm)
  metadata['num_gpus'] = QueryNumberOfGpus(vm)
  metadata['peer_to_peer_gpu_topology'] = GetPeerToPeerTopology(vm)
  return metadata


def GetPeerToPeerTopology(vm):
  """Returns a string specifying which GPUs can access each other via p2p.

  Example:
    If p2p topology from nvidia-smi topo -p2p r looks like this:

      0   1   2   3
    0 X   OK  NS  NS
    1 OK  X   NS  NS
    2 NS  NS  X   OK
    3 NS  NS  OK  X

    GetTopology will return 'Y Y N N;Y Y N N;N N Y Y;N N Y Y'
  """
  stdout, _ = vm.RemoteCommand('nvidia-smi topo -p2p r', should_log=True)
  lines = [line.split() for line in stdout.splitlines()]
  num_gpus = len(lines[0])

  results = []
  for idx, line in enumerate(lines[1:]):
    if idx >= num_gpus:
      break
    results.append(' '.join(line[1:]))

  # Delimit each GPU result with semicolons,
  # and simplify the result character set to 'Y' and 'N'.
  return (';'.join(results)
          .replace('X', 'Y')   # replace X (self) with Y
          .replace('OK', 'Y')  # replace OK with Y
          .replace('NS', 'N')) # replace NS (not supported) with N


def GetGpuType(vm):
  """Return the type of NVIDIA gpu(s) installed on the vm.

  Args:
    vm: virtual machine to query

  Returns:
    type of gpus installed on the vm as a string

  Raises:
    NvidiaSmiParseOutputException: if nvidia-smi output cannot be parsed
    HeterogeneousGpuTypesException: if more than one gpu type is detected
  """
  stdout, _ = vm.RemoteCommand('nvidia-smi -L', should_log=True)
  try:
    gpu_types = [line.split(' ')[3] for line in stdout.splitlines() if line]
  except:
    raise NvidiaSmiParseOutputException('Unable to parse gpu type')
  if any(gpu_type != gpu_types[0] for gpu_type in gpu_types):
    raise HeterogeneousGpuTypesException(
        'PKB only supports one type of gpu per VM')

  if 'K80' in gpu_types[0]:
    return NVIDIA_TESLA_K80
  if 'P100' in gpu_types[0]:
    return NVIDIA_TESLA_P100
  if 'V100' in gpu_types[0]:
    return NVIDIA_TESLA_V100
  raise UnsupportedClockSpeedException(
      'Gpu type {0} is not supported by PKB'.format(gpu_types[0]))


def GetDriverVersion(vm):
  """Returns the NVIDIA driver version as a string"""
  stdout, _ = vm.RemoteCommand('nvidia-smi', should_log=True)
  regex = 'Driver Version\:\s+(\S+)'
  match = re.search(regex, stdout)
  try:
    return str(match.group(1))
  except:
    raise NvidiaSmiParseOutputException('Unable to parse driver version')


def QueryNumberOfGpus(vm):
  """Returns the number of Nvidia GPUs on the system"""
  stdout, _ = vm.RemoteCommand('sudo nvidia-smi --query-gpu=count --id=0 '
                               '--format=csv', should_log=True)
  return int(stdout.split()[1])


def SetAndConfirmGpuClocks(vm):
  """Sets and confirms the GPU clock speed and autoboost policy.

  The clock values are provided either by the gpu_pcie_bandwidth_clock_speeds
  flags, or from gpu-specific defaults. If a device is queried and its
  clock speed does not align with what it was just set to, an exception will
  be raised.

  Args:
    vm: the virtual machine to operate on.

  Raises:
    UnsupportedClockSpeedException if a GPU did not accept the
    provided clock speeds.
  """
  gpu_type = GetGpuType(vm)
  gpu_clock_speeds = GPU_DEFAULTS[gpu_type]['base_clock']
  autoboost_enabled = GPU_DEFAULTS[gpu_type]['autoboost_enabled']

  if FLAGS.gpu_clock_speeds is not None:
    gpu_clock_speeds = FLAGS.gpu_clock_speeds
  if FLAGS.gpu_autoboost_enabled is not None:
    autoboost_enabled = FLAGS.gpu_autoboost_enabled

  desired_memory_clock = gpu_clock_speeds[0]
  desired_graphics_clock = gpu_clock_speeds[1]
  EnablePersistenceMode(vm)
  SetGpuClockSpeed(vm, desired_memory_clock, desired_graphics_clock)
  SetAutoboostDefaultPolicy(vm, autoboost_enabled)
  num_gpus = QueryNumberOfGpus(vm)
  for i in range(num_gpus):
    if QueryGpuClockSpeed(vm, i) != (desired_memory_clock,
                                     desired_graphics_clock):
      raise UnsupportedClockSpeedException('Unrecoverable error setting '
                                           'GPU #{} clock speed to {},{}'
                                           .format(i, desired_memory_clock,
                                                   desired_graphics_clock))


def EnablePersistenceMode(vm):
  """Enables persistence mode on the NVIDIA driver.

  Args:
    vm: virtual machine to operate on
  """
  vm.RemoteCommand('sudo nvidia-smi -pm 1')


def SetAutoboostDefaultPolicy(vm, autoboost_enabled):
  """Sets the autoboost policy to the specified value.

  For each GPU on the VM, this function will set the autoboost policy
  to the value specified by autoboost_enabled.
  Args:
    vm: virtual machine to operate on
    autoboost_enabled: bool or None. Value (if any) to set autoboost policy to
  """
  if autoboost_enabled is None:
    return

  num_gpus = QueryNumberOfGpus(vm)
  for device_id in range(0, num_gpus):
    current_state = QueryAutoboostPolicy(vm, device_id)
    if current_state['autoboost_default'] != autoboost_enabled:
      vm.RemoteCommand('sudo nvidia-smi --auto-boost-default={0} --id={1}'
                       .format(1 if autoboost_enabled else 0, device_id))


def SetGpuClockSpeed(vm, memory_clock_speed, graphics_clock_speed):
  """Sets autoboost and memory and graphics clocks to the specified frequency.

  Args:
    vm: virtual machine to operate on
    memory_clock_speed: desired speed of the memory clock, in MHz
    graphics_clock_speed: desired speed of the graphics clock, in MHz
  """
  num_gpus = QueryNumberOfGpus(vm)
  for device_id in range(0, num_gpus):
    current_clock_speeds = QueryGpuClockSpeed(vm, device_id)
    if (
        current_clock_speeds[0] != memory_clock_speed or
        current_clock_speeds[1] != graphics_clock_speed):
      vm.RemoteCommand('sudo nvidia-smi -ac {},{} --id={}'.format(
          memory_clock_speed,
          graphics_clock_speed,
          device_id
      ))


def QueryAutoboostPolicy(vm, device_id):
  """Returns the state of autoboost and autoboost_default.

  Args:
    vm: virtual machine to operate on
    device_id: id of GPU device to query

  Returns:
    dict containing values for autoboost and autoboost_default.
    Values can be True (autoboost on), False (autoboost off),
    and None (autoboost not supported).

  """
  autoboost_regex = r'Auto Boost\s*:\s*(\S+)'
  autoboost_default_regex = r'Auto Boost Default\s*:\s*(\S+)'
  query = ('sudo nvidia-smi -q -d CLOCK --id={0}'.format(device_id))
  stdout, _ = vm.RemoteCommand(query, should_log=True)
  autoboost_match = re.search(autoboost_regex, stdout)
  autoboost_default_match = re.search(autoboost_default_regex, stdout)

  nvidia_smi_output_string_to_value = {
      'On': True,
      'Off': False,
      'N/A': None,
  }

  try:
    return {
        'autoboost': nvidia_smi_output_string_to_value[
            autoboost_match.group(1)],
        'autoboost_default': nvidia_smi_output_string_to_value[
            autoboost_default_match.group(1)]
    }
  except:
    raise NvidiaSmiParseOutputException('Unable to parse Auto Boost policy')


def QueryGpuClockSpeed(vm, device_id):
  """Returns the value of the memory and graphics clock.

  All clock values are in MHz.

  Args:
    vm: virtual machine to operate on
    device_id: id of GPU device to query

  Returns:
    Tuple of clock speeds in MHz in the form (memory clock, graphics clock).
  """
  query = ('sudo nvidia-smi --query-gpu=clocks.applications.memory,'
           'clocks.applications.graphics --format=csv --id={0}'
           .format(device_id))
  stdout, _ = vm.RemoteCommand(query, should_log=True)
  clock_speeds = stdout.splitlines()[1]
  matches = regex_util.ExtractAllMatches(EXTRACT_CLOCK_SPEEDS_REGEX,
                                         clock_speeds)[0]
  return (int(matches[0]), int(matches[1]))


def _CheckNvidiaSmiExists(vm):
  """Returns whether nvidia-smi is installed or not"""
  resp, _ = vm.RemoteHostCommand('command -v nvidia-smi',
                                 ignore_failure=True,
                                 suppress_warning=True)
  if resp.rstrip() == "":
    return False
  return True


def DoPostInstallActions(vm):
  SetAndConfirmGpuClocks(vm)


@vm_util.Retry(timeout=900)
def _InstallCuda(vm):
  """Installs CUDA Toolkit from NVIDIA, with retry.

  Steps taken from section 3.6 found here:
  http://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html
  """
  vm.RemoteCommand('wget %s' % CUDA_TOOLKIT_UBUNTU_URL)
  vm.RemoteCommand('sudo dpkg -i %s' % CUDA_TOOLKIT_UBUNTU)
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y cuda-8-0')


def AptInstall(vm):
  """Installs CUDA toolkit 8 on the VM if not already installed"""
  if _CheckNvidiaSmiExists(vm):
    DoPostInstallActions(vm)
    return

  vm.Install('build_tools')
  vm.Install('wget')
  _InstallCuda(vm)
  vm.Reboot()
  DoPostInstallActions(vm)
  # NVIDIA CUDA Profile Tools Interface.
  # This library provides advanced profiling support
  vm.RemoteCommand('sudo apt-get install -y libcupti-dev')


def YumInstall(vm):
  """TODO: PKB currently only supports the installation of CUDA toolkit
     on Ubuntu.
  """
  raise NotImplementedError()


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  pass


def Uninstall(vm):
  """Removes the CUDA toolkit.

  Note that reinstallation does not work correctly, i.e. you cannot reinstall
  CUDA by calling _Install() again.
  """
  vm.RemoteCommand('rm %s' % CUDA_TOOLKIT_UBUNTU)
  vm.RemoteCommand('sudo rm -rf %s' % FLAGS.cuda_toolkit_installation_dir)
