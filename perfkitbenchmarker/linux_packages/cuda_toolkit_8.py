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

FLAGS = flags.FLAGS


# TODO: Test the CUDA Ubuntu 14.04 installer, and if everything works ok,
# automatically install the correct package depending on the OS image.
CUDA_TOOLKIT_UBUNTU = 'cuda-repo-ubuntu1604_8.0.61-1_amd64.deb'
CUDA_TOOLKIT_UBUNTU_URL = (
    'http://developer.download.nvidia.com/compute/cuda'
    '/repos/ubuntu1604/x86_64/%s' % CUDA_TOOLKIT_UBUNTU)
CUDA_TOOLKIT_INSTALL_DIR = '/usr/local/cuda'
EXTRACT_CLOCK_SPEEDS_REGEX = r'(\d*).*,\s*(\d*)'


class UnsupportedClockSpeedException(Exception):
  pass


class NvidiaSmiParseOutputException(Exception):
  pass


class HeterogeneousGpuTypesException(Exception):
  pass


class UnsupportedGpuTypeException(Exception):
  pass


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
  return metadata


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
  SetGpuClockSpeedAndAutoboost(vm, autoboost_enabled, desired_memory_clock,
                               desired_graphics_clock)
  num_gpus = QueryNumberOfGpus(vm)
  for i in range(num_gpus):
    if QueryGpuClockSpeed(vm, i) != (desired_memory_clock,
                                     desired_graphics_clock):
      raise UnsupportedClockSpeedException('Unrecoverable error setting '
                                           'GPU #{} clock speed to {},{}'
                                           .format(i, desired_memory_clock,
                                                   desired_graphics_clock))


def SetGpuClockSpeedAndAutoboost(vm,
                                 autoboost_enabled,
                                 memory_clock_speed,
                                 graphics_clock_speed):
  """Sets autoboost and memory and graphics clocks to the specified frequency.

  Persistence mode is enabled as well. Note that these settings are
  lost after reboot.

  Args:
    vm: virtual machine to operate on
    autoboost_enabled: bool or None. Value (if any) to set autoboost policy to
    memory_clock_speed: desired speed of the memory clock, in MHz
    graphics_clock_speed: desired speed of the graphics clock, in MHz
  """
  vm.RemoteCommand('sudo nvidia-smi -pm 1')
  if autoboost_enabled is not None:
    vm.RemoteCommand('sudo nvidia-smi --auto-boost-default=%s' % (
        1 if autoboost_enabled else 0))
  vm.RemoteCommand('sudo nvidia-smi -ac {},{}'.format(memory_clock_speed,
                                                      graphics_clock_speed))


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
  vm.RemoteCommand('sudo rm -rf %s' % CUDA_TOOLKIT_INSTALL_DIR)
