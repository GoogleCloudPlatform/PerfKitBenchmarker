# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing NVIDIA Driver installation.
"""

import re
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util


NVIDIA_DRIVER_LOCATION_BASE = 'https://us.download.nvidia.com/tesla'

NVIDIA_TESLA_K80 = 'k80'
NVIDIA_TESLA_P4 = 'p4'
NVIDIA_TESLA_P100 = 'p100'
NVIDIA_TESLA_V100 = 'v100'
NVIDIA_TESLA_T4 = 't4'

"""Default GPU clocks and autoboost configurations.

Base_clock is the default clock speeds when setting the GPU clocks. Max_clock
is currently unused. The clock speeds are in the format of
[memory_clock in MHz, graphics_clock in MHz].
"""
GPU_DEFAULTS = {
    NVIDIA_TESLA_K80: {
        'base_clock': [2505, 562],
        'max_clock': [2505, 875],
        'autoboost_enabled': True,
    },
    NVIDIA_TESLA_P4: {
        'base_clock': [3003, 885],
        'max_clock': [3003, 1531],
        'autoboost_enabled': None,
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
    NVIDIA_TESLA_T4: {
        'base_clock': [5001, 585],
        'max_clock': [5001, 1590],
        'autoboost_enabled': None,
    },
}

EXTRACT_CLOCK_SPEEDS_REGEX = r'(\d*).*,\s*(\d*)'

flag_util.DEFINE_integerlist('gpu_clock_speeds',
                             None,
                             'desired gpu clock speeds in the form '
                             '[memory clock, graphics clock]')

flags.DEFINE_boolean('gpu_autoboost_enabled', None,
                     'whether gpu autoboost is enabled')

flags.DEFINE_string('nvidia_driver_version', None,
                    'The version of nvidia driver to install. '
                    'For example, "418.67" or "418.87.01"')

flags.DEFINE_string('nvidia_driver_x_library_path', '/usr/lib',
                    'X library path for nvidia driver installation')

flags.DEFINE_string('nvidia_driver_x_module_path', '/usr/lib/xorg/modules',
                    'X module path for nvidia driver installation')

flags.DEFINE_boolean('nvidia_driver_persistence_mode', None,
                     'whether to enable persistence mode on the NVIDIA GPU')

FLAGS = flags.FLAGS


class UnsupportedClockSpeedError(Exception):
  pass


class NvidiaSmiParseOutputError(Exception):
  pass


class HeterogeneousGpuTypesError(Exception):
  pass


class UnsupportedGpuTypeError(Exception):
  pass


def CheckNvidiaGpuExists(vm):
  """Returns whether NVIDIA GPU exists or not on the vm.

  Args:
    vm: The virtual machine to check.

  Returns:
    True or False depending on whether NVIDIA GPU exists.
  """
  vm.Install('pciutils')
  output, _ = vm.RemoteCommand('sudo lspci', should_log=True)
  regex = re.compile(r'3D controller: NVIDIA Corporation')
  return regex.search(output) is not None


def CheckNvidiaSmiExists(vm):
  """Returns whether nvidia-smi is installed or not on a VM.

  Args:
    vm: The virtual to check.

  Returns:
    True or False depending on whether nvidia-smi command exists.
  """
  resp, _ = vm.RemoteHostCommand('command -v nvidia-smi',
                                 ignore_failure=True,
                                 suppress_warning=True)
  return bool(resp.rstrip())


def GetDriverVersion(vm):
  """Returns the NVIDIA driver version as a string.

  Args:
    vm: Virtual machine to query.

  Returns:
    String containing NVIDIA driver version installed.

  Raises:
    NvidiaSmiParseOutputError: If nvidia-smi output cannot be parsed.
  """
  stdout, _ = vm.RemoteCommand('nvidia-smi', should_log=True)
  regex = r'Driver Version\:\s+(\S+)'
  match = re.search(regex, stdout)
  if match:
    return str(match.group(1))
  raise NvidiaSmiParseOutputError('Unable to parse driver version from {}'
                                  .format(stdout))


def GetGpuType(vm):
  """Return the type of NVIDIA gpu(s) installed on the vm.

  Args:
    vm: Virtual machine to query.

  Returns:
    Type of gpus installed on the vm as a string.

  Raises:
    NvidiaSmiParseOutputError: If nvidia-smi output cannot be parsed.
    HeterogeneousGpuTypesError: If more than one gpu type is detected.
    UnsupportedClockSpeedError: If gpu type is not supported.

  Example:
    If 'nvidia-smi -L' returns:

    GPU 0: Tesla V100-SXM2-16GB (UUID: GPU-1a046bb9-e456-45d3-5a35-52da392d09a5)
    GPU 1: Tesla V100-SXM2-16GB (UUID: GPU-56cf4732-054c-4e40-9680-0ec27e97d21c)
    GPU 2: Tesla V100-SXM2-16GB (UUID: GPU-4c7685ad-4b3a-8adc-ce20-f3a945127a8a)
    GPU 3: Tesla V100-SXM2-16GB (UUID: GPU-0b034e63-22be-454b-b395-382e2d324728)
    GPU 4: Tesla V100-SXM2-16GB (UUID: GPU-b0861159-4727-ef2f-ff66-73a765f4ecb6)
    GPU 5: Tesla V100-SXM2-16GB (UUID: GPU-16ccaf51-1d1f-babe-9f3d-377e900bf37e)
    GPU 6: Tesla V100-SXM2-16GB (UUID: GPU-6eba1fa6-de10-80e9-ec5f-4b8beeff7e12)
    GPU 7: Tesla V100-SXM2-16GB (UUID: GPU-cba5a243-219c-df12-013e-1dbc98a8b0de)

    GetGpuType() will return:

    ['V100-SXM2-16GB', 'V100-SXM2-16GB', 'V100-SXM2-16GB', 'V100-SXM2-16GB',
     'V100-SXM2-16GB', 'V100-SXM2-16GB', 'V100-SXM2-16GB', 'V100-SXM2-16GB']
  """
  stdout, _ = vm.RemoteCommand('nvidia-smi -L', should_log=True)
  try:
    gpu_types = [line.split(' ')[3] for line in stdout.splitlines() if line]
  except:
    raise NvidiaSmiParseOutputError('Unable to parse gpu type from {}'
                                    .format(stdout))
  if any(gpu_type != gpu_types[0] for gpu_type in gpu_types):
    raise HeterogeneousGpuTypesError(
        'PKB only supports one type of gpu per VM')

  if 'K80' in gpu_types[0]:
    return NVIDIA_TESLA_K80
  if 'P4' in gpu_types[0]:
    return NVIDIA_TESLA_P4
  if 'P100' in gpu_types[0]:
    return NVIDIA_TESLA_P100
  if 'V100' in gpu_types[0]:
    return NVIDIA_TESLA_V100
  if 'T4' in gpu_types[0]:
    return NVIDIA_TESLA_T4
  raise UnsupportedClockSpeedError(
      'Gpu type {0} is not supported by PKB'.format(gpu_types[0]))


def QueryNumberOfGpus(vm):
  """Returns the number of NVIDIA GPUs on the system.

  Args:
    vm: Virtual machine to query.

  Returns:
    Integer indicating the number of NVIDIA GPUs present on the vm.
  """
  stdout, _ = vm.RemoteCommand('sudo nvidia-smi --query-gpu=count --id=0 '
                               '--format=csv', should_log=True)
  return int(stdout.split()[1])


def GetPeerToPeerTopology(vm):
  """Returns a string specifying which GPUs can access each other via p2p.

  Args:
    vm: Virtual machine to operate on.

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
          .replace('X', 'Y')    # replace X (self) with Y
          .replace('OK', 'Y')   # replace OK with Y
          .replace('NS', 'N'))  # replace NS (not supported) with N


def SetAndConfirmGpuClocks(vm):
  """Sets and confirms the GPU clock speed and autoboost policy.

  The clock values are provided either by the gpu_pcie_bandwidth_clock_speeds
  flags, or from gpu-specific defaults. If a device is queried and its
  clock speed does not align with what it was just set to, an exception will
  be raised.

  Args:
    vm: The virtual machine to operate on.

  Raises:
    UnsupportedClockSpeedError: If a GPU did not accept the
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
      raise UnsupportedClockSpeedError(
          'Unrecoverable error setting GPU #{} clock speed to {},{}'.format(
              i, desired_memory_clock, desired_graphics_clock))


def SetGpuClockSpeed(vm, memory_clock_speed, graphics_clock_speed):
  """Sets autoboost and memory and graphics clocks to the specified frequency.

  Args:
    vm: Virtual machine to operate on.
    memory_clock_speed: Desired speed of the memory clock, in MHz.
    graphics_clock_speed: Desired speed of the graphics clock, in MHz.
  """
  num_gpus = QueryNumberOfGpus(vm)
  for device_id in range(num_gpus):
    current_clock_speeds = QueryGpuClockSpeed(vm, device_id)
    if current_clock_speeds != (memory_clock_speed, graphics_clock_speed):
      vm.RemoteCommand('sudo nvidia-smi -ac {},{} --id={}'.format(
          memory_clock_speed,
          graphics_clock_speed,
          device_id
      ))


def QueryGpuClockSpeed(vm, device_id):
  """Returns the value of the memory and graphics clock.

  All clock values are in MHz.

  Args:
    vm: Virtual machine to operate on.
    device_id: Id of GPU device to query.

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


def EnablePersistenceMode(vm):
  """Enables persistence mode on the NVIDIA driver.

  Args:
    vm: Virtual machine to operate on.
  """
  vm.RemoteCommand('sudo nvidia-smi -pm 1')


def SetAutoboostDefaultPolicy(vm, autoboost_enabled):
  """Sets the autoboost policy to the specified value.

  For each GPU on the VM, this function will set the autoboost policy
  to the value specified by autoboost_enabled.
  Args:
    vm: Virtual machine to operate on.
    autoboost_enabled: Bool or None. Value (if any) to set autoboost policy to
  """
  if autoboost_enabled is None:
    return

  num_gpus = QueryNumberOfGpus(vm)
  for device_id in range(num_gpus):
    current_state = QueryAutoboostPolicy(vm, device_id)
    if current_state['autoboost_default'] != autoboost_enabled:
      vm.RemoteCommand('sudo nvidia-smi --auto-boost-default={0} --id={1}'
                       .format(1 if autoboost_enabled else 0, device_id))


def QueryAutoboostPolicy(vm, device_id):
  """Returns the state of autoboost and autoboost_default.

  Args:
    vm: Virtual machine to operate on.
    device_id: Id of GPU device to query.

  Returns:
    Dict containing values for autoboost and autoboost_default.
    Values can be True (autoboost on), False (autoboost off),
    and None (autoboost not supported).

  Raises:
    NvidiaSmiParseOutputError: If output from nvidia-smi can not be parsed.
  """
  autoboost_regex = r'Auto Boost\s*:\s*(\S+)'
  autoboost_default_regex = r'Auto Boost Default\s*:\s*(\S+)'
  query = 'sudo nvidia-smi -q -d CLOCK --id={0}'.format(device_id)
  stdout, _ = vm.RemoteCommand(query, should_log=True)
  autoboost_match = re.search(autoboost_regex, stdout)
  autoboost_default_match = re.search(autoboost_default_regex, stdout)

  nvidia_smi_output_string_to_value = {
      'On': True,
      'Off': False,
      'N/A': None,
  }

  if (autoboost_match is None) or (autoboost_default_match is None):
    raise NvidiaSmiParseOutputError('Unable to parse Auto Boost policy from {}'
                                    .format(stdout))
  return {
      'autoboost': nvidia_smi_output_string_to_value[
          autoboost_match.group(1)],
      'autoboost_default': nvidia_smi_output_string_to_value[
          autoboost_default_match.group(1)]
  }


def GetMetadata(vm):
  """Returns gpu-specific metadata as a dict.

  Args:
    vm: Virtual machine to operate on.

  Returns:
    A dict of gpu-specific metadata.
  """
  clock_speeds = QueryGpuClockSpeed(vm, 0)
  autoboost_policy = QueryAutoboostPolicy(vm, 0)
  return {
      'gpu_memory_clock': clock_speeds[0],
      'gpu_graphics_clock': clock_speeds[1],
      'gpu_autoboost': autoboost_policy['autoboost'],
      'gpu_autoboost_default': autoboost_policy['autoboost_default'],
      'nvidia_driver_version': GetDriverVersion(vm),
      'gpu_type': GetGpuType(vm),
      'num_gpus': QueryNumberOfGpus(vm),
      'peer_to_peer_gpu_topology': GetPeerToPeerTopology(vm),
  }


def DoPostInstallActions(vm):
  """Perform post NVIDIA driver install action on the vm.

  Args:
    vm: The virtual machine to operate on.
  """
  SetAndConfirmGpuClocks(vm)


def Install(vm):
  """Install NVIDIA GPU driver on the vm.

  Args:
    vm: The virtual machine to install NVIDIA driver on.
  """
  version_to_install = FLAGS.nvidia_driver_version
  if version_to_install is None:
    return

  location = ('{base}/{version}/NVIDIA-Linux-x86_64-{version}.run'
              .format(base=NVIDIA_DRIVER_LOCATION_BASE,
                      version=version_to_install))

  vm.Install('wget')
  tokens = re.split('/', location)
  filename = tokens[-1]
  vm.RemoteCommand('wget {location} && chmod 755 {filename} '
                   .format(location=location, filename=filename),
                   should_log=True)
  vm.RemoteCommand('sudo ./{filename} -q -x-module-path={x_module_path} '
                   '--ui=none -x-library-path={x_library_path} '
                   '--no-install-compat32-libs'
                   .format(filename=filename,
                           x_module_path=FLAGS.nvidia_driver_x_module_path,
                           x_library_path=FLAGS.nvidia_driver_x_library_path),
                   should_log=True)
  if FLAGS.nvidia_driver_persistence_mode:
    EnablePersistenceMode(vm)
