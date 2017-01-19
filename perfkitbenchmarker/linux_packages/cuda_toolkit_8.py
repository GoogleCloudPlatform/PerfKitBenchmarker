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


"""Module containing CUDA toolkit 8 installation and cleanup functions."""

from perfkitbenchmarker import regex_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util


TESLA_K80_MAX_CLOCK_SPEEDS = [2505, 875]
flag_util.DEFINE_integerlist('gpu_clock_speeds',
                             flag_util.IntegerList(TESLA_K80_MAX_CLOCK_SPEEDS),
                             'desired gpu clock speeds in the form '
                             '[memory clock, graphics clock]')


FLAGS = flags.FLAGS


# TODO: Test the CUDA Ubuntu 14.04 installer, and if everything works ok,
# automatically install the correct package depending on the OS image.
CUDA_TOOLKIT_UBUNTU = 'cuda-repo-ubuntu1604_8.0.44-1_amd64.deb'
CUDA_TOOLKIT_UBUNTU_URL = (
    'http://developer.download.nvidia.com/compute/cuda'
    '/repos/ubuntu1604/x86_64/%s' % CUDA_TOOLKIT_UBUNTU)
CUDA_TOOLKIT_INSTALL_DIR = '/usr/local/cuda'
EXTRACT_CLOCK_SPEEDS_REGEX = r'(\d*).*,\s*(\d*)'


class UnsupportedClockSpeedException(Exception):
  pass


def QueryNumberOfGpus(vm):
  """Returns the number of Nvidia GPUs on the system"""
  stdout, _ = vm.RemoteCommand('sudo nvidia-smi --query-gpu=count --id=0 '
                               '--format=csv', should_log=True)
  return int(stdout.split()[1])


def SetAndConfirmGpuClocks(vm):
  """Sets and confirms the GPU clock speed.

  The clock values are provided in the gpu_pcie_bandwidth_clock_speeds
  flag. If a device is queried and its clock speed does not allign with
  what it was just set to, an expection will be raised.

  Args:
    vm: the virtual machine to operate on.

  Raises:
    UnsupportedClockSpeedException if a GPU did not accept the
    provided clock speeds.
  """
  desired_memory_clock = FLAGS.gpu_clock_speeds[0]
  desired_graphics_clock = FLAGS.gpu_clock_speeds[1]
  SetGpuClockSpeed(vm, desired_memory_clock, desired_graphics_clock)
  num_gpus = QueryNumberOfGpus(vm)
  for i in range(num_gpus):
    if QueryGpuClockSpeed(vm, i) != (desired_memory_clock,
                                     desired_graphics_clock):
      raise UnsupportedClockSpeedException('Unrecoverable error setting '
                                           'GPU #{} clock speed to {},{}'
                                           .format(i, desired_memory_clock,
                                                   desired_graphics_clock))


def SetGpuClockSpeed(vm, memory_clock_speed, graphics_clock_speed):
  """Sets the memory and graphics clocks to the specified frequency.

  Persistence mode is enabled as well. Note that these settings are
  lost after reboot.

  Args:
    vm: virtual machine to operate on
    memory_clock_speed: desired speed of the memory clock, in MHz
    graphics_clock_speed: desired speed of the graphics clock, in MHz
  """
  vm.RemoteCommand('sudo nvidia-smi -pm 1')
  vm.RemoteCommand('sudo nvidia-smi -ac {},{}'.format(memory_clock_speed,
                                                      graphics_clock_speed))


def QueryGpuClockSpeed(vm, device_id):
  """Returns the user-specified values of the memory and graphics clock.

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


def AptInstall(vm):
  """Installs CUDA toolkit 8 on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand('wget %s' % CUDA_TOOLKIT_UBUNTU_URL)
  vm.RemoteCommand('sudo dpkg -i %s' % CUDA_TOOLKIT_UBUNTU)
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y cuda')
  vm.RemoteCommand('sudo reboot', ignore_failure=True)
  vm.WaitForBootCompletion()


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
