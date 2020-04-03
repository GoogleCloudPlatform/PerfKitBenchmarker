# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing CUDA toolkit installation and cleanup functions.

This module installs CUDA toolkit from NVIDIA, configures gpu clock speeds
and autoboost settings, and exposes a method to collect gpu metadata. Currently
Tesla K80 and P100 gpus are supported, provided that there is only a single
type of gpu per system.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import posixpath
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import nvidia_driver

# There is no way to tell the apt-get installation
# method what dir to install the cuda toolkit to
CUDA_HOME = '/usr/local/cuda'

flags.DEFINE_enum(
    'cuda_toolkit_version',
    '9.0', ['9.0', '10.0', '10.1', '10.2', 'None', ''],
    'Version of CUDA Toolkit to install. '
    'Input "None" or empty string to skip installation',
    module_name=__name__)

FLAGS = flags.FLAGS

CUDA_PIN = 'https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/cuda-ubuntu1604.pin'

CUDA_10_2_TOOLKIT = 'http://developer.download.nvidia.com/compute/cuda/10.2/Prod/local_installers/cuda-repo-ubuntu1604-10-2-local-10.2.89-440.33.01_1.0-1_amd64.deb'

CUDA_10_1_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/10.1/Prod/local_installers/cuda-repo-ubuntu1604-10-1-local-10.1.243-418.87.00_1.0-1_amd64.deb'

CUDA_10_0_TOOLKIT = 'https://developer.nvidia.com/compute/cuda/10.0/Prod/local_installers/cuda-repo-ubuntu1604-10-0-local-10.0.130-410.48_1.0-1_amd64'

CUDA_9_0_TOOLKIT = 'https://developer.nvidia.com/compute/cuda/9.0/Prod/local_installers/cuda-repo-ubuntu1604-9-0-local_9.0.176-1_amd64-deb'
CUDA_9_0_PATCH = 'https://developer.nvidia.com/compute/cuda/9.0/Prod/patches/1/cuda-repo-ubuntu1604-9-0-local-cublas-performance-update_1.0-1_amd64-deb'


class UnsupportedCudaVersionError(Exception):
  pass


class NvccParseOutputError(Exception):
  pass


def GetMetadata(vm):
  """Returns gpu-specific metadata as a dict.

  Args:
    vm: virtual machine to operate on

  Returns:
    A dict of gpu- and CUDA- specific metadata.
  """
  metadata = nvidia_driver.GetMetadata(vm)
  metadata['cuda_toolkit_version'] = FLAGS.cuda_toolkit_version
  metadata['cuda_toolkit_home'] = CUDA_HOME
  return metadata


def DoPostInstallActions(vm):
  """Perform post NVIDIA driver install action on the vm.

  Args:
    vm: the virtual machine to operate on
  """
  nvidia_driver.DoPostInstallActions(vm)


def GetCudaToolkitVersion(vm):
  """Get the CUDA toolkit version on the vm, based on nvcc.

  Args:
    vm: the virtual machine to query

  Returns:
    A string containing the active CUDA toolkit version,
    None if nvcc could not be found

  Raises:
    NvccParseOutputError: On can not parse nvcc output
  """
  stdout, _ = vm.RemoteCommand(
      posixpath.join(CUDA_HOME, 'bin/nvcc') + ' --version',
      ignore_failure=True, suppress_warning=True)
  if bool(stdout.rstrip()):
    regex = r'release (\S+),'
    match = re.search(regex, stdout)
    if match:
      return str(match.group(1))
    raise NvccParseOutputError('Unable to parse nvcc version output from {}'
                               .format(stdout))
  else:
    return None


def _InstallCudaPatch(vm, patch_url):
  """Installs CUDA Toolkit patch from NVIDIA.

  Args:
    vm: VM to install patch on
    patch_url: url of the CUDA patch to install
  """
  # Need to append .deb to package name because the file downloaded from
  # NVIDIA is missing the .deb extension.
  basename = posixpath.basename(patch_url) + '.deb'
  vm.RemoteCommand('wget -q %s -O %s' % (patch_url,
                                         basename))
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-get update')
  # Need to be extra careful on the command below because without these
  # precautions, it was brining up a menu option about grub's menu.lst
  # on AWS Ubuntu16.04 and thus causing the RemoteCommand to hang and fail.
  vm.RemoteCommand(
      'sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -yq cuda')


def _InstallCuda9Point0(vm):
  """Installs CUDA Toolkit 9.0 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = posixpath.basename(CUDA_9_0_TOOLKIT) + '.deb'
  vm.RemoteCommand('wget -q %s -O %s' % (CUDA_9_0_TOOLKIT,
                                         basename))
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add /var/cuda-repo-9-0-local/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y cuda')
  _InstallCudaPatch(vm, CUDA_9_0_PATCH)


def _InstallCuda10Point0(vm):
  """Installs CUDA Toolkit 10.0 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = posixpath.basename(CUDA_10_0_TOOLKIT) + '.deb'
  vm.RemoteCommand('wget -q %s -O %s' % (CUDA_10_0_TOOLKIT,
                                         basename))
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add '
                   '/var/cuda-repo-10-0-local-10.0.130-410.48/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y cuda-toolkit-10-0 cuda-tools-10-0 '
                   'cuda-libraries-10-0 cuda-libraries-dev-10-0')


def _InstallCuda10Point1(vm):
  """Installs CUDA Toolkit 10.1 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = posixpath.basename(CUDA_10_1_TOOLKIT)
  vm.RemoteCommand('wget %s' % CUDA_PIN)
  vm.RemoteCommand('sudo mv cuda-ubuntu1604.pin '
                   '/etc/apt/preferences.d/cuda-repository-pin-600')
  vm.RemoteCommand('wget -q %s' % CUDA_10_1_TOOLKIT)
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add '
                   '/var/cuda-repo-10-1-local-10.1.243-418.87.00/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y cuda-toolkit-10-1 cuda-tools-10-1 '
                   'cuda-libraries-10-1 cuda-libraries-dev-10-1')


def _InstallCuda10Point2(vm):
  """Installs CUDA Toolkit 10.2 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = posixpath.basename(CUDA_10_2_TOOLKIT)
  vm.RemoteCommand('wget %s' % CUDA_PIN)
  vm.RemoteCommand('sudo mv cuda-ubuntu1604.pin '
                   '/etc/apt/preferences.d/cuda-repository-pin-600')
  vm.RemoteCommand('wget -q %s' % CUDA_10_2_TOOLKIT)
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add '
                   '/var/cuda-repo-10-2-local-10.2.89-440.33.01/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y cuda-toolkit-10-2 cuda-tools-10-2 '
                   'cuda-libraries-10-2 cuda-libraries-dev-10-2')


def AptInstall(vm):
  """Installs CUDA toolkit on the VM if not already installed."""
  version_to_install = FLAGS.cuda_toolkit_version
  if (version_to_install == 'None' or not version_to_install):
    return
  current_version = GetCudaToolkitVersion(vm)
  if current_version == version_to_install:
    return

  vm.Install('build_tools')
  vm.Install('wget')
  vm.Install('nvidia_driver')

  if version_to_install == '9.0':
    _InstallCuda9Point0(vm)
  elif version_to_install == '10.0':
    _InstallCuda10Point0(vm)
  elif version_to_install == '10.1':
    _InstallCuda10Point1(vm)
  elif version_to_install == '10.2':
    _InstallCuda10Point2(vm)
  else:
    raise UnsupportedCudaVersionError()
  DoPostInstallActions(vm)
  # NVIDIA CUDA Profile Tools Interface.
  # This library provides advanced profiling support
  if (version_to_install != '10.1' and version_to_install != '10.2'):
    # cupti is part of cuda10.1/2, and installed as cuda-cupti-10-1/2
    vm.RemoteCommand('sudo apt-get install -y libcupti-dev')


def YumInstall(vm):
  """Installs CUDA toolkit on the VM if not already installed.

  TODO: PKB currently only supports the installation of CUDA toolkit on Ubuntu.

  Args:
    vm: VM to install CUDA on
  """
  del vm  # unused
  raise NotImplementedError()


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  pass


def Uninstall(vm):
  """Removes the CUDA toolkit.

  Args:
    vm: VM that installed CUDA

  Note that reinstallation does not work correctly, i.e. you cannot reinstall
  CUDA by calling _Install() again.
  """
  vm.RemoteCommand('rm -f cuda-repo-ubuntu1604*')
  vm.RemoteCommand('sudo rm -rf {cuda_home}'.format(cuda_home=CUDA_HOME))
