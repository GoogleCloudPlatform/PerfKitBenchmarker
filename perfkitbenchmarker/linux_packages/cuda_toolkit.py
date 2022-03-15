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


import posixpath
import re

from absl import flags
from perfkitbenchmarker.linux_packages import nvidia_driver

# There is no way to tell the apt-get installation
# method what dir to install the cuda toolkit to
CUDA_HOME = '/usr/local/cuda'

flags.DEFINE_enum(
    'cuda_toolkit_version',
    '11.5', [
        '9.0', '10.0', '10.1', '10.2', '11.0', '11.1', '11.2', '11.3', '11.4',
        '11.5', '11.6', 'None', ''
    ], 'Version of CUDA Toolkit to install. '
    'Input "None" or empty string to skip installation',
    module_name=__name__)

FLAGS = flags.FLAGS

CUDA_PIN = 'https://developer.download.nvidia.com/compute/cuda/repos/{os}/{cpu_arch}/cuda-{os}.pin'

CUDA_11_6_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.6.1/local_installers/cuda-repo-{os}-11-6-local_11.6.1-510.47.03-1_{cpu_arch}.deb'
CUDA_11_5_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.5.2/local_installers/cuda-repo-{os}-11-5-local_11.5.2-495.29.05-1_{cpu_arch}.deb'
CUDA_11_4_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.4.4/local_installers/cuda-repo-{os}-11-4-local_11.4.4-470.82.01-1_{cpu_arch}.deb'
CUDA_11_3_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.3.1/local_installers/cuda-repo-{os}-11-3-local_11.3.1-465.19.01-1_{cpu_arch}.deb'
CUDA_11_2_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.2.2/local_installers/cuda-repo-{os}-11-2-local_11.2.2-460.32.03-1_{cpu_arch}.deb'
CUDA_11_1_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.1.1/local_installers/cuda-repo-{os}-11-1-local_11.1.1-455.32.00-1_{cpu_arch}.deb'
CUDA_11_0_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/11.0.3/local_installers/cuda-repo-{os}-11-0-local_11.0.3-450.51.06-1_{cpu_arch}.deb'
CUDA_10_2_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/10.2/Prod/local_installers/cuda-repo-{os}-10-2-local-10.2.89-440.33.01_1.0-1_{cpu_arch}.deb'
CUDA_10_1_TOOLKIT = 'https://developer.download.nvidia.com/compute/cuda/10.1/Prod/local_installers/cuda-repo-{os}-10-1-local-10.1.243-418.87.00_1.0-1_{cpu_arch}.deb'
CUDA_10_0_TOOLKIT = 'https://developer.nvidia.com/compute/cuda/10.0/Prod/local_installers/cuda-repo-{os}-10-0-local-10.0.130-410.48_1.0-1_{cpu_arch}'
CUDA_9_0_TOOLKIT = 'https://developer.nvidia.com/compute/cuda/9.0/Prod/local_installers/cuda-repo-{os}-9-0-local_9.0.176-1_{cpu_arch}-deb'
CUDA_9_0_PATCH = 'https://developer.nvidia.com/compute/cuda/9.0/Prod/patches/1/cuda-repo-{os}-9-0-local-cublas-performance-update_1.0-1_{cpu_arch}-deb'


def _CudaOs(os_type):
  return re.sub('_.*$', '', os_type)


def _GetCpuArchPath(vm):
  """Returns the CPU architecture of the VM."""
  return 'arm64' if vm.host_arch else 'x86_64'


def _GetCpuArch(vm):
  """Returns the CPU architecture of the VM."""
  return 'arm64' if vm.host_arch else 'amd64'


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
  metadata['vm_name'] = vm.name
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
  basename = posixpath.basename(
      CUDA_9_0_TOOLKIT.format(os=_CudaOs(vm.OS_TYPE),
                              cpu_arch=_GetCpuArch(vm))) + '.deb'
  vm.RemoteCommand('wget -q %s -O %s' % (CUDA_9_0_TOOLKIT.format(
      os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)), basename))
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add /var/cuda-repo-9-0-local/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('cuda-toolkit-9-0 cuda-tools-9-0 cuda-libraries-9-0 '
                     'cuda-libraries-dev-9-0')
  _InstallCudaPatch(
      vm,
      CUDA_9_0_PATCH.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)))


def _InstallCuda10Point0(vm):
  """Installs CUDA Toolkit 10.0 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = (
      f'{posixpath.basename(CUDA_10_0_TOOLKIT.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)))}.deb'
  )
  vm.RemoteCommand(
      f'wget -q {CUDA_10_0_TOOLKIT.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm))} -O '
      f'{basename}')
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add '
                   '/var/cuda-repo-10-0-local-10.0.130-410.48/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('cuda-toolkit-10-0 cuda-tools-10-0 cuda-libraries-10-0 '
                     'cuda-libraries-dev-10-0')


def _InstallCuda10Point1(vm):
  """Installs CUDA Toolkit 10.1 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = posixpath.basename(
      CUDA_10_1_TOOLKIT.format(
          os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)))
  vm.RemoteCommand(
      'wget -q %s' %
      CUDA_PIN.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArchPath(vm)))
  vm.RemoteCommand(f'sudo mv cuda-{_CudaOs(vm.OS_TYPE)}.pin '
                   '/etc/apt/preferences.d/cuda-repository-pin-600')
  vm.RemoteCommand('wget -q %s' % CUDA_10_1_TOOLKIT.format(
      os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)))
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add '
                   '/var/cuda-repo-10-1-local-10.1.243-418.87.00/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('cuda-toolkit-10-1 cuda-tools-10-1 cuda-libraries-10-1 '
                     'cuda-libraries-dev-10-1')


def _InstallCuda10Point2(vm):
  """Installs CUDA Toolkit 10.2 from NVIDIA.

  Args:
    vm: VM to install CUDA on
  """
  basename = posixpath.basename(
      CUDA_10_2_TOOLKIT.format(
          os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)))
  vm.RemoteCommand(
      'wget -q %s' %
      CUDA_PIN.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArchPath(vm)))
  vm.RemoteCommand(f'sudo mv cuda-{_CudaOs(vm.OS_TYPE)}.pin '
                   '/etc/apt/preferences.d/cuda-repository-pin-600')
  vm.RemoteCommand('wget -q %s' % CUDA_10_2_TOOLKIT.format(
      os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm)))
  vm.RemoteCommand('sudo dpkg -i %s' % basename)
  vm.RemoteCommand('sudo apt-key add '
                   '/var/cuda-repo-10-2-local-10.2.89-440.33.01/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('cuda-toolkit-10-2 cuda-tools-10-2 cuda-libraries-10-2 '
                     'cuda-libraries-dev-10-2')


def _InstallCuda11Generic(vm, toolkit_fmt, version_dash):
  """Installs CUDA Toolkit 11.x from NVIDIA.

  Args:
    vm: VM to install CUDA on
    toolkit_fmt: format string to use for the toolkit name
    version_dash: Version (ie 11-1) to install
  """
  toolkit = toolkit_fmt.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArch(vm))
  basename = posixpath.basename(toolkit)
  vm.RemoteCommand(
      f'wget -q {CUDA_PIN.format(os=_CudaOs(vm.OS_TYPE), cpu_arch=_GetCpuArchPath(vm))}'
  )
  vm.RemoteCommand(f'sudo mv cuda-{_CudaOs(vm.OS_TYPE)}.pin '
                   '/etc/apt/preferences.d/cuda-repository-pin-600')
  vm.RemoteCommand(f'wget -q {toolkit}')
  vm.RemoteCommand(f'sudo dpkg -i {basename}')
  vm.RemoteCommand(
      'sudo apt-key add '
      f'/var/cuda-repo-{_CudaOs(vm.OS_TYPE)}-{version_dash}-local/7fa2af80.pub')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages(f'cuda-toolkit-{version_dash} '
                     f'cuda-tools-{version_dash} '
                     f'cuda-libraries-{version_dash} '
                     f'cuda-libraries-dev-{version_dash}')


def _InstallCuda11Point0(vm):
  _InstallCuda11Generic(vm, CUDA_11_0_TOOLKIT, '11-0')


def _InstallCuda11Point1(vm):
  _InstallCuda11Generic(vm, CUDA_11_1_TOOLKIT, '11-1')


def _InstallCuda11Point2(vm):
  _InstallCuda11Generic(vm, CUDA_11_2_TOOLKIT, '11-2')


def _InstallCuda11Point3(vm):
  _InstallCuda11Generic(vm, CUDA_11_3_TOOLKIT, '11-3')


def _InstallCuda11Point4(vm):
  _InstallCuda11Generic(vm, CUDA_11_4_TOOLKIT, '11-4')


def _InstallCuda11Point5(vm):
  _InstallCuda11Generic(vm, CUDA_11_5_TOOLKIT, '11-5')


def _InstallCuda11Point6(vm):
  _InstallCuda11Generic(vm, CUDA_11_6_TOOLKIT, '11-6')


def AptInstall(vm):
  """Installs CUDA toolkit on the VM if not already installed."""
  version_to_install = FLAGS.cuda_toolkit_version
  if (version_to_install == 'None' or not version_to_install):
    return
  current_version = GetCudaToolkitVersion(vm)
  if current_version == version_to_install:
    return

  cuda_path = f'/usr/local/cuda-{FLAGS.cuda_toolkit_version}'
  if vm.TryRemoteCommand(f'stat {cuda_path}'):
    vm.RemoteCommand('sudo rm -rf /usr/local/cuda', ignore_failure=True)
    vm.RemoteCommand(f'sudo ln -s {cuda_path} /usr/local/cuda')
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
  elif version_to_install == '11.0':
    _InstallCuda11Point0(vm)
  elif version_to_install == '11.1':
    _InstallCuda11Point1(vm)
  elif version_to_install == '11.2':
    _InstallCuda11Point2(vm)
  elif version_to_install == '11.3':
    _InstallCuda11Point3(vm)
  elif version_to_install == '11.4':
    _InstallCuda11Point4(vm)
  elif version_to_install == '11.5':
    _InstallCuda11Point5(vm)
  elif version_to_install == '11.6':
    _InstallCuda11Point6(vm)
  else:
    raise UnsupportedCudaVersionError()
  DoPostInstallActions(vm)
  # NVIDIA CUDA Profile Tools Interface.
  # This library provides advanced profiling support
  if version_to_install in ('9.0', '10.0'):
    # cupti is part of cuda>=10.1, and installed as cuda-cupti-10-1/2
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
  vm.RemoteCommand(f'rm -f cuda-repo-{_CudaOs(vm.OS_TYPE)}*')
  vm.RemoteCommand('sudo rm -rf {cuda_home}'.format(cuda_home=CUDA_HOME))
