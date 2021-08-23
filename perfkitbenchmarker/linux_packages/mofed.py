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


"""Module containing Mellanox OpenFabrics driver installation functions."""

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types

FLAGS = flags.FLAGS

flags.DEFINE_string('mofed_version', '5.0-2.1.8.0', 'Mellanox OFED version')

# TODO(tohaowu) Add DEBIAN9, CENTOS7, RHEL
MOFED_OS_MAPPING = {
    os_types.UBUNTU1604: 'ubuntu16.04',
    os_types.UBUNTU1604_CUDA9: 'ubuntu16.04',
    os_types.UBUNTU1804: 'ubuntu18.04',
}

# Mellanox OpenFabrics drivers
MLNX_OFED_DOWNLOAD_URL = ('https://www.mellanox.com/downloads/ofed/MLNX_OFED-'
                          '{version}/MLNX_OFED_LINUX-{version}-{os}-x86_64.tgz')


def _Install(vm):
  """Installs the Mellanox OpenFabrics driver on the VM."""
  if vm.OS_TYPE not in MOFED_OS_MAPPING:
    raise ValueError('OS type {} not in {}'.format(vm.OS_TYPE,
                                                   sorted(MOFED_OS_MAPPING)))
  driver = MLNX_OFED_DOWNLOAD_URL.format(version=FLAGS.mofed_version,
                                         os=MOFED_OS_MAPPING[vm.OS_TYPE])
  vm.InstallPackages('libdapl2 libmlx4-1')
  vm.RemoteCommand(f'wget --retry-connrefused --tries=3 --waitretry=5 {driver}')
  vm.RemoteCommand('tar zxvf MLNX_OFED_LINUX-*-x86_64.tgz')
  stdout, _ = vm.RemoteCommand('cd MLNX_OFED_LINUX-*-x86_64 && sudo '
                               './mlnxofedinstall --force --skip-repo')
  if 'Installation passed successfully' not in stdout:
    raise errors.Benchmarks.PrepareException(
        'Mellanox OpenFabrics driver isn\'t installed successfully.')
  vm.RemoteCommand('sudo /etc/init.d/openibd restart')
  vm.RemoteCommand("sudo sed -i -e 's/# OS.EnableRDMA=y/"
                   "OS.EnableRDMA=y/g' /etc/waagent.conf")
  vm.RemoteCommand("sudo sed -i -e 's/# OS.UpdateRdmaDriver=y/"
                   "OS.UpdateRdmaDriver=y/g' /etc/waagent.conf")
  vm.Reboot()
  # Check IB status.
  stdout, _ = vm.RemoteCommand('sudo ibdev2netdev -v')
  if 'port 1 (ACTIVE) ==> ib0 (Up)' not in stdout:
    raise errors.Benchmarks.PrepareException('Infiniband is not up.')


def YumInstall(vm):
  """Installs the Mellanox OpenFabrics driver on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Mellanox OpenFabrics driver on the VM."""
  _Install(vm)
