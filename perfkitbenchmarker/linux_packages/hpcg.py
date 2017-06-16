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

"""Module containing HPCG.

This binary was built by the HPCG team on Centos 7.2 system with gcc 4.8.5,
cuda version 8.0.61 and OpenMPI version 1.6.5.
A second version was built with openmpi 1.10.2.
"""

from perfkitbenchmarker.linux_packages import INSTALL_DIR

HPCG_NAME = 'hpcg-3.1_cuda8_ompi1.10.2_gcc485_sm_35_sm_50_sm_60_ver_3_28_17'
HPCG_TAR = '%s.tgz' % HPCG_NAME
HPCG_URL = 'http://www.hpcg-benchmark.org/downloads/%s' % HPCG_TAR
HPCG_DIR = '%s/%s' % (INSTALL_DIR, HPCG_NAME)


def AptInstall(vm):
  """Install the HPCG package on the VM.

  Args:
    vm: vm to target
  """
  vm.Install('wget')
  vm.InstallPackages('libopenmpi-dev numactl')
  vm.Install('cuda_toolkit_8')
  vm.RemoteCommand('cd %s && wget %s' % (INSTALL_DIR, HPCG_URL))
  vm.RemoteCommand('cd %s && tar xvf %s' % (INSTALL_DIR, HPCG_TAR))


def YumInstall(_):
  """Install the HPCG package on the VM using yum."""
  raise NotImplementedError(
      'Installation of HPCG is only supported on Ubuntu')
