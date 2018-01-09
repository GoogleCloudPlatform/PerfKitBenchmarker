# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

This binary was built by NVIDIA with GCC 4.8.5,
OpenMPI 1.10.2, and CUDA 9 with support for Volta, Kepler,
Maxwell, and Pascal chips.

There is also an older version with CUDA 8 support.
"""

import posixpath
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import INSTALL_DIR

HPCG_CUDA_9 = 'http://www.hpcg-benchmark.org/downloads/hpcg-3.1_cuda9_ompi1.10.2_gcc485_sm_35_sm_50_sm_60_sm_70_ver_10_8_17.tgz'
HPCG_CUDA_8 = 'http://www.hpcg-benchmark.org/downloads/hpcg-3.1_cuda8_ompi1.10.2_gcc485_sm_35_sm_50_sm_60_ver_3_28_17.tgz'
HPCG_DIR = '%s/%s' % (INSTALL_DIR, 'hpcg')

HPCG_CUDA_8_BINARY = 'xhpcg-3.1_gcc_485_cuda8061_ompi_1_10_2_sm_35_sm_50_sm_60_ver_3_28_17'
HPCG_CUDA_9_BINARY = 'xhpcg-3.1_gcc_485_cuda90176_ompi_1_10_2_sm_35_sm_50_sm_60_sm_70_ver_10_8_17'

FLAGS = flags.FLAGS


def AptInstall(vm):
  """Install the HPCG package on the VM.

  Args:
    vm: vm to target
  """
  vm.Install('wget')
  vm.InstallPackages('libopenmpi-dev numactl')
  vm.Install('cuda_toolkit')

  if FLAGS.cuda_toolkit_version == '8.0':
    hpcg_version = HPCG_CUDA_8
    hpcg_binary = HPCG_CUDA_8_BINARY
  elif FLAGS.cuda_toolkit_version == '9.0':
    hpcg_version = HPCG_CUDA_9
    hpcg_binary = HPCG_CUDA_9_BINARY
  else:
    raise cuda_toolkit.UnsupportedCudaVersionException(
        'HPCG only supports CUDA 8 and CUDA 9')

  vm.RemoteCommand('cd %s && wget %s' % (INSTALL_DIR, hpcg_version))
  vm.RemoteCommand('rm -rf %s' % HPCG_DIR)
  vm.RemoteCommand('mkdir %s' % HPCG_DIR)
  vm.RemoteCommand('cd %s && tar xvf %s --directory=%s --strip-components=1' %
                   (INSTALL_DIR,
                    posixpath.basename(hpcg_version),
                    HPCG_DIR))
  # Create a symlink from the hpcg binary to 'hpcg'
  vm.RemoteCommand('cd %s && ln -s %s %s' % (HPCG_DIR, hpcg_binary, 'hpcg'))


def YumInstall(_):
  """Install the HPCG package on the VM using yum."""
  raise NotImplementedError(
      'Installation of HPCG is only supported on Ubuntu')
