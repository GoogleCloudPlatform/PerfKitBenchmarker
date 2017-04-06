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


"""Module containing SHOC Benchmark Suite installation and
   cleanup functions.
"""


from perfkitbenchmarker.linux_packages import INSTALL_DIR
import os


SHOC_GIT_URL = 'https://github.com/vetter/shoc.git'
SHOC_DIR = '%s/shoc' % INSTALL_DIR
SHOC_BIN_DIR = os.path.join(SHOC_DIR, 'bin')
SHOC_PATCH = 'shoc_config.patch'
APT_PACKAGES = 'wget automake git zip libopenmpi-dev'


def _IsShocInstalled(vm):
  """Returns whether shoc is installed or not"""
  command = os.path.join(SHOC_BIN_DIR, 'shocdriver')
  resp, _ = vm.RemoteHostCommand('command -v %s' % command,
                                 ignore_failure=True,
                                 suppress_warning=True)
  if resp.rstrip() == "":
    return False
  return True


def AptInstall(vm):
  """Installs the SHOC benchmark suite on the VM."""
  if _IsShocInstalled(vm):
    return

  vm.InstallPackages(APT_PACKAGES)
  vm.Install('cuda_toolkit_8')

  vm.RemoteCommand('cd %s && git clone %s' % (INSTALL_DIR, SHOC_GIT_URL))
  vm.RemoteCommand(('cd %s && ./configure '
                    'CUDA_CPPFLAGS=-gencode=arch=compute_37,code=compute_37 '
                    'NVCC=/usr/local/cuda/bin/nvcc')
                   % SHOC_DIR)
  vm.RemoteCommand('cd %s && make -j8 && make install' % SHOC_DIR)


def YumInstall(vm):
  """TODO: PKB currently only supports the installation of SHOC
     on Ubuntu.
  """
  raise NotImplementedError()
