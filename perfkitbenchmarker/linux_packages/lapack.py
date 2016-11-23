# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing lapack installation and cleanup functions."""

import os
from perfkitbenchmarker import vm_util

LAPACK_VERSION = '3.6.1'
LAPACK_FOLDER = 'lapack-%s' % LAPACK_VERSION
LAPACK_TAR = '%s.tgz' % LAPACK_FOLDER
LAPACK_URL = 'http://www.netlib.org/lapack/%s' % LAPACK_TAR
LAPACK_DIR = os.path.join(vm_util.VM_TMP_DIR, LAPACK_FOLDER)


def _Install(vm):
  """Install LAPACK lib."""
  vm.Install('fortran')
  vm.Install('cmake')
  vm.RemoteCommand(
      'cd %s; wget %s; tar xf %s' % (
          vm_util.VM_TMP_DIR, LAPACK_URL, LAPACK_TAR))
  vm.RemoteCommand(
      'cd %s; mv make.inc.example make.inc; cmake .; make -j %s' % (
          LAPACK_DIR, vm.num_cpus))
  vm.RemoteCommand(
      'cd %s; make -j %s' % (os.path.join(LAPACK_DIR, 'BLAS'), vm.num_cpus))


def YumInstall(vm):
  """Installs the lapack package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the lapack package on the VM."""
  _Install(vm)
