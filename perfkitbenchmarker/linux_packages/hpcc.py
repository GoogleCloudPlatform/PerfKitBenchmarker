# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing HPCC installation and cleanup functions.

The HPC Challenge is a collection of High Performance Computing benchmarks,
including High Performance Linpack (HPL). More information can be found here:
http://icl.cs.utk.edu/hpcc/
"""

import re

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import openblas

HPCC_TAR = 'hpcc-1.4.3.tar.gz'
HPCC_URL = 'http://icl.cs.utk.edu/projectsfiles/hpcc/download/' + HPCC_TAR
HPCC_DIR = '%s/hpcc-1.4.3' % vm_util.VM_TMP_DIR
MAKE_FLAVOR = 'Linux_PII_CBLAS'
HPCC_MAKEFILE = 'Make.' + MAKE_FLAVOR
HPCC_MAKEFILE_PATH = HPCC_DIR + '/hpl/' + HPCC_MAKEFILE


def _Install(vm):
  """Installs the HPCC package on the VM."""
  vm.Install('wget')
  vm.Install('openmpi')
  vm.Install('openblas')
  vm.RemoteCommand('wget %s -P %s' % (HPCC_URL, vm_util.VM_TMP_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (vm_util.VM_TMP_DIR, HPCC_TAR))
  vm.RemoteCommand(
      'cp %s/hpl/setup/%s %s' % (HPCC_DIR, HPCC_MAKEFILE, HPCC_MAKEFILE_PATH))
  sed_cmd = (
      'sed -i -e "/^MP/d" -e "s/gcc/mpicc/" -e "s/g77/mpicc/" '
      '-e "s/\\$(HOME)\\/netlib\\/ARCHIVES\\/Linux_PII/%s/" '
      '-e "s/libcblas.*/libopenblas.a/" '
      '-e "s/\\-lm/\\-lgfortran \\-lm/" %s' %
      (re.escape(openblas.OPENBLAS_DIR), HPCC_MAKEFILE_PATH))
  vm.RemoteCommand(sed_cmd)
  vm.RemoteCommand('cd %s; make arch=Linux_PII_CBLAS' % HPCC_DIR)


def YumInstall(vm):
  """Installs the HPCC package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the HPCC package on the VM."""
  _Install(vm)
