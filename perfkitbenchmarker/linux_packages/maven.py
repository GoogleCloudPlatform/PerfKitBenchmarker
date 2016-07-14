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


"""Module containing maven installation and cleanup functions."""

from perfkitbenchmarker import vm_util

MVN_TAR = 'apache-maven-3.3.9-bin.tar.gz'
MVN_URL = ('http://www.us.apache.org/dist/maven/maven-3/3.3.9/binaries/' +
           MVN_TAR)
MVN_DIR = '%s/apache-maven-3.3.9' % vm_util.VM_TMP_DIR


def _Install(vm):
  """Installs the maven package on the VM."""
  vm.Install('openjdk')
  vm.Install('wget')
  vm.RemoteCommand('wget %s -P %s' % (MVN_URL, vm_util.VM_TMP_DIR))
  vm.RemoteCommand('cd %s && tar xvzf %s' % (vm_util.VM_TMP_DIR, MVN_TAR))


def YumInstall(vm):
  """Installs the maven package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the maven package on the VM."""
  _Install(vm)
