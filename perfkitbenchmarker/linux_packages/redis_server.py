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


"""Module containing redis installation and cleanup functions."""

from perfkitbenchmarker import vm_util

REDIS_TAR = 'redis-2.8.9.tar.gz'
REDIS_DIR = '%s/redis-2.8.9' % vm_util.VM_TMP_DIR
REDIS_URL = 'http://download.redis.io/releases/' + REDIS_TAR


def _Install(vm):
  """Installs the redis package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand('wget %s -P %s' % (REDIS_URL, vm_util.VM_TMP_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (vm_util.VM_TMP_DIR, REDIS_TAR))
  vm.RemoteCommand('cd %s && make' % REDIS_DIR)


def YumInstall(vm):
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-dev')
  _Install(vm)
