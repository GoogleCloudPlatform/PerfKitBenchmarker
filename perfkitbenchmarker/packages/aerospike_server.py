# Copyright 2014 Google Inc. All rights reserved.
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


"""Module containing aerospike server installation and cleanup functions."""

from perfkitbenchmarker import vm_util

GIT_REPO = 'https://github.com/aerospike/aerospike-server.git'
GIT_TAG = '3.3.19'
AEROSPIKE_DIR = '%s/aerospike-server' % vm_util.VM_TMP_DIR
AEROSPIKE_CONF_PATH = '%s/as/etc/aerospike_dev.conf' % AEROSPIKE_DIR


def _Install(vm):
  """Installs the Aerospike server on the VM."""
  vm.Install('build_tools')
  vm.Install('lua5_1')
  vm.Install('openssl')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, AEROSPIKE_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1} && git submodule update --init '
                   '&& make'.format(AEROSPIKE_DIR, GIT_TAG))


def YumInstall(vm):
  """Installs the memtier package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the memtier package on the VM."""
  _Install(vm)
