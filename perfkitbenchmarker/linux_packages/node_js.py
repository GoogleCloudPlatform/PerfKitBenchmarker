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


"""Module containing node.js installation and cleanup functions."""

from perfkitbenchmarker import vm_util

GIT_REPO = 'https://github.com/joyent/node.git'
GIT_TAG = 'v0.11.14'
NODE_DIR = '%s/node' % vm_util.VM_TMP_DIR


def _Install(vm):
  """Installs the node.js package on the VM."""
  vm.Install('build_tools')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, NODE_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(NODE_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && ./configure --prefix=/usr'.format(NODE_DIR))
  vm.RemoteCommand('cd {0} && make && sudo make install'.format(NODE_DIR))


def YumInstall(vm):
  """Installs the node.js package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the node.js package on the VM."""
  _Install(vm)


def _Uninstall(vm):
  """Uninstalls the node.js package on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(NODE_DIR))


def YumUninstall(vm):
  """Uninstalls the node.js package on the VM."""
  _Uninstall(vm)


def AptUninstall(vm):
  """Uninstalls the node.js package on the VM."""
  _Uninstall(vm)
