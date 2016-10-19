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


"""Module containing aerospike C client installation and cleanup functions."""

from perfkitbenchmarker import data

AEROSPIKE_CLIENT = 'https://github.com/aerospike/aerospike-client-c.git'
CLIENT_DIR = 'aerospike-client-c'
CLIENT_VERSION = '4.0.4'
PATCH_FILE = 'aerospike.patch'


def _Install(vm):
  """Installs the aerospike client on the VM."""
  vm.Install('build_tools')
  vm.Install('lua5_1')
  vm.Install('openssl')
  clone_command = 'git clone %s'
  vm.RemoteCommand(clone_command % AEROSPIKE_CLIENT)
  build_command = ('cd %s && git checkout %s && git submodule update --init '
                   '&& make')
  vm.RemoteCommand(build_command % (CLIENT_DIR, CLIENT_VERSION))

  # Apply a patch to the client benchmark so we have access to average latency
  # of requests. Switching over to YCSB should obviate this.
  vm.PushDataFile(PATCH_FILE)
  benchmark_dir = '%s/benchmarks/src/main' % CLIENT_DIR
  vm.RemoteCommand('cp aerospike.patch %s' % benchmark_dir)
  vm.RemoteCommand('cd %s && patch -p1 -f  < aerospike.patch' % benchmark_dir)
  vm.RemoteCommand('sed -i -e "s/lpthread/lpthread -lz/" '
                   '%s/benchmarks/Makefile' % CLIENT_DIR)
  vm.RemoteCommand('cd %s/benchmarks && make' % CLIENT_DIR)


def AptInstall(vm):
  """Installs the aerospike client on the VM."""
  _Install(vm)


def YumInstall(vm):
  """Installs the aerospike client on the VM."""
  _Install(vm)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(PATCH_FILE)


def Uninstall(vm):
  vm.RemoteCommand('sudo rm -rf aerospike-client-c')
