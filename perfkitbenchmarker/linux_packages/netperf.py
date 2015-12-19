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


"""Module containing netperf installation and cleanup functions."""

from perfkitbenchmarker import vm_util

NETPERF_TAR = 'netperf-2.6.0.tar.gz'
NETPERF_URL = 'ftp://ftp.netperf.org/netperf/archive/%s' % NETPERF_TAR
NETPERF_DIR = '%s/netperf-2.6.0' % vm_util.VM_TMP_DIR
NETSERVER_PATH = NETPERF_DIR + '/src/netserver'
NETPERF_PATH = NETPERF_DIR + '/src/netperf'


def _Install(vm):
  """Installs the netperf package on the VM."""
  vm.Install('build_tools')
  vm.Install('curl')
  vm.RemoteCommand('curl %s -o %s/%s' % (
      NETPERF_URL, vm_util.VM_TMP_DIR, NETPERF_TAR))
  vm.RemoteCommand('cd %s && tar xvzf %s' % (vm_util.VM_TMP_DIR, NETPERF_TAR))
  vm.RemoteCommand('cd %s && ./configure && make' % NETPERF_DIR)


def YumInstall(vm):
  """Installs the netperf package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the netperf package on the VM."""
  _Install(vm)
