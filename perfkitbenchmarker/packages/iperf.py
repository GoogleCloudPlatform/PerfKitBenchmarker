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


"""Module containing iperf installation and cleanup functions."""

from perfkitbenchmarker import errors

IPERF_RPM = 'http://pkgs.repoforge.org/iperf/iperf-2.0.4-1.el7.rf.x86_64.rpm'


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.InstallPackages('iperf')


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  try:
    vm.InstallEpelRepo()
    _Install(vm)
  # RHEL 7 does not have an iperf package in the standard/EPEL repositories
  except errors.VmUtil.SshConnectionError:
    vm.RemoteCommand('sudo rpm -ivh %s' % IPERF_RPM)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
