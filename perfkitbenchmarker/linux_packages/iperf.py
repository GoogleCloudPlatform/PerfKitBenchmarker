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


"""Module containing iperf installation and cleanup functions."""

import re

from perfkitbenchmarker import errors

IPERF_EL6_RPM = ('http://pkgs.repoforge.org/iperf/'
                 'iperf-2.0.4-1.el6.rf.x86_64.rpm')
IPERF_EL7_RPM = ('http://pkgs.repoforge.org/iperf/'
                 'iperf-2.0.4-1.el7.rf.x86_64.rpm')


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.InstallPackages('iperf')


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  try:
    vm.InstallEpelRepo()
    _Install(vm)
  # RHEL 7 does not have an iperf package in the standard/EPEL repositories
  except errors.VirtualMachine.RemoteCommandError as e:
    stdout, _ = vm.RemoteCommand('cat /etc/redhat-release')
    major_version = int(re.search('release ([0-9])', stdout).group(1))
    if major_version == 6:
      iperf_rpm = IPERF_EL6_RPM
    elif major_version == 7:
      iperf_rpm = IPERF_EL7_RPM
    else:
      raise e
    vm.RemoteCommand('sudo rpm -ivh %s' % iperf_rpm)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
