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

import posixpath

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR

PACKAGE_NAME = 'iperf'
IPERF_ZIP = '2.0.4-RELEASE.zip'
IPERF_DIR = 'iperf-2.0.4-RELEASE'
PREPROVISIONED_DATA = {
    IPERF_ZIP: 'c3aec97a653fa8705dc167700d002963'
}
PACKAGE_DATA_URL = {
    IPERF_ZIP: posixpath.join('https://github.com/esnet/iperf/archive',
                              IPERF_ZIP)}


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.InstallPackages('iperf')


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  try:
    vm.InstallEpelRepo()
    _Install(vm)
  # RHEL 7 does not have an iperf package in the standard/EPEL repositories
  except errors.VirtualMachine.RemoteCommandError:
    vm.Install('build_tools')
    vm.Install('unzip')
    vm.InstallPreprovisionedPackageData(
        PACKAGE_NAME, PREPROVISIONED_DATA.keys(), INSTALL_DIR)
    vm.RemoteCommand(
        'cd %s; unzip %s; cd %s; ./configure; make; sudo make install' % (
            INSTALL_DIR, IPERF_ZIP, IPERF_DIR))


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
