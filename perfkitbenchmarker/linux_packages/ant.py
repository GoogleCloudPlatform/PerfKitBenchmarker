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


"""Module containing Ant installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import linux_packages

ANT_TAR = 'apache-ant-1.9.6-bin.tar.gz'
ANT_TAR_URL = 'https://archive.apache.org/dist/ant/binaries/' + ANT_TAR

PACKAGE_NAME = 'ant'
PREPROVISIONED_DATA = {
    ANT_TAR:
        '90d28c0202871bd9875a5da6d982f362bb3114d346b9d8ae58860b8d3312c21c'}
PACKAGE_DATA_URL = {ANT_TAR: ANT_TAR_URL}
ANT_HOME_DIR = posixpath.join(linux_packages.INSTALL_DIR, PACKAGE_NAME)


def _Install(vm):
  """Installs the Ant package on the VM."""
  vm.Install('wget')
  vm.InstallPreprovisionedPackageData(PACKAGE_NAME, PREPROVISIONED_DATA.keys(),
                                      linux_packages.INSTALL_DIR)
  vm.RemoteCommand('cd {0}  && tar -zxf apache-ant-1.9.6-bin.tar.gz && '
                   'ln -s {0}/apache-ant-1.9.6/ {1}'.format(
                       linux_packages.INSTALL_DIR, ANT_HOME_DIR))


def YumInstall(vm):
  """Installs the Ant package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Ant package on the VM."""
  _Install(vm)
