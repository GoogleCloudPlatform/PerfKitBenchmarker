# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module allows instalation Extra Packages for Enterprise Linux repository."""

import posixpath
import re
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR

EPEL6_RPM = 'epel-release-6-8.noarch.rpm'
EPEL7_RPM = 'epel-release-7-11.noarch.rpm'
PACKAGE_NAME = 'epel_release'
EPEL_BASE_URL = 'https://dl.fedoraproject.org/pub/epel/'
EPEL_URL_PATH = 'x86_64/Packages/e'

PREPROVISIONED_DATA = {
    EPEL6_RPM:
        'e5ed9ecf22d0c4279e92075a64c757ad2b38049bcf5c16c4f2b75d5f6860dc0d',
    EPEL7_RPM:
        '33b8b8250960874014d7c66d04ef12df3a71277b9eb6f2146bba7553daeaf910'
}
PACKAGE_DATA_URL = {
    EPEL6_RPM: posixpath.join(
        EPEL_BASE_URL, '6', EPEL_URL_PATH, EPEL6_RPM),
    EPEL7_RPM: posixpath.join(
        EPEL_BASE_URL, '7', EPEL_URL_PATH, EPEL7_RPM)
}


def AptInstall(vm):
  del vm
  raise NotImplementedError()


def YumInstall(vm):
  """Installs epel-release repo."""
  try:
    vm.InstallPackages('epel-release')
  except errors.VirtualMachine.RemoteCommandError:
    stdout, _ = vm.RemoteCommand('cat /etc/redhat-release')
    major_version = int(re.search('release ([0-9])', stdout).group(1))
    if major_version == 6:
      epel_rpm = EPEL6_RPM
    elif major_version == 7:
      epel_rpm = EPEL7_RPM
    else:
      raise
    vm.InstallPreprovisionedPackageData(
        PACKAGE_NAME,
        PREPROVISIONED_DATA.keys(),
        INSTALL_DIR)
    vm.RemoteCommand('sudo rpm -ivh --force %s' % posixpath.join(
        INSTALL_DIR, epel_rpm))
  vm.InstallPackages('yum-utils')
  vm.RemoteCommand('sudo yum-config-manager --enable epel')
