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
"""Module containing go lang package installation and cleanup."""

from perfkitbenchmarker.linux_packages import INSTALL_DIR

PACKAGE_NAME = 'go_lang'

# Download  go language release binary. When the binary need to be updated to
# to a new version, please update the value of GO_TAR.
GO_TAR = 'go1.12.9.linux-amd64.tar.gz'
GO_URL = 'https://dl.google.com/go/' + GO_TAR
PREPROVISIONED_DATA = {
    GO_TAR: 'ac2a6efcc1f5ec8bdc0db0a988bb1d301d64b6d61b7e8d9e42f662fbb75a2b9b'
}
PACKAGE_DATA_URL = {GO_TAR: GO_URL}
GO_VERSION = '1.12.9'
GO_DIR = '%s/go-%s' % (INSTALL_DIR, GO_VERSION)
GO_BIN = '/usr/local/go/bin/go'


def Install(vm):
  """Install go lang package on the VM."""
  vm.InstallPreprovisionedPackageData(PACKAGE_NAME, PREPROVISIONED_DATA.keys(),
                                      INSTALL_DIR)
  vm.RemoteCommand('cd %s && sudo tar -C /usr/local -xzf %s' %
                   (INSTALL_DIR, GO_TAR))
  vm.RemoteCommand('export PATH=$PATH:/usr/local/go/bin')


def Uninstall(_):
  """Uninstalls go lang package on the VM."""
  # No clean way to uninstall everything. The VM will be deleted at the end
  # of the test.
  pass
