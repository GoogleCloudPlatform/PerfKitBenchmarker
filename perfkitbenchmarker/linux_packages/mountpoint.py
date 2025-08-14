# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module for installing the S3 Mountpoint package."""

from perfkitbenchmarker import linux_packages

_DEB_URL_BY_ARCH = {
    'x86_64': (
        'https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb'
    ),
    'aarch64': (
        'https://s3.amazonaws.com/mountpoint-s3-release/latest/arm64/mount-s3.deb'
    ),
}
_RPM_URL_BY_ARCH = {
    'x86_64': (
        'https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm'
    ),
    'aarch64': (
        'https://s3.amazonaws.com/mountpoint-s3-release/latest/arm64/mount-s3.rpm'
    ),
}


def AptInstall(vm):
  """Installs the S3 Mountpoint package on the VM."""
  url = _DEB_URL_BY_ARCH.get(vm.cpu_arch)
  _Install(vm, url, 'mount-s3.deb')


def YumInstall(vm):
  """Installs the S3 Mountpoint package on the VM."""
  url = _RPM_URL_BY_ARCH.get(vm.cpu_arch)
  _Install(vm, url, 'mount-s3.rpm')


def _Install(vm, url, out_file):
  """Installs the S3 Mountpoint package on the VM.

  Args:
    vm: The VM to install the package on.
    url: The URL to download the package from.
    out_file: The name of the file to save the downloaded package as.

  Raises:
    ValueError: If the VM's CPU architecture is not supported.
  """
  if not url:
    raise ValueError(f'Unsupported architecture: {vm.cpu_arch}')

  vm.InstallPackages('wget')
  vm.RemoteCommand(
      f'sudo mkdir -p {linux_packages.INSTALL_DIR} && sudo wget {url} -O'
      f' {linux_packages.INSTALL_DIR}/{out_file}'
  )
  vm.RemoteCommand(
      f'cd {linux_packages.INSTALL_DIR} && sudo apt-get install -y ./{out_file}'
  )
  vm.RemoteCommand('mount-s3 --version')
