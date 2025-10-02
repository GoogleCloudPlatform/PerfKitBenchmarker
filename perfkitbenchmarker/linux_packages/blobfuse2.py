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
"""Module for installing the Blobfuse2 package."""

from perfkitbenchmarker import os_types


_UBUNTU24_PACKAGE_REPO = 'https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb'


def AptInstall(vm):
  """Installs Blobfuse2 on the VM."""
  if vm.OS_TYPE != os_types.UBUNTU2404:
    raise NotImplementedError(
        'Only installation of Blobfuse2 on Ubuntu 24.04 is supported.'
    )
  vm.InstallPackages('wget')
  vm.RemoteCommand(f'sudo wget {_UBUNTU24_PACKAGE_REPO}')
  vm.RemoteCommand('sudo dpkg -i packages-microsoft-prod.deb')
  vm.InstallPackages('libfuse3-dev fuse3 blobfuse2')
