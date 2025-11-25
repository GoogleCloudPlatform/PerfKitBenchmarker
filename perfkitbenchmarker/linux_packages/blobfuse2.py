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


_BLOBFUSE2_REPO_URL = 'https://github.com/Azure/azure-storage-fuse.git'
_BLOBFUSE2_REPO_NAME = 'azure-storage-fuse'
_BLOBFUSE2_VERSION = 'blobfuse2-2.5.0'


def AptInstall(vm):
  """Installs Blobfuse2 on the VM."""
  vm.InstallPackages('golang-go libfuse3-dev fuse3')
  vm.RemoteCommand(f'git clone {_BLOBFUSE2_REPO_URL}')
  vm.RemoteCommand(
      f'cd {_BLOBFUSE2_REPO_NAME} && git checkout {_BLOBFUSE2_VERSION} &&'
      ' ./build.sh'
  )
  vm.RemoteCommand(
      f'sudo cp {_BLOBFUSE2_REPO_NAME}/blobfuse2 /usr/local/bin/'
  )
