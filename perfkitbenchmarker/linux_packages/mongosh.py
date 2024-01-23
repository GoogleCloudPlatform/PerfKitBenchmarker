# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing mongosh installation and cleanup functions.

See https://www.mongodb.com/docs/mongodb-shell/ for more details.
"""

from perfkitbenchmarker.linux_packages import mongodb_server


def YumInstall(vm):
  """Installs the mongodb package on the VM."""
  mongodb_server.YumSetup(vm)
  vm.InstallPackages('mongodb-mongosh')


def AptInstall(vm):
  """Installs the mongodb package on the VM."""
  mongodb_server.AptSetup(vm)
  vm.InstallPackages('mongodb-org')


def RunCommand(vm, command: str) -> tuple[str, str]:
  """Runs a mongosh command on the VM."""
  return vm.RemoteCommand(f'mongosh --eval "{command}" --verbose')
