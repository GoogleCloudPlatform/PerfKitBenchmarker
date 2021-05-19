# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing install postgresql server.
"""


def AptInstall(vm):
  """Installs the postgres package on the VM."""
  vm.RemoteCommand(
      'sudo sh -c \'echo '
      '"deb https://apt.postgresql.org/pub/repos/apt '
      '$(lsb_release -cs)-pgdg main" '
      '> /etc/apt/sources.list.d/pgdg.list\'')

  vm.RemoteCommand(
      'wget --quiet -O - '
      'https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get -y install postgresql-13')
