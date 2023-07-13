# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing Spanner PGAdapter installation functions.

PGAdapter allows the VM to communicate with a PostgreSQL-enabled Spanner
database. See https://cloud.google.com/spanner/docs/pgadapter.
"""

PGADAPTER_URL = 'https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz'


def AptInstall(vm):
  """Installs the pgadapter package on the VM."""
  # psmisc helps with restarting PGAdapter.
  vm.InstallPackages('default-jre psmisc')
  vm.RemoteCommand(f'wget {PGADAPTER_URL}')
  vm.RemoteCommand('tar -xzvf pgadapter.tar.gz')
