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


"""Module containing mariadb installation and cleanup functions."""

MYSQL_PSWD = 'perfkitbenchmarker'

MARIADB_CONFIG_PATH = '/etc/mysql/mariadb.conf.d/50-server.cnf'

MARIADB = 'mariadb'


def AptInstall(vm):
  """Installs the mysql package on the VM."""
  vm.InstallPackages('mariadb-server')
  vm.RemoteCommand(f"""sudo mysql_secure_installation <<EOF

# Set root password (modify as needed)
y
{MYSQL_PSWD}
{MYSQL_PSWD}
n
y
y
y
EOF
""")


def AptGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  del vm
  return MARIADB_CONFIG_PATH


def AptGetServiceName(vm):
  """Returns the name of the mysql service."""
  del vm
  return MARIADB
