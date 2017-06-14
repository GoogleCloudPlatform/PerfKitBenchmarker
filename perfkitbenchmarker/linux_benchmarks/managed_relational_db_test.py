# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Test for managed relational database provisioning"""

from perfkitbenchmarker import configs

BENCHMARK_NAME = 'managed_relational_db_test'
BENCHMARK_CONFIG = """
managed_relational_db_test:
  description: test managed relational database provisioning
  managed_relational_db:
    database: mysql
    database_version: '5.6'
    vm_spec:
      GCP:
        machine_type: n1-standard-1
        zone: us-central1-c
      AWS:
        machine_type: db.t1.micro
        zone: us-west-2a
    disk_spec:
      GCP:
        disk_size: 50
        disk_type: standard
      AWS:
        disk_size: 5
        disk_type: gp2
  vm_groups:
    client:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  pass


def Prepare(benchmark_spec):
  vm = benchmark_spec.vms[0]
  vm.InstallPackages('mysql-client')


def Run(benchmark_spec):
  vm = benchmark_spec.vms[0]
  db = benchmark_spec.managed_relational_db
  db_endpoint = db.GetEndpoint()
  db_port = db.GetPort()
  db_username = db.GetUsername()
  db_password = db.GetPassword()
  print db_endpoint
  print db_port
  print db.GetUsername()
  print db.GetPassword()
  stdout, _ = vm.RemoteCommand(
      "mysql -h {0} -P {1} -u {2} --password={3} "
      "-e \'SHOW VARIABLES LIKE \"%version%\";\'"
      .format(db_endpoint, db_port, db_username, db_password))
  print stdout


  return []


def Cleanup(benchmark_spec):
  pass
