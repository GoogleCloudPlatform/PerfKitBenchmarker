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
"""Test for managed relational database provisioning.

This is a set of benchmarks that measures performance of SQL Databases on
managed SQL services.

- On AWS, we will use RDS+MySQL and RDS PostgreSQL.
- On GCP, we will use Cloud SQL v2 (Performance Edition).
As other cloud providers deliver a managed SQL service, we will add it here.

To run this benchmark the following flags may be used:
- database: {mysql, postgres} Declares type of database.
- database_name: Defaults to 'pkb-db-{run_uri}'.
- database_username: Defaults to 'pkb-db-user-{run_uri}'.
- database_password: Defaults to random 10-character alpha-numeric string.
- database_version: {5.7, 9.6} Defaulted to latest for type.
- high_availability: Boolean.
- data_disk_size:
- cpu:
- ram:


As of June 2017 to make this benchmark run for GCP you must install the
gcloud beta component. This is necessary because creating a Cloud SQL instance
with a non-default storage size is in beta right now. This can be removed when
this feature is part of the default components.
See https://cloud.google.com/sdk/gcloud/reference/beta/sql/instances/create
for more information.
To run this benchmark for GCP it is required to install a non-default gcloud
component. Otherwise this benchmark will fail.
To ensure that gcloud beta is installed, type
        'gcloud components list'
into the terminal. This will output all components and status of each.
Make sure that
  name: gcloud Beta Commands
  id:  beta
has status: Installed.
If not, run
        'gcloud components install beta'
to install it. This will allow this benchmark to properly create an instance.
"""

from perfkitbenchmarker import configs

BENCHMARK_NAME = 'postgres_managed_relational_db_test'
BENCHMARK_CONFIG = """
postgres_managed_relational_db_test:
  description: test managed relational database provisioning
  managed_relational_db:
    database: postgres
    database_version: '9.6'
    vm_spec:
      GCP:
        machine_type:
          cpus: 1
          memory: 3840MiB
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
