# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Measures the performance of provisioning a relational database.

Provisioning metrics are automatically collected by benchmark_spec.py, which
is why we do not need to do anything in the prepare/run phases of this file.
"""

from absl import flags
from perfkitbenchmarker import configs

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'provision_relational_db'
BENCHMARK_CONFIG = """
provision_relational_db:
  description: Measures the performance of provisioning a relational database.
  relational_db:
    engine: postgres
    db_spec:
      GCP:
        machine_type: db-n1-standard-16
        zone: us-central1-c
      AWS:
        machine_type: db.m4.4xlarge
        zone: us-east-1a
      Azure:
        machine_type: GP_Gen5_2
        zone: westus
    db_disk_spec:
      GCP:
        disk_size: 128
        disk_type: pd-ssd
      AWS:
        disk_size: 128
        disk_type: gp2
      Azure:
        disk_size: 128
    vm_groups:
      servers:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_B4ms
            zone: westus
        disk_spec: *default_500_gb
      clients:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_B4ms
            zone: westus
        disk_spec:
          GCP:
            disk_size: 500
            disk_type: pd-ssd
          AWS:
            disk_size: 500
            disk_type: gp2
          Azure:
            disk_size: 500
            disk_type: Premium_LRS
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def Run(_):
  return []


def Cleanup(_):
  pass
