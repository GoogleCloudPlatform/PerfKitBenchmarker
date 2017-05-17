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

import os
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'managed_relational_db_test'
BENCHMARK_CONFIG = """
managed_relational_db_test:
  description: test managed relational database provisioning
  managed_relational_db:
    database: mysql
    version: '5.6'
    vm_spec:
      GCP:
        machine_type: n1-standard-1
      AWS:
        machine_type: db.t2.small
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
  pass


def Run(benchmark_spec):
  return []

def Cleanup(benchmark_spec):
  pass
