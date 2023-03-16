# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark for timing cryptographic keys.

All timing and reporting is handled by resource.py. This simply gives a
convenient place to define the Benchmark Config
"""

from typing import List

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'provision_key'

BENCHMARK_CONFIG = """
provision_key:
  description: >
      Time spinning up a cryptographic key.
  key:
    cloud: GCP
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def Run(_) -> List[sample.Sample]:
  return []


def Cleanup(_):
  pass
