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
"""Runs a BigQuery search index benchmark."""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import bigquery

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'bq_search_index'
BENCHMARK_CONFIG = """
bq_search_index:
  description: Runs a BigQuery search index benchmark.
  flags:
    cloud: GCP
    bq_client_interface: PYTHON
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-16
          zone: us-central1-f
"""

_TABLE = flags.DEFINE_string(
    'bq_search_index_table',
    None,
    'The table to use for the search index benchmark.',
)
_INDEX_COLUMNS = flags.DEFINE_list(
    'bq_search_index_columns',
    [],
    'The columns to include in the search index.',
)


def GetConfig(user_config):
  """Load and return benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and set up the benchmark."""
  # TODO(user): Add any necessary setup logic here.
  pass


def Run(benchmark_spec):
  """Run the benchmark and return a list of samples."""
  # TODO(user): Implement the benchmark logic here.
  return []


def Cleanup(benchmark_spec):
  """Cleanup the benchmark."""
  # TODO(user): Add any necessary cleanup logic here.
  pass
