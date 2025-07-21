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






"""TODO

Run command:

./pkb.py \
--cloud=GCP  \
--benchmarks=edw_create_index_benchmark \
--bq_client_interface=PYTHON  \
--config_override=edw_create_index_benchmark.edw_service.type=bigquery \
--config_override=edw_create_index_benchmark.edw_service.cluster_identifier=p3rf-bq-search.search_index_dataset \
--gcp_service_account=bigquery-testing-pkb@p3rf-bigquery-smallquery-slots.iam.gserviceaccount.com \
--gcp_service_account_key_file=/Users/saksena/Downloads/p3rf-bigquery-smallquery-slots.json \
--edw_index_creation_query_dir=edw/bigquery/search_index/create_index \
--edw_index_deletion_query_dir=edw/bigquery/search_index/delete_index \
--metadata=cloud:GCP \
--project=saksena-test \
--zones=us-central1-c 
"""

"""Benchmark for creating an index in an EDW service."""

import logging
import os
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'edw_create_index_benchmark'

BENCHMARK_CONFIG = """
edw_create_index_benchmark:
  description: Benchmark for creating an index in an EDW service.
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

flags.DEFINE_string(
    'edw_index_creation_query_dir',
    '',
    'Optional local directory containing all query files. '
    'Can be absolute or relative to the executable.',
)

flags.DEFINE_string(
    'edw_index_deletion_query_dir',
    '',
    'Optional local directory containing all query files. '
    'Can be absolute or relative to the executable.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Loads and returns the benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepares the client VM to run the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  benchmark_spec.always_call_cleanup = True
  edw_service_instance = benchmark_spec.edw_service
  vm = benchmark_spec.vms[0]

  edw_service_instance.GetClientInterface().SetProvisionedAttributes(
      benchmark_spec
  )
  edw_service_instance.GetClientInterface().Prepare('edw_common')

  query_location = os.path.join(FLAGS.edw_index_creation_query_dir, 'create_index_query')
  vm.PushDataFile(query_location)

  query_location = os.path.join(FLAGS.edw_index_deletion_query_dir, 'delete_index_query')
  vm.PushDataFile(query_location)

def Run(benchmark_spec):
  """Runs the benchmark and returns a list of samples.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  results = []

  edw_service_instance = benchmark_spec.edw_service
  client_interface = edw_service_instance.GetClientInterface()
  vm = benchmark_spec.vms[0]

  # Delete Index If Exists
  execution_time, metadata = client_interface.ExecuteQuery('delete_index_query')
  results.append(sample.Sample('search_index_deletion_time', execution_time, 'seconds', metadata))

  execution_time, metadata = client_interface.ExecuteQuery('create_index_query')
  # TODO: Wait for Async Index creation
  results.append(sample.Sample('search_index_creation_time', execution_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  """Cleans up the benchmark resources.

  Args:
    benchmark_spec: The benchmark specification.
  """
  # TODO: Delete Index
  benchmark_spec.edw_service.Cleanup()
