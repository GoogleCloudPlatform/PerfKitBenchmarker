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

"""Benchmark for creating a search index in an EDW service.

The benchmark first issues a command to create the search index on an existing
table in an
EDW service(BigQuery or Snowflake), then measures the time it takes for the
index to fully cover the table.

./pkb.py \
--cloud=GCP  \
--benchmarks=edw_index_ingestion_benchmark \
--bq_client_interface=PYTHON  \
--config_override=edw_index_ingestion_benchmark.edw_service.type=bigquery \
--config_override=edw_index_ingestion_benchmark.edw_service.cluster_identifier=p3rf-bq-search.search_index_dataset \
--gcp_service_account=bigquery-testing-pkb@p3rf-bigquery-smallquery-slots.iam.gserviceaccount.com \
--gcp_service_account_key_file=/home/shuninglin/p3rf-bq-search-050c6559ed66.json \
--edw_index_ingestion_query_dir=edw/bigquery/search_index/CUJ2 \
--edw_power_queries=verify_no_table_query,create_index_query,create_table_query,delete_index_query,delete_table_query,ingest_data_query,check_index_coverage_query \
--metadata=cloud:GCP \
--project=p3rf-bq-search \
--zones=us-central1-c 


TODO: Set up SF tables and queries and test if they work.
"""

import logging
import os
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'edw_index_ingestion_benchmark'
BENCHMARK_CONFIG = """
edw_index_ingestion_benchmark:
  description: Benchmark for checking how fast is index propogation under ingestion.
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

flags.DEFINE_string(
    'edw_index_ingestion_query_dir',
    '',
    'Optional local directory containing all query files. '
    'Can be absolute or relative to the executable.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config: dict[Any, Any]) -> dict[Any, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
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

  query_locations: list[str] = [
      os.path.join(FLAGS.edw_index_ingestion_query_dir, query)
      for query in FLAGS.edw_power_queries.split(',')
  ]
  any(vm.PushDataFile(query_loc) for query_loc in query_locations)


def _EnsureNoTable(
    client_interface: edw_service.EdwClientInterface,  timeout: int = 30
) -> None:
  """Ensures that the table does not exist.

  Args:
    client_interface: The EDW client interface.
    timeout: The maximum time(in seconds) to wait for the index to fully cover.
  """
  start_time = time.time()
  while True:
    time_elapsed = time.time() - start_time
    if time_elapsed > timeout:
      logging.error('Timed out waiting for table to be deleted')
      # TODO: find a way to stop the benchmark in case of timeout
      break
    _, metadata = client_interface.ExecuteQuery('verify_no_table_query')
    if metadata and metadata.get('rows_returned', 0) > 0:
        client_interface.ExecuteQuery('delete_table_query')
    else:
        break
    time.sleep(1)


def _EnsureFullyIndexed(
    client_interface: edw_service.EdwClientInterface,  results: list[sample.Sample], timeout: int = 1200
) -> None:
  """Checks if the existing index has fully covered the table.

  Args:
    client_interface: The EDW client interface.
    timeout: The maximum time(in seconds) to wait for the index to fully cover.
    results: The list of samples to append to.
  """
  start_time = time.time()
  while True:
    time_elapsed = time.time() - start_time
    if time_elapsed > timeout:
      logging.error('Timed out waiting for initial index to to fully cover.')
      # TODO: find a way to stop the benchmark in case of timeout
      break
    _, metadata = client_interface.ExecuteQuery('check_index_coverage_query')
    if metadata and metadata.get('rows_returned', 0) > 0:
        results.append(
            sample.Sample(
                'initial_index_fully_available_time', time_elapsed, 'seconds', metadata
            )
        )
        break
    time.sleep(1)


def _CreateTable(
    client_interface: edw_service.EdwClientInterface,
    results: list[sample.Sample],
) -> None:
  """Create the table for CUJ2 benchmark by cloning from the existing table for CUJ1

  Args:
    client_interface: The EDW client interface.
    results: The list of samples to append to.
  """
  execution_time, metadata = client_interface.ExecuteQuery('create_table_query')
  results.append(
      sample.Sample(
          'table_creation_time', execution_time, 'seconds', metadata
      )
  )

def _CreateIndex(
    client_interface: edw_service.EdwClientInterface,
    results: list[sample.Sample],
) -> None:
  """Creates an index and records the execution time.

  Args:
    client_interface: The EDW client interface.
    results: The list of samples to append to.
  """
  execution_time, metadata = client_interface.ExecuteQuery('create_index_query')
  results.append(
      sample.Sample(
          'search_index_creation_time', execution_time, 'seconds', metadata
      )
  )

def _IngestData(
    client_interface: edw_service.EdwClientInterface,
    results: list[sample.Sample],
) -> None:
  """Ingest data to the table

  Args:
    client_interface: The EDW client interface.
    results: The list of samples to append to.
  """
  execution_time, metadata = client_interface.ExecuteQuery('ingest_data_query')
  results.append(
      sample.Sample(
          'data_ingestion_completion_time', execution_time, 'seconds', metadata
      )
  )

def _MeasureCoveringTime(
    client_interface: edw_service.EdwClientInterface,
    results: list[sample.Sample],
    timeout: int = 1200,
) -> None:
  """Measures the time it takes for the index to be fully built(reaching 100% coverage).

  Args:
    client_interface: The EDW client interface.
    results: The list of samples to append to.
    timeout: The maximum time(in seconds) to wait for the index to be fully
      built.
  """
  start_time = time.time()
  while True:
    _, metadata = client_interface.ExecuteQuery('check_index_coverage_query')
    time_elapsed = time.time() - start_time
    if metadata and metadata.get('rows_returned', 0) > 0:
      results.append(
          sample.Sample(
              'search_index_fully_available_time', time_elapsed, 'seconds', metadata
          )
      )
      break
    if time_elapsed > timeout:
      logging.error('Timed out waiting for index to fully cover the table.')
      # TODO: find a way to stop the benchmark in case of timeout
      break
    else:
      time.sleep(1)


def _DeleteTable(
    client_interface: edw_service.EdwClientInterface
) -> None:
  """Deletes a table and records the execution time.

  Args:
    client_interface: The EDW client interface.
  """
  client_interface.ExecuteQuery('delete_table_query')


def _DeleteIndex(
    client_interface: edw_service.EdwClientInterface
) -> None:
  """Deletes an index and records the execution time.

  Args:
    client_interface: The EDW client interface.
  """
  client_interface.ExecuteQuery('delete_index_query')



def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark and returns a list of samples.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  results: list[sample.Sample] = []

  edw_service_instance = benchmark_spec.edw_service
  client_interface = edw_service_instance.GetClientInterface()

  _EnsureNoTable(client_interface)

  _CreateTable(client_interface, results)

  _CreateIndex(client_interface, results)

  _EnsureFullyIndexed(client_interface, results)

  _IngestData(client_interface, results)

  _MeasureCoveringTime(client_interface, results)

  return results


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleans up the benchmark resources.

  Args:
    benchmark_spec: The benchmark specification.
  """
  benchmark_spec.edw_service.Cleanup()

  edw_service_instance = benchmark_spec.edw_service
  _DeleteIndex(edw_service_instance.GetClientInterface())
  _DeleteTable(edw_service_instance.GetClientInterface())