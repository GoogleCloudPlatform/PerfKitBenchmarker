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

"""TODO

Example run command:

./pkb.py \
--cloud=GCP  \
--benchmarks=edw_index_ingestion_benchmark \
--bq_client_interface=PYTHON  \
--config_override=edw_index_ingestion_benchmark.edw_service.type=bigquery \
--config_override=edw_index_ingestion_benchmark.edw_service.cluster_identifier=p3rf-bq-search.search_index_dataset \
--gcp_service_account=1036392050503-compute@developer.gserviceaccount.com \
--gcp_service_account_key_file=/home/shuninglin/p3rf-bq-search-050c6559ed66.json \
--edw_index_measure_query_dir=edw/bigquery/search_index/measure_performance \
--edw_power_queries=load_init_data_query,create_index_query,concurrent_ingestion_query,search_query \
--metadata=cloud:GCP \
--project=p3rf-bq-search \
--zones=us-central1-c 
"""
from multiprocessing.managers import ListProxy
import time


import logging
import os
from typing import Any
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_benchmark_results_aggregator as results_aggregator
import multiprocessing
from multiprocessing.synchronize import Event as SyncEvent
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'edw_index_ingestion_benchmark'

BENCHMARK_CONFIG = """
edw_index_ingestion_benchmark:
  description: Edw search index benchmark
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

flags.DEFINE_string(
    'edw_index_measure_query_dir',
    '',
    'Local directory containing performance measurement queries. '
    'Can be absolute or relative to the executable.',
)

flags.DEFINE_integer(
  'edw_index_ingestion_search_iterations',
  1,
  'How many times to run the search query'
)

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install script execution environment on the client vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  edw_service_instance = benchmark_spec.edw_service
  vm = benchmark_spec.vms[0]

  edw_service_instance.GetClientInterface().SetProvisionedAttributes(
      benchmark_spec
  )
  edw_service_instance.GetClientInterface().Prepare('edw_common')

  query_locations = [
      os.path.join(FLAGS.edw_index_measure_query_dir, query)
      for query in FLAGS.edw_power_queries.split(',')
  ]
  any(vm.PushDataFile(query_loc) for query_loc in query_locations)


# TODO: Record metrics from these queries too
def _execute_data_load(client_interface: edw_service.EdwClientInterface, concurrent_ingestion_query:str, complete:SyncEvent, results:ListProxy[sample.Sample]):
  """Executes the data loading query."""
  logging.info('Starting data loading loop')
  iter = 0
  while (not complete.is_set()):
     execution_time, metadata = client_interface.ExecuteQuery(concurrent_ingestion_query) 
     metadata['load_iter'] = iter
     iter += 1
     results.append(sample.Sample('load_query_execution_time', execution_time, 'seconds', metadata))
  logging.info('Data loading loop completed')


def _execute_index_queries(client_interface: edw_service.EdwClientInterface, search_query:str, results:ListProxy[sample.Sample], num_queries=5, interval=0):
  """Executes the index search query in a separate process."""
  logging.info('Starting index search query loop')
  for i in range(num_queries):
    time.sleep(interval)
    logging.info('Running index search query iteration %d', i + 1)
    execution_time, metadata = client_interface.ExecuteQuery(search_query, print_results=True) 
    logging.info('Index search query iteration %d completed in execution time: %d', i + 1, execution_time)
    metadata['index_query_iter'] = i
    results.append(sample.Sample('query_execution_Time', execution_time, 'seconds', metadata))
  logging.info('Index search query loop completed')


def Run(benchmark_spec):
  """Run phase executes the sql scripts on edw cluster and collects duration."""

  results  = []

  with multiprocessing.Manager() as manager:
    results_proxy: ListProxy[sample.Sample] = manager.list()
    edw_service_instance: edw_service.EdwService = benchmark_spec.edw_service
    client_interface: edw_service.EdwClientInterface = edw_service_instance.GetClientInterface()

    client_interface.ExecuteQuery('load_init_data_query') 
    client_interface.ExecuteQuery('create_index_query') 
    
    searches_finished = multiprocessing.Event()

    # Create separate processes for concurrent data ingestion and index querying
    data_load_process = multiprocessing.Process(
        target=_execute_data_load,
        args=(client_interface, 'concurrent_ingestion_query', searches_finished, results_proxy)
    )

    data_load_process.start()
    logging.info("Starting search query dispatch")
    _execute_index_queries(client_interface, 'search_query', results_proxy, num_queries=FLAGS.edw_index_ingestion_search_iterations)
    logging.info("Search queries completed. Ending data load.")
    searches_finished.set()
    data_load_process.join()
    results = list(results_proxy)

  return results


def Cleanup(benchmark_spec):
  benchmark_spec.edw_service.Cleanup()
