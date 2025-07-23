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

Run command:

./pkb.py \
--cloud=GCP  \
--benchmarks=edw_index_benchmark \
--bq_client_interface=PYTHON  \
--config_override=edw_index_benchmark.edw_service.type=bigquery \
--config_override=edw_index_benchmark.edw_service.cluster_identifier=p3rf-bq-search.search_index_dataset \
--gcp_service_account=bigquery-testing-pkb@p3rf-bigquery-smallquery-slots.iam.gserviceaccount.com \
--gcp_service_account_key_file=/Users/saksena/Downloads/p3rf-bigquery-smallquery-slots.json \
--edw_index_local_query_dir=edw/bigquery/search_index/measure_performance \
--edw_power_queries=search_query \
--metadata=cloud:GCP \
--project=saksena-test \
--zones=us-central1-c 



./pkb.py \
--cloud=AWS \
--benchmarks=edw_index_benchmark \
--config_override=edw_index_benchmark.edw_service.type=snowflake_aws \
--config_override=edw_index_benchmark.edw_service.cluster_identifier= \
--edw_index_local_query_dir=edw/snowflake_aws/search_index/measure_performance \
--edw_power_queries=test_query \
--machine_type=m4.large \
--metadata=cloud:AWS \
--snowflake_client_interface=JDBC \
--snowflake_jdbc_client_jar=/Users/saksena/Downloads/snowflake-jdbc-client-2.13-enterprise.jar \
--snowflake_database=SEARCH_INDEX \
--snowflake_schema=INDEX_TEST \
--snowflake_warehouse=XSMALL_TEST \
--zones=us-west-2a 

"""


import logging
import os
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_benchmark_results_aggregator as results_aggregator
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'edw_index_benchmark'

BENCHMARK_CONFIG = """
edw_index_benchmark:
  description: Edw search index benchmark
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

flags.DEFINE_string(
    'edw_index_local_query_dir',
    '',
    'Optional local directory containing all query files. '
    'Can be absolute or relative to the executable.',
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
      os.path.join(FLAGS.edw_index_local_query_dir, query)
      for query in FLAGS.edw_power_queries.split(',')
  ]
  any(vm.PushDataFile(query_loc) for query_loc in query_locations)


def Run(benchmark_spec):
  """Run phase executes the sql scripts on edw cluster and collects duration."""
  results = []

  edw_service_instance = benchmark_spec.edw_service
  client_interface = edw_service_instance.GetClientInterface()

  all_queries = FLAGS.edw_power_queries.split(',')

  for query in all_queries:
    execution_time, metadata = client_interface.ExecuteQuery(query)
    results.append(sample.Sample('query_execution_Time', execution_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  benchmark_spec.edw_service.Cleanup()
