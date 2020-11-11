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

"""Runs Enterprise Data Warehouse (edw) performance benchmarks.

This benchmark adds the ability to run arbitrary sql workloads on hosted fully
managed data warehouse solutions such as Redshift and BigQuery.
"""


import logging
import os
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_benchmark_results_aggregator as results_aggregator
from perfkitbenchmarker import edw_service

BENCHMARK_NAME = 'edw_benchmark'

BENCHMARK_CONFIG = """
edw_benchmark:
  description: Sample edw benchmark
  edw_service:
    type: redshift
    cluster_identifier: _cluster_id_
    endpoint: cluster.endpoint
    db: _database_name_
    user: _username_
    password: _password_
    node_type: dc1.large
    node_count: 2
    snapshot:
  vm_groups:
    client:
      vm_spec: *default_single_core
"""

flags.DEFINE_string(
    'local_query_dir', '',
    'Optional local directory containing all query files. '
    'Can be absolute or relative to the executable.')

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
      benchmark_spec)
  edw_service_instance.GetClientInterface().Prepare('edw_common')

  query_locations = [
      os.path.join(FLAGS.local_query_dir, query)
      for query in FLAGS.edw_power_queries.split(',')
  ]
  any(vm.PushFile(query_loc) for query_loc in query_locations)


def Run(benchmark_spec):
  """Run phase executes the sql scripts on edw cluster and collects duration."""
  results = []

  edw_service_instance = benchmark_spec.edw_service
  client_interface = edw_service_instance.GetClientInterface()

  # Run a warm up query in case there are cold start issues.
  client_interface.WarmUpQuery()

  # Default to executing just the sample query if no queries are provided.
  all_queries = FLAGS.edw_power_queries.split(',') or [
      os.path.basename(edw_service.SAMPLE_QUERY_PATH)
  ]

  # Accumulator for the entire benchmark's performance
  benchmark_performance = results_aggregator.EdwBenchmarkPerformance(
      total_iterations=FLAGS.edw_suite_iterations, expected_queries=all_queries)

  # Multiple iterations of the suite are performed to avoid cold start penalty
  for i in range(1, FLAGS.edw_suite_iterations + 1):
    iteration = str(i)
    # Accumulator for the current suite's performance
    iteration_performance = results_aggregator.EdwPowerIterationPerformance(
        iteration_id=iteration, total_queries=len(all_queries))

    for query in all_queries:
      execution_time, metadata = client_interface.ExecuteQuery(query)
      iteration_performance.add_query_performance(query, execution_time,
                                                  metadata)
    benchmark_performance.add_iteration_performance(iteration_performance)

  # Execution complete, generate results only if the benchmark was successful.
  benchmark_metadata = {}
  benchmark_metadata.update(edw_service_instance.GetMetadata())
  if benchmark_performance.is_successful():
    query_samples = benchmark_performance.get_all_query_performance_samples(
        metadata=benchmark_metadata)
    results.extend(query_samples)

    geomean_samples = (
        benchmark_performance.get_queries_geomean_performance_samples(
            metadata=benchmark_metadata))
    results.extend(geomean_samples)
  else:
    logging.error('At least one query failed, so not reporting any results.')
  return results


def Cleanup(benchmark_spec):
  benchmark_spec.edw_service.Cleanup()
