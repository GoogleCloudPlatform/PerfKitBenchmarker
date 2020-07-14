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

from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_benchmark_results_aggregator as results_aggregator
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags

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
  edw_service_instance.GetClientInterface().Prepare(benchmark_spec.name)

  query_locations = [
      os.path.join(FLAGS.local_query_dir, query)
      for query in FLAGS.queries_to_execute
  ]
  any(vm.PushFile(query_loc) for query_loc in query_locations)


def Run(benchmark_spec):
  """Run phase executes the sql scripts on edw cluster and collects duration."""
  results = []

  edw_service_instance = benchmark_spec.edw_service

  # Run a warm up query in case there are cold start issues.
  edw_service_instance.GetClientInterface().WarmUpQuery()

  # Default to executing just the sample query if no queries are provided.
  queries_to_execute = FLAGS.queries_to_execute or [
      os.path.basename(edw_service.SAMPLE_QUERY_PATH)
  ]

  # Accumulator for the entire benchmark's performance
  benchmark_performance = results_aggregator.EdwBenchmarkPerformance(
      total_iterations=FLAGS.edw_suite_iterations,
      expected_suite_queries=queries_to_execute)

  # Multiple iterations of the suite are performed to avoid cold start penalty
  for i in range(1, FLAGS.edw_suite_iterations + 1):
    iteration = str(i)
    # Accumulator for the current suite's performance
    suite_performance = results_aggregator.EdwSuitePerformance(
        suite_name='edw',
        suite_sequence=iteration,
        total_suite_queries=len(queries_to_execute))

    for query in queries_to_execute:
      time, metadata = edw_service_instance.GetClientInterface().ExecuteQuery(
          query)
      edw_query_performance = results_aggregator.EdwQueryPerformance(
          query_name=query, performance=time, metadata=metadata)
      suite_performance.add_query_performance(edw_query_performance)
    benchmark_performance.add_suite_performance(iteration, suite_performance)

  # Execution complete, generate results only if the benchmark was successful.
  benchmark_metadata = {}
  benchmark_metadata.update(edw_service_instance.GetMetadata())
  if benchmark_performance.is_successful():
    query_samples = benchmark_performance.get_all_query_performance_samples(
        metadata=benchmark_metadata)
    results.extend(query_samples)

    wall_time_samples = benchmark_performance.get_wall_time_performance_samples(
        metadata=benchmark_metadata)
    results.extend(wall_time_samples)

    geomean_samples = (
        benchmark_performance.get_queries_geomean_performance_samples(
            metadata=benchmark_metadata))
    results.extend(geomean_samples)
  else:
    logging.error('At least one query failed, so not reporting any results.')
  return results


def Cleanup(benchmark_spec):
  benchmark_spec.edw_service.Cleanup()
