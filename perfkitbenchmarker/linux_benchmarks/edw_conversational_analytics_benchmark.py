# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

r"""Runs Conversational Analytics performance benchmarks using BigQuery geminidataanalytics."""

import logging

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_benchmark_results_aggregator as results_aggregator
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'edw_conversational_analytics_benchmark'

BENCHMARK_CONFIG = """
edw_conversational_analytics_benchmark:
  description: Conversational Analytics performance benchmark using BigQuery.
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
    endpoint: cluster.endpoint
    db: _database_name_
    user: _username_
    password: _password_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

_DATASET = flags.DEFINE_enum(
    'dataset',
    'ecomm',
    ['ecomm', 'call_center'],
    'The dataset to run: ecomm or call_center.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  if not FLAGS.bq_ca_agent:
    raise errors.Config.InvalidValue('Missing required flag: --bq_ca_agent')


def Prepare(benchmark_spec):
  """Install script execution environment on the client vm."""
  benchmark_spec.always_call_cleanup = True
  edw_service_instance = benchmark_spec.edw_service

  # Assign provisioned attributes
  query_client = edw_service_instance.GetClientInterface()
  query_client.SetProvisionedAttributes(benchmark_spec)

  ca_client = edw_service_instance.GetConversationalAnalyticsClientInterface()
  ca_client.SetProvisionedAttributes(benchmark_spec)
  benchmark_spec.ca_client = ca_client

  # Prepare the client environment for both clients
  query_client.Prepare('edw_common')
  ca_client.Prepare('edw_common')


def _RunConversationalQuery(q, ca_client, ca_iteration_performance):
  """Ask the conversational analytics question and record performance."""
  execution_time, metadata = ca_client.ExecuteQuery(q.question)
  ca_iteration_performance.add_query_performance(
      q.question, execution_time, metadata
  )


def _RunGroundTruthQuery(q, query_client, gt_iteration_performance):
  """Execute ground truth SQL and record performance."""
  sql_file_name = f'{q.db_id}_gt.sql'
  vm_util.CreateRemoteFile(
      query_client.client_vm, q.ground_truth_sql, sql_file_name
  )
  gt_execution_time, gt_metadata = query_client.ExecuteQuery(
      sql_file_name, print_results=True
  )
  gt_metadata['question'] = q.question
  gt_metadata['ground_truth_sql'] = q.ground_truth_sql
  if 'query_results' in gt_metadata:
    gt_metadata['ground_truth_data'] = gt_metadata['query_results']
  else:
    logging.warning(
        'No query results found in ground truth query execution'
        ' metadata: %s',
        gt_metadata,
    )
  gt_iteration_performance.add_query_performance(
      f'{q.question}_gt', gt_execution_time, gt_metadata
  )


def _RunIteration(
    iteration_id,
    question_list,
    ca_client,
    query_client,
    ca_expected_queries,
    gt_expected_queries,
):
  """Run a single iteration of the benchmark suite."""
  ca_iteration_performance = results_aggregator.EdwPowerIterationPerformance(
      iteration_id=iteration_id, total_queries=len(ca_expected_queries)
  )
  gt_iteration_performance = results_aggregator.EdwPowerIterationPerformance(
      iteration_id=iteration_id, total_queries=len(gt_expected_queries)
  )

  for q in question_list:
    _RunConversationalQuery(q, ca_client, ca_iteration_performance)
    _RunGroundTruthQuery(q, query_client, gt_iteration_performance)

  return ca_iteration_performance, gt_iteration_performance


def Run(benchmark_spec):
  """Run phase executes conversational queries and collects latencies and metadata."""
  edw_service_instance = benchmark_spec.edw_service
  query_client = edw_service_instance.GetClientInterface()
  ca_client = benchmark_spec.ca_client

  # Load dataset
  question_list = [
      q
      for q in edw_service_instance.GetConversationalAnalyticsQuestionList()
      if q.db_id == _DATASET.value
  ]

  # Determine expected queries (both the question and the ground truth)
  ca_expected_queries = [q.question for q in question_list]
  gt_expected_queries = [f'{q.question}_gt' for q in question_list]

  # Accumulator for the entire benchmark's performance
  ca_performance = results_aggregator.EdwBenchmarkPerformance(
      total_iterations=FLAGS.edw_suite_iterations,
      expected_queries=ca_expected_queries,
  )
  gt_query_performance = results_aggregator.EdwBenchmarkPerformance(
      total_iterations=FLAGS.edw_suite_iterations,
      expected_queries=gt_expected_queries,
  )

  # Multiple iterations of the suite
  for i in range(1, FLAGS.edw_suite_iterations + 1):
    ca_iter_perf, gt_iter_perf = _RunIteration(
        iteration_id=str(i),
        question_list=question_list,
        ca_client=ca_client,
        query_client=query_client,
        ca_expected_queries=ca_expected_queries,
        gt_expected_queries=gt_expected_queries,
    )
    ca_performance.add_iteration_performance(ca_iter_perf)
    gt_query_performance.add_iteration_performance(gt_iter_perf)

  # Execution complete, generate results only if the benchmark was successful.
  if not gt_query_performance.is_successful():
    raise errors.Benchmarks.RunError(
        'Ground Truth query execution failed.'
    )

  benchmark_metadata = {
      'agent': FLAGS.bq_ca_agent,
      'dataset': _DATASET.value,
  }
  benchmark_metadata.update(edw_service_instance.GetMetadata())

  results = []
  results.extend(
      ca_performance.get_all_query_performance_samples(
          metadata=benchmark_metadata
      )
  )
  if ca_performance.is_successful():
    results.extend(
        ca_performance.get_queries_geomean_performance_samples(
            metadata=benchmark_metadata
        )
    )
  results.extend(
      gt_query_performance.get_all_query_performance_samples(
          metadata=benchmark_metadata
      )
  )
  results.extend(
      gt_query_performance.get_queries_geomean_performance_samples(
          metadata=benchmark_metadata
      )
  )

  return results


def Cleanup(benchmark_spec):
  benchmark_spec.edw_service.Cleanup()
