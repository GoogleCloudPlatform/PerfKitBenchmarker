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

import dataclasses
import json
import logging
from typing import Any

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

_QUERY_RESULT_SIZE_LIMIT_BYTES = 800 * 1024


@dataclasses.dataclass
class _BenchmarkPerformanceSuite:
  """Accumulator class tracking expectations, results, and clients for the run."""

  edw_service_instance: Any
  ca_client: Any
  query_client: Any
  question_list: list[Any]
  ca_expected_queries: list[str]
  ca_performance: results_aggregator.EdwBenchmarkPerformance
  gt_expected_queries: list[str] | None = None
  gt_query_performance: results_aggregator.EdwBenchmarkPerformance | None = None
  predict_expected_queries: list[str] | None = None
  predict_query_performance: (
      results_aggregator.EdwBenchmarkPerformance | None
  ) = None

  @classmethod
  def FromEdwServiceAndClientInterface(
      cls,
      edw_service_instance: Any,
      ca_client: Any,
      query_client: Any,
  ) -> '_BenchmarkPerformanceSuite':
    """Initializes expected queries and benchmark performance accumulators.

    Args:
      edw_service_instance: The EDW service instance.
      ca_client: The Conversational Analytics client.
      query_client: The query client.

    Returns:
      A _BenchmarkPerformanceSuite containing initialized queries and
      performances.
    """
    run_predict = not ca_client.fetches_results_immediately
    is_competitor = IsCompetitor(edw_service_instance)

    question_list = [
        q
        for q in edw_service_instance.GetConversationalAnalyticsQuestionList()
        if q.db_id == _DATASET.value
    ]

    ca_expected_queries = [q.question for q in question_list]

    ca_performance = results_aggregator.EdwBenchmarkPerformance(
        total_iterations=FLAGS.edw_suite_iterations,
        expected_queries=ca_expected_queries,
    )

    gt_expected_queries = None
    gt_query_performance = None
    predict_expected_queries = None
    predict_query_performance = None

    if run_predict:
      predict_expected_queries = [
          f'{q.question}_predict' for q in question_list
      ]
      predict_query_performance = results_aggregator.EdwBenchmarkPerformance(
          total_iterations=FLAGS.edw_suite_iterations,
          expected_queries=predict_expected_queries,
      )
    if not is_competitor:
      gt_expected_queries = [f'{q.question}_gt' for q in question_list]
      gt_query_performance = results_aggregator.EdwBenchmarkPerformance(
          total_iterations=FLAGS.edw_suite_iterations,
          expected_queries=gt_expected_queries,
      )

    return cls(
        edw_service_instance=edw_service_instance,
        ca_client=ca_client,
        query_client=query_client,
        question_list=question_list,
        ca_expected_queries=ca_expected_queries,
        ca_performance=ca_performance,
        gt_expected_queries=gt_expected_queries,
        gt_query_performance=gt_query_performance,
        predict_expected_queries=predict_expected_queries,
        predict_query_performance=predict_query_performance,
    )

  def RunIteration(
      self,
      iteration_id: str,
  ) -> tuple[
      results_aggregator.EdwPowerIterationPerformance,
      results_aggregator.EdwPowerIterationPerformance | None,
      results_aggregator.EdwPowerIterationPerformance | None,
  ]:
    """Runs a single iteration of the benchmark suite.

    Args:
      iteration_id: The ID of the iteration.

    Returns:
      A tuple of (ca_iteration_performance, gt_iteration_performance,
      predict_iteration_performance), where:
        - ca_iteration_performance: EdwPowerIterationPerformance containing
          conversational analytics query performance.
        - gt_iteration_performance: EdwPowerIterationPerformance or None if the
          service is a competitor.
        - predict_iteration_performance: EdwPowerIterationPerformance or None
          if the client fetches results immediately.
    """
    ca_iteration_performance = results_aggregator.EdwPowerIterationPerformance(
        iteration_id=iteration_id, total_queries=len(self.ca_expected_queries)
    )
    gt_iteration_performance = None
    predict_iteration_performance = None

    # If the service does not fetch results immediately, it requires two-step
    # execution: firstly run agent to get predict SQL, then run the predict SQL.
    if not self.ca_client.fetches_results_immediately:
      predict_iteration_performance = (
          results_aggregator.EdwPowerIterationPerformance(
              iteration_id=iteration_id,
              total_queries=len(self.predict_expected_queries),
          )
      )

    # Only run ground truth queries for BQ/Looker
    if not IsCompetitor(self.edw_service_instance):
      gt_iteration_performance = (
          results_aggregator.EdwPowerIterationPerformance(
              iteration_id=iteration_id,
              total_queries=len(self.gt_expected_queries),
          )
      )

    for q in self.question_list:
      _RunConversationalQuery(q, self.ca_client, ca_iteration_performance)
      if predict_iteration_performance:
        predict_sql = _RetrievePredictQuery(q, ca_iteration_performance)
        if predict_sql:
          _RunPredictQuery(
              q, predict_sql, self.query_client, predict_iteration_performance
          )
        else:
          logging.warning(
              'No predict SQL generated for question: %s', q.question
          )
          predict_iteration_performance.add_query_performance(
              f'{q.question}_predict',
              -1.0,
              {
                  'question': q.question,
                  'predict_sql': '',
                  'error_message': (
                      'No predict SQL generated by Cortex Analyst.'
                  ),
              },
          )

      if gt_iteration_performance:
        _RunGroundTruthQuery(q, self.query_client, gt_iteration_performance)

    return (
        ca_iteration_performance,
        gt_iteration_performance,
        predict_iteration_performance,
    )

  def BuildResults(self) -> list[Any]:
    """Builds and returns the list of performance samples."""
    if self.predict_query_performance:
      if _ShouldFailBenchmarkForQueryFailure(self.predict_query_performance):
        raise errors.Benchmarks.RunError('Predict query execution failed.')
    if self.gt_query_performance:
      if not self.gt_query_performance.is_successful():
        raise errors.Benchmarks.RunError('Ground Truth query execution failed.')

    benchmark_metadata = {
        'dataset': _DATASET.value,
    }
    if FLAGS.bq_ca_agent:
      benchmark_metadata['agent'] = FLAGS.bq_ca_agent
    if FLAGS.snowflake_ca_semantic_view:
      benchmark_metadata['agent'] = FLAGS.snowflake_ca_semantic_view
    benchmark_metadata.update(self.edw_service_instance.GetMetadata())

    results = []
    results.extend(
        self.ca_performance.get_all_query_performance_samples(
            metadata=benchmark_metadata
        )
    )
    if self.ca_performance.is_successful():
      results.extend(
          self.ca_performance.get_queries_geomean_performance_samples(
              metadata=benchmark_metadata
          )
      )

    if self.predict_query_performance:
      results.extend(
          self.predict_query_performance.get_all_query_performance_samples(
              metadata=benchmark_metadata
          )
      )
      if self.predict_query_performance.is_successful():
        results.extend(
            self.predict_query_performance.get_queries_geomean_performance_samples(
                metadata=benchmark_metadata
            )
        )
    if self.gt_query_performance:
      results.extend(
          self.gt_query_performance.get_all_query_performance_samples(
              metadata=benchmark_metadata
          )
      )
      results.extend(
          self.gt_query_performance.get_queries_geomean_performance_samples(
              metadata=benchmark_metadata
          )
      )

    return results


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Checks if the required flags are passed.

  Args:
    benchmark_config: The benchmark configuration.
  """
  if not benchmark_config or not getattr(benchmark_config, 'edw_service', None):
    raise errors.Config.InvalidValue(
        'Benchmark configuration must contain edw_service.'
    )
  edw_service_type = benchmark_config.edw_service.type

  if edw_service_type == 'bigquery':
    if not FLAGS.bq_ca_agent:
      raise errors.Config.InvalidValue('Missing required flag: --bq_ca_agent')
  elif edw_service_type.startswith('snowflake'):
    if not FLAGS.snowflake_ca_semantic_view:
      raise errors.Config.InvalidValue(
          'Missing required flag: --snowflake_ca_semantic_view'
      )


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


def _RunConversationalQuery(
    q: Any,
    ca_client: Any,
    ca_iteration_performance: results_aggregator.EdwPowerIterationPerformance,
) -> None:
  """Ask the conversational analytics question and record performance."""
  execution_time, metadata = ca_client.ExecuteQuery(q.question)
  ca_iteration_performance.add_query_performance(
      q.question, execution_time, metadata
  )


def _RunGroundTruthQuery(
    q: Any,
    query_client: Any,
    gt_iteration_performance: results_aggregator.EdwPowerIterationPerformance,
) -> None:
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
  gt_iteration_performance.add_query_performance(
      f'{q.question}_gt', gt_execution_time, gt_metadata
  )


def _RetrievePredictQuery(
    q: Any,
    ca_iteration_performance: results_aggregator.EdwPowerIterationPerformance,
) -> str | None:
  """Parses the predict query from ca_iteration_performance."""
  query_performance = ca_iteration_performance.performance.get(q.question)
  if query_performance and query_performance.is_successful():
    return query_performance.metadata.get('generated_sql')
  return None


def _GetSerializedMetadataSize(metadata: dict[str, Any]) -> int:
  """Calculates the size of the serialized metadata in bytes."""
  return len(json.dumps(metadata, default=str).encode('utf-8'))


def _RunPredictQuery(
    q: Any,
    predict_sql: str,
    query_client: Any,
    predict_iteration_performance: results_aggregator.EdwPowerIterationPerformance,
) -> None:
  """Execute predict SQL and record performance."""
  sql_file_name = f'{q.db_id}_predict.sql'
  vm_util.CreateRemoteFile(query_client.client_vm, predict_sql, sql_file_name)
  predict_execution_time, predict_metadata = query_client.ExecuteQuery(
      sql_file_name, print_results=True
  )
  predict_metadata['question'] = q.question
  predict_metadata['predict_sql'] = predict_sql
  if 'query_results' in predict_metadata:
    predict_metadata['predict_data'] = predict_metadata['query_results']

  # Enforce result size limit (safety check for Capacitor)
  serialized_size = _GetSerializedMetadataSize(predict_metadata)
  if serialized_size > _QUERY_RESULT_SIZE_LIMIT_BYTES:
    logging.warning(
        'Predict query results size is too large: %d bytes. Treating as'
        ' failure.',
        serialized_size,
    )
    predict_metadata.pop('query_results', None)
    predict_metadata.pop('predict_data', None)
    predict_metadata['is_result_too_large'] = True
    limit_kb = _QUERY_RESULT_SIZE_LIMIT_BYTES / 1024
    predict_metadata['error_message'] = (
        f'Query result size exceeded safety limit of {limit_kb:.0f}KB. Got'
        f' {serialized_size} bytes.'
    )
    predict_execution_time = -1.0
  predict_iteration_performance.add_query_performance(
      f'{q.question}_predict', predict_execution_time, predict_metadata
  )


def IsCompetitor(edw_service_instance) -> bool:
  """Returns True if the edw service is a competitor (Snowflake or Databricks)."""
  return edw_service_instance.SERVICE_TYPE.startswith(
      'snowflake'
  ) or edw_service_instance.SERVICE_TYPE.startswith('databricks')


def _ShouldFailBenchmarkForQueryFailure(predict_query_performance) -> bool:
  """Returns True if there are failures that should fail the benchmark.

  Failures caused by empty predict SQL or too large results do not fail
  the benchmark.

  Args:
    predict_query_performance: The predict query benchmark performance.
  """
  for (
      iteration_perf
  ) in predict_query_performance.iteration_performances.values():
    if isinstance(
        iteration_perf, results_aggregator.EdwPowerIterationPerformance
    ):
      for query_perf in iteration_perf.performance.values():
        if not query_perf.is_successful():
          if query_perf.metadata.get('predict_sql'):
            if not query_perf.metadata.get('is_result_too_large'):
              return True
  return False


def Run(benchmark_spec) -> list[Any]:
  """Run phase executes conversational queries and collects latencies and metadata."""
  edw_service_instance = benchmark_spec.edw_service
  query_client = edw_service_instance.GetClientInterface()
  ca_client = benchmark_spec.ca_client

  suite = _BenchmarkPerformanceSuite.FromEdwServiceAndClientInterface(
      edw_service_instance, ca_client, query_client
  )

  # Multiple iterations of the suite
  for i in range(1, FLAGS.edw_suite_iterations + 1):
    ca_iter_perf, gt_iter_perf, predict_iter_perf = suite.RunIteration(
        iteration_id=str(i)
    )
    suite.ca_performance.add_iteration_performance(ca_iter_perf)
    if suite.predict_query_performance:
      suite.predict_query_performance.add_iteration_performance(
          predict_iter_perf
      )
    if suite.gt_query_performance:
      suite.gt_query_performance.add_iteration_performance(gt_iter_perf)

  return suite.BuildResults()


def Cleanup(benchmark_spec):
  benchmark_spec.edw_service.Cleanup()
