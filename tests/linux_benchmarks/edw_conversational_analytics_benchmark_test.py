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

"""Tests for edw_conversational_analytics_benchmark, verifying Prepare and Run behavior."""

import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import edw_benchmark_results_aggregator as results_aggregator
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import edw_conversational_analytics_benchmark
from perfkitbenchmarker.providers.gcp import bigquery
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_TEST_CONFIG = """
edw_conversational_analytics_benchmark:
  description: Conversational Analytics performance benchmark using BigQuery.
  edw_service:
    type: bigquery
    cluster_identifier: test-project.test-dataset
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""


class EdwConversationalAnalyticsBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    FLAGS.gcp_service_account_key_file = 'fake-key.json'

    self.spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=_TEST_CONFIG,
        benchmark_name='edw_conversational_analytics_benchmark',
    )
    self.spec.ConstructEdwService()
    self.service = self.spec.edw_service

    self.mock_client_interface = mock.Mock()
    self.mock_client_interface.project_id = 'test-project'
    self.mock_client_interface.dataset_id = 'test-dataset'
    self.mock_client_interface.client_vm = mock.Mock()

    self.service.client_interface = self.mock_client_interface  # pyrefly: ignore[missing-attribute]

    self.ca_client = bigquery.ConversationalAnalyticsClientInterface(
        'test-project', 'test-dataset'
    )
    self.service.GetConversationalAnalyticsClientInterface = mock.Mock(  # pyrefly: ignore[missing-attribute]
        return_value=self.ca_client
    )

    self.spec.vms = [self.mock_client_interface.client_vm]  # pyrefly: ignore[read-only]
    setattr(self.spec, 'ca_client', self.ca_client)

    self.create_remote_file_patcher = mock.patch.object(
        edw_conversational_analytics_benchmark.vm_util, 'CreateRemoteFile'
    )
    self.mock_create_remote_file = self.create_remote_file_patcher.start()
    self.addCleanup(self.create_remote_file_patcher.stop)

  @flagsaver.flagsaver(bq_ca_agent='')
  def testCheckPrerequisitesRaisesValueErrorWhenFlagMissing(self):
    mock_config = mock.Mock()
    mock_config.edw_service.type = 'bigquery'
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'Missing required flag: --bq_ca_agent'
    ):
      edw_conversational_analytics_benchmark.CheckPrerequisites(mock_config)

  def testCheckPrerequisitesRaisesValueErrorWhenEdwServiceMissing(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        'Benchmark configuration must contain edw_service.',
    ):
      edw_conversational_analytics_benchmark.CheckPrerequisites(None)

  def testCheckPrerequisitesRaisesValueErrorWhenEdwServiceNone(self):
    mock_config = mock.Mock()
    mock_config.edw_service = None
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        'Benchmark configuration must contain edw_service.',
    ):
      edw_conversational_analytics_benchmark.CheckPrerequisites(mock_config)

  @flagsaver.flagsaver(
      snowflake_ca_semantic_view='',
  )
  def testCheckPrerequisitesRaisesValueErrorWhenSnowflakeFlagsMissing(self):
    mock_config = mock.Mock()
    mock_config.edw_service.type = 'snowflake_aws'
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        'Missing required flag: --snowflake_ca_semantic_view',
    ):
      edw_conversational_analytics_benchmark.CheckPrerequisites(mock_config)

  @flagsaver.flagsaver(
      bq_ca_agent='projects/test/locations/us-central1/dataAgents/test-agent',
      gcp_service_account_key_file='fake-key.json',
      data_search_paths=['cloud/performance/artemis/data'],
  )
  @mock.patch.object(
      bigquery.ConversationalAnalyticsClientInterface,
      'Prepare',
      autospec=True,
  )
  def testPrepareOverridesClientInterfaceAndCallsPrepare(self, mock_ca_prepare):
    # Act
    edw_conversational_analytics_benchmark.Prepare(self.spec)

    # Assert
    ca_client = getattr(self.spec, 'ca_client')
    with self.subTest(name='ca_client_class'):
      self.assertIsInstance(
          ca_client,
          bigquery.ConversationalAnalyticsClientInterface,
      )
    with self.subTest(name='client_vm_copied'):
      self.assertEqual(
          ca_client.client_vm,
          self.mock_client_interface.client_vm,
      )
    with self.subTest(name='query_client_not_saved'):
      self.assertFalse(hasattr(self.spec, 'query_client'))
    with self.subTest(name='query_client_prepared'):
      self.mock_client_interface.Prepare.assert_called_once_with('edw_common')
    with self.subTest(name='ca_client_prepared'):
      mock_ca_prepare.assert_called_once_with(ca_client, 'edw_common')

  def _SetupQuestions(self, questions=None):
    if questions is None:
      questions = [
          edw_service.ConversationalAnalyticsQuestion(
              question='What is the total revenue?',
              db_id='call_center',
              ground_truth_sql='SELECT 1;',
          ),
          edw_service.ConversationalAnalyticsQuestion(
              question='Show me top users',
              db_id='call_center',
              ground_truth_sql='SELECT 2;',
          ),
      ]
    self.service.GetConversationalAnalyticsQuestionList = mock.Mock(  # pyrefly: ignore[missing-attribute]
        return_value=questions
    )
    self.service.GetMetadata = mock.Mock(return_value={'service_meta': 'val'})  # pyrefly: ignore[missing-attribute]
    return questions

  def _CreateSuccessIterationPerformance(self, iteration_id, queries):
    ca_iter = results_aggregator.EdwPowerIterationPerformance(
        iteration_id, len(queries)
    )
    gt_iter = results_aggregator.EdwPowerIterationPerformance(
        iteration_id, len(queries)
    )
    for q in queries:
      ca_iter.add_query_performance(q, 1.0, {})
      gt_iter.add_query_performance(f'{q}_gt', 1.0, {})
    return ca_iter, gt_iter, None

  @flagsaver.flagsaver(
      bq_ca_agent='projects/test/locations/us-central1/dataAgents/test-agent',
      edw_suite_iterations=2,
      dataset='call_center',
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite,
      'RunIteration',
      autospec=True,
  )
  def testRunCallsRunIterationCorrectNumberOfTimes(self, mock_run_iteration):
    # Arrange
    questions = self._SetupQuestions()
    queries = [q.question for q in questions]
    mock_run_iteration.side_effect = [
        self._CreateSuccessIterationPerformance('1', queries),
        self._CreateSuccessIterationPerformance('2', queries),
    ]

    # Act
    edw_conversational_analytics_benchmark.Run(self.spec)

    # Assert
    self.assertEqual(mock_run_iteration.call_count, 2)
    mock_run_iteration.assert_has_calls([
        mock.call(mock.ANY, iteration_id='1'),
        mock.call(mock.ANY, iteration_id='2'),
    ])

    # Inspect suite properties passed in call 1
    suite_1 = mock_run_iteration.call_args_list[0][0][0]
    self.assertEqual(
        suite_1.ca_expected_queries,
        ['What is the total revenue?', 'Show me top users'],
    )
    self.assertEqual(
        suite_1.gt_expected_queries,
        ['What is the total revenue?_gt', 'Show me top users_gt'],
    )
    self.assertIsNone(suite_1.predict_expected_queries)

    # Inspect suite properties passed in call 2 (same instance).
    suite_2 = mock_run_iteration.call_args_list[1][0][0]
    self.assertIs(suite_1, suite_2)

  @flagsaver.flagsaver(
      bq_ca_agent='projects/test/locations/us-central1/dataAgents/test-agent',
      edw_suite_iterations=2,
      dataset='call_center',
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite,
      'RunIteration',
      autospec=True,
  )
  def testRunReturnsCorrectSamples(self, mock_run_iteration):
    # Arrange
    self._SetupQuestions()

    # Iteration 1 perfs
    ca_iter1 = results_aggregator.EdwPowerIterationPerformance('1', 2)
    ca_iter1.add_query_performance(
        'What is the total revenue?', 1.5, {'job_id': 'job1'}
    )
    ca_iter1.add_query_performance('Show me top users', 2.0, {'job_id': 'job2'})

    gt_iter1 = results_aggregator.EdwPowerIterationPerformance('1', 2)
    gt_iter1.add_query_performance(
        'What is the total revenue?_gt',
        0.5,
        {
            'job_id': 'gt_job1',
            'query_results': {'rows': [{'a': 1}]},
            'ground_truth_data': {'rows': [{'a': 1}]},
        },
    )
    gt_iter1.add_query_performance(
        'Show me top users_gt',
        0.8,
        {
            'job_id': 'gt_job2',
            'query_results': {'rows': [{'b': 2}]},
            'ground_truth_data': {'rows': [{'b': 2}]},
        },
    )

    # Iteration 2 perfs
    ca_iter2 = results_aggregator.EdwPowerIterationPerformance('2', 2)
    ca_iter2.add_query_performance(
        'What is the total revenue?', 1.2, {'job_id': 'job3'}
    )
    ca_iter2.add_query_performance('Show me top users', 2.2, {'job_id': 'job4'})

    gt_iter2 = results_aggregator.EdwPowerIterationPerformance('2', 2)
    gt_iter2.add_query_performance(
        'What is the total revenue?_gt',
        0.4,
        {
            'job_id': 'gt_job3',
            'query_results': {'rows': [{'c': 3}]},
            'ground_truth_data': {'rows': [{'c': 3}]},
        },
    )
    gt_iter2.add_query_performance(
        'Show me top users_gt',
        0.9,
        {
            'job_id': 'gt_job4',
            'query_results': {'rows': [{'d': 4}]},
            'ground_truth_data': {'rows': [{'d': 4}]},
        },
    )

    mock_run_iteration.side_effect = [
        (ca_iter1, gt_iter1, None),
        (ca_iter2, gt_iter2, None),
    ]

    # Act
    samples = edw_conversational_analytics_benchmark.Run(self.spec)

    # Assert
    # Sample count assertions
    self.assertLen(samples, 18)

    # Assert metric types and content
    metrics = [s.metric for s in samples]
    self.assertEqual(metrics.count('edw_raw_query_time'), 8)
    self.assertEqual(metrics.count('edw_aggregated_query_time'), 4)
    self.assertEqual(metrics.count('edw_iteration_geomean_time'), 4)
    self.assertEqual(metrics.count('edw_aggregated_geomean'), 2)

    # GT Metadata Verification
    gt_samples = [
        s
        for s in samples
        if s.metric == 'edw_raw_query_time'
        and s.metadata.get('query').endswith('_gt')
    ]
    self.assertLen(gt_samples, 4)
    self.assertEqual(
        gt_samples[0].metadata.get('ground_truth_data'), {'rows': [{'a': 1}]}
    )
    self.assertEqual(
        gt_samples[1].metadata.get('ground_truth_data'), {'rows': [{'b': 2}]}
    )
    self.assertEqual(
        gt_samples[2].metadata.get('ground_truth_data'), {'rows': [{'c': 3}]}
    )
    self.assertEqual(
        gt_samples[3].metadata.get('ground_truth_data'), {'rows': [{'d': 4}]}
    )

  @flagsaver.flagsaver(
      bq_ca_agent='projects/test/locations/us-central1/dataAgents/test-agent',
      edw_suite_iterations=1,
      dataset='call_center',
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite,
      'RunIteration',
      autospec=True,
  )
  def testRunFiltersQuestionsByDatasetFlag(self, mock_run_iteration):
    # Arrange
    self._SetupQuestions([
        edw_service.ConversationalAnalyticsQuestion(
            question='What is the total revenue?',
            db_id='call_center',
            ground_truth_sql='SELECT 1;',
        ),
        edw_service.ConversationalAnalyticsQuestion(
            question='Count impressions',
            db_id='ecomm',
            ground_truth_sql='SELECT 2;',
        ),
    ])
    active_queries = ['What is the total revenue?']
    mock_run_iteration.return_value = self._CreateSuccessIterationPerformance(
        '1', active_queries
    )

    # Act
    edw_conversational_analytics_benchmark.Run(self.spec)

    # Assert
    mock_run_iteration.assert_called_once_with(mock.ANY, iteration_id='1')
    self_arg = mock_run_iteration.call_args[0][0]
    self.assertLen(self_arg.question_list, 1)
    self.assertEqual(
        self_arg.question_list[0].question, 'What is the total revenue?'
    )

  @flagsaver.flagsaver(
      bq_ca_agent='projects/test/locations/us-central1/dataAgents/test-agent',
      edw_suite_iterations=1,
      dataset='call_center',
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite,
      'RunIteration',
      autospec=True,
  )
  def testRunDoesNotRaiseRunErrorWhenConversationalQueryFails(
      self, mock_run_iteration
  ):
    # Arrange
    self._SetupQuestions()

    ca_iter = results_aggregator.EdwPowerIterationPerformance('1', 2)
    ca_iter.add_query_performance('What is the total revenue?', -1.0, {})
    ca_iter.add_query_performance('Show me top users', -1.0, {})

    gt_iter = results_aggregator.EdwPowerIterationPerformance('1', 2)
    gt_iter.add_query_performance(
        'What is the total revenue?_gt',
        0.5,
        {'query_results': {'rows': [{'a': 1}]}},
    )
    gt_iter.add_query_performance(
        'Show me top users_gt', 0.8, {'query_results': {'rows': [{'b': 2}]}}
    )

    mock_run_iteration.return_value = (ca_iter, gt_iter, None)

    # Act
    samples = edw_conversational_analytics_benchmark.Run(self.spec)

    # Assert
    self.assertTrue(samples)

  @flagsaver.flagsaver(
      bq_ca_agent='projects/test/locations/us-central1/dataAgents/test-agent',
      edw_suite_iterations=1,
      dataset='call_center',
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite,
      'RunIteration',
      autospec=True,
  )
  def testRunRaisesRunErrorWhenGroundTruthQueryFails(self, mock_run_iteration):
    # Arrange
    self._SetupQuestions()

    ca_iter = results_aggregator.EdwPowerIterationPerformance('1', 2)
    ca_iter.add_query_performance('What is the total revenue?', 1.5, {})
    ca_iter.add_query_performance('Show me top users', 2.0, {})

    gt_iter = results_aggregator.EdwPowerIterationPerformance('1', 2)
    gt_iter.add_query_performance('What is the total revenue?_gt', -1.0, {})
    gt_iter.add_query_performance('Show me top users_gt', 0.8, {})

    mock_run_iteration.return_value = (ca_iter, gt_iter, None)

    # Act & Assert
    with self.assertRaises(errors.Benchmarks.RunError):
      edw_conversational_analytics_benchmark.Run(self.spec)

  def testRunConversationalQuery(self):
    # Arrange
    q = mock.Mock(question='What is the revenue?')
    ca_client = mock.Mock()
    ca_client.ExecuteQuery.return_value = (1.5, {'meta': 'data'})
    ca_iteration_performance = mock.Mock()

    # Act
    edw_conversational_analytics_benchmark._RunConversationalQuery(
        q, ca_client, ca_iteration_performance
    )

    # Assert
    ca_client.ExecuteQuery.assert_called_once_with('What is the revenue?')
    ca_iteration_performance.add_query_performance.assert_called_once_with(
        'What is the revenue?', 1.5, {'meta': 'data'}
    )

  @mock.patch.object(
      edw_conversational_analytics_benchmark,
      '_GetSerializedMetadataSize',
      autospec=True,
  )
  def testEnforceQueryResultSizeLimit(self, mock_get_size):
    with self.subTest(name='within_limit'):
      metadata_ok = {'foo': 'bar'}
      mock_get_size.return_value = 100
      res = edw_conversational_analytics_benchmark._EnforceQueryResultSizeLimit(
          metadata_ok, 2.5
      )
      self.assertEqual(res, 2.5)
      self.assertNotIn('is_result_too_large', metadata_ok)

    with self.subTest(name='exceeds_limit'):
      metadata_large = {'query_results': 'large_data'}
      mock_get_size.return_value = 900000
      res = edw_conversational_analytics_benchmark._EnforceQueryResultSizeLimit(
          metadata_large, 2.5
      )
      self.assertEqual(res, -1.0)
      self.assertTrue(metadata_large.get('is_result_too_large'))
      self.assertNotIn('query_results', metadata_large)

  @mock.patch.object(
      edw_conversational_analytics_benchmark,
      '_GetSerializedMetadataSize',
      autospec=True,
  )
  def testRunConversationalQueryTooLarge(self, mock_get_size):
    # Arrange
    q = mock.Mock(question='What is the revenue?')
    ca_client = mock.Mock()
    ca_client.ExecuteQuery.return_value = (1.5, {'predict_data': 'results'})
    ca_iteration_performance = mock.Mock()
    mock_get_size.return_value = 900000

    # Act
    edw_conversational_analytics_benchmark._RunConversationalQuery(
        q, ca_client, ca_iteration_performance
    )

    # Assert
    ca_iteration_performance.add_query_performance.assert_called_once_with(
        'What is the revenue?',
        -1.0,
        {
            'is_result_too_large': True,
            'error_message': (
                'Query result size exceeded safety limit of 800KB. Got 900000'
                ' bytes.'
            ),
        },
    )

  def testRunGroundTruthQuery(self):
    # Arrange
    q = mock.Mock(
        question='What is the revenue?',
        db_id='ecomm',
        ground_truth_sql='SELECT 1;',
    )
    query_client = mock.Mock()
    query_client.client_vm = mock.Mock()
    query_client.ExecuteQuery.return_value = (
        0.5,
        {'query_results': 'results'},
    )
    gt_iteration_performance = mock.Mock()

    # Act
    edw_conversational_analytics_benchmark._RunGroundTruthQuery(
        q, query_client, gt_iteration_performance
    )

    # Assert
    self.mock_create_remote_file.assert_called_once_with(
        query_client.client_vm, 'SELECT 1;', 'ecomm_gt.sql'
    )
    query_client.ExecuteQuery.assert_called_once_with(
        'ecomm_gt.sql', print_results=True
    )
    gt_iteration_performance.add_query_performance.assert_called_once_with(
        'What is the revenue?_gt',
        0.5,
        {
            'query_results': 'results',
            'question': 'What is the revenue?',
            'ground_truth_sql': 'SELECT 1;',
            'ground_truth_data': 'results',
        },
    )

  @mock.patch.object(
      edw_conversational_analytics_benchmark, '_RunConversationalQuery'
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark, '_RunGroundTruthQuery'
  )
  def testRunIteration(self, mock_run_gt, mock_run_ca):
    # Arrange
    question_list = [mock.Mock(), mock.Mock()]
    ca_client = mock.Mock()
    query_client = mock.Mock()

    suite = edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite(
        edw_service_instance=self.service,
        ca_client=ca_client,
        query_client=query_client,
        question_list=question_list,
        ca_expected_queries=['q1', 'q2'],
        ca_performance=mock.Mock(),
        gt_expected_queries=['q1_gt', 'q2_gt'],
    )

    # Act
    ca_perf, gt_perf, predict_perf = suite.RunIteration(iteration_id='1')

    # Assert
    self.assertEqual(mock_run_ca.call_count, 2)
    self.assertEqual(mock_run_gt.call_count, 2)
    assert ca_perf is not None
    assert gt_perf is not None
    self.assertEqual(ca_perf.id, '1')
    self.assertEqual(gt_perf.id, '1')
    self.assertIsNone(predict_perf)

  def testRetrievePredictQuerySuccess(self):
    # Arrange
    q = mock.Mock(question='What is the revenue?')
    ca_iteration_performance = mock.Mock()
    query_performance = mock.Mock()
    query_performance.is_successful.return_value = True
    query_performance.metadata = {'generated_sql': 'SELECT * FROM revenue'}
    ca_iteration_performance.performance = {
        'What is the revenue?': query_performance
    }

    # Act
    predict_sql = edw_conversational_analytics_benchmark._RetrievePredictQuery(
        q, ca_iteration_performance
    )

    # Assert
    self.assertEqual(predict_sql, 'SELECT * FROM revenue')

  def testRetrievePredictQueryFailsOrMissing(self):
    # Arrange
    q = mock.Mock(question='What is the revenue?')
    ca_iteration_performance = mock.Mock()
    ca_iteration_performance.performance = {}

    # Act
    predict_sql = edw_conversational_analytics_benchmark._RetrievePredictQuery(
        q, ca_iteration_performance
    )

    # Assert
    self.assertIsNone(predict_sql)

  def testRunPredictQuerySuccess(self):
    # Arrange
    q = mock.Mock(question='What is the revenue?', db_id='ecomm')
    query_client = mock.Mock()
    query_client.client_vm = mock.Mock()
    query_client.ExecuteQuery.return_value = (
        1.5,
        {'query_results': 'results'},
    )
    predict_iteration_performance = mock.Mock()

    # Act
    edw_conversational_analytics_benchmark._RunPredictQuery(
        q, 'SELECT 1;', query_client, predict_iteration_performance
    )

    # Assert
    self.mock_create_remote_file.assert_called_once_with(
        query_client.client_vm, 'SELECT 1;', 'ecomm_predict.sql'
    )
    query_client.ExecuteQuery.assert_called_once_with(
        'ecomm_predict.sql', print_results=True
    )
    predict_iteration_performance.add_query_performance.assert_called_once_with(
        'What is the revenue?_predict',
        1.5,
        {
            'query_results': 'results',
            'predict_data': 'results',
            'question': 'What is the revenue?',
            'predict_sql': 'SELECT 1;',
        },
    )

  @mock.patch.object(
      edw_conversational_analytics_benchmark, '_GetSerializedMetadataSize'
  )
  def testRunPredictQueryTooLarge(self, mock_get_size):
    # Arrange
    q = mock.Mock(question='What is the revenue?', db_id='ecomm')
    query_client = mock.Mock()
    query_client.client_vm = mock.Mock()
    query_client.ExecuteQuery.return_value = (
        1.5,
        {'query_results': 'results'},
    )
    predict_iteration_performance = mock.Mock()
    mock_get_size.return_value = 900000

    # Act
    edw_conversational_analytics_benchmark._RunPredictQuery(
        q, 'SELECT 1;', query_client, predict_iteration_performance
    )

    # Assert
    predict_iteration_performance.add_query_performance.assert_called_once_with(
        'What is the revenue?_predict',
        -1.0,
        {
            'question': 'What is the revenue?',
            'predict_sql': 'SELECT 1;',
            'is_result_too_large': True,
            'error_message': (
                'Query result size exceeded safety limit of 800KB. Got 900000'
                ' bytes.'
            ),
        },
    )

  @mock.patch.object(
      edw_conversational_analytics_benchmark, '_RunConversationalQuery'
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark, '_RunGroundTruthQuery'
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark, '_RetrievePredictQuery'
  )
  @mock.patch.object(edw_conversational_analytics_benchmark, '_RunPredictQuery')
  def testRunIterationSnowflake(
      self, mock_run_predict, mock_retrieve_predict, mock_run_gt, mock_run_ca
  ):
    # Arrange
    question_list = [mock.Mock(), mock.Mock()]
    ca_client = mock.Mock()
    ca_client.fetches_results_immediately = False
    query_client = mock.Mock()
    snowflake_service = mock.Mock(SERVICE_TYPE='snowflake_aws')
    mock_retrieve_predict.return_value = 'SELECT 1;'

    suite = edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite(
        edw_service_instance=snowflake_service,
        ca_client=ca_client,
        query_client=query_client,
        question_list=question_list,
        ca_expected_queries=['q1', 'q2'],
        ca_performance=mock.Mock(),
        predict_expected_queries=['q1_predict', 'q2_predict'],
    )

    # Act
    ca_perf, gt_perf, predict_perf = suite.RunIteration(iteration_id='1')

    # Assert
    self.assertEqual(mock_run_ca.call_count, 2)
    self.assertEqual(mock_retrieve_predict.call_count, 2)
    self.assertEqual(mock_run_predict.call_count, 2)
    self.assertEqual(mock_run_gt.call_count, 0)
    self.assertIsNotNone(ca_perf)
    self.assertIsNone(gt_perf)
    self.assertIsNotNone(predict_perf)
    self.assertEqual(ca_perf.id, '1')
    self.assertEqual(predict_perf.id, '1')

  @flagsaver.flagsaver(
      snowflake_ca_semantic_view='my-snowflake-semantic-view',
      edw_suite_iterations=2,
      dataset='call_center',
  )
  @mock.patch.object(
      edw_conversational_analytics_benchmark._BenchmarkPerformanceSuite,
      'RunIteration',
      autospec=True,
  )
  def testRunSnowflake(self, mock_run_iteration):
    # Arrange
    mock_snowflake_service = mock.Mock(SERVICE_TYPE='snowflake_aws')
    mock_snowflake_service.GetMetadata.return_value = {'service_meta': 'val'}
    mock_snowflake_client = mock.Mock()
    mock_snowflake_service.GetClientInterface.return_value = (
        mock_snowflake_client
    )

    questions = [
        edw_service.ConversationalAnalyticsQuestion(
            question='What is the total revenue?',
            db_id='call_center',
            ground_truth_sql='SELECT 1;',
        ),
        edw_service.ConversationalAnalyticsQuestion(
            question='Show me top users',
            db_id='call_center',
            ground_truth_sql='SELECT 2;',
        ),
    ]
    mock_snowflake_service.GetConversationalAnalyticsQuestionList.return_value = (
        questions
    )

    mock_ca_client = mock.Mock()
    mock_ca_client.fetches_results_immediately = False

    snowflake_spec = mock.Mock(
        edw_service=mock_snowflake_service,
        ca_client=mock_ca_client,
        vms=[self.mock_client_interface.client_vm],
    )

    ca_iter1 = results_aggregator.EdwPowerIterationPerformance('1', 2)
    ca_iter1.add_query_performance('What is the total revenue?', 1.5, {})
    ca_iter1.add_query_performance('Show me top users', 2.0, {})

    predict_iter1 = results_aggregator.EdwPowerIterationPerformance('1', 2)
    predict_iter1.add_query_performance(
        'What is the total revenue?_predict', 1.0, {}
    )
    predict_iter1.add_query_performance('Show me top users_predict', 1.2, {})

    ca_iter2 = results_aggregator.EdwPowerIterationPerformance('2', 2)
    ca_iter2.add_query_performance('What is the total revenue?', 1.1, {})
    ca_iter2.add_query_performance('Show me top users', 1.9, {})

    predict_iter2 = results_aggregator.EdwPowerIterationPerformance('2', 2)
    predict_iter2.add_query_performance(
        'What is the total revenue?_predict', 0.9, {}
    )
    predict_iter2.add_query_performance('Show me top users_predict', 1.1, {})

    mock_run_iteration.side_effect = [
        (ca_iter1, None, predict_iter1),
        (ca_iter2, None, predict_iter2),
    ]

    # Act
    samples = edw_conversational_analytics_benchmark.Run(snowflake_spec)

    # Assert
    self.assertEqual(mock_run_iteration.call_count, 2)
    mock_run_iteration.assert_has_calls([
        mock.call(mock.ANY, iteration_id='1'),
        mock.call(mock.ANY, iteration_id='2'),
    ])

    # Inspect suite properties passed in call 1
    suite_1 = mock_run_iteration.call_args_list[0][0][0]
    self.assertEqual(
        suite_1.ca_expected_queries,
        ['What is the total revenue?', 'Show me top users'],
    )
    self.assertIsNone(suite_1.gt_expected_queries)
    self.assertEqual(
        suite_1.predict_expected_queries,
        ['What is the total revenue?_predict', 'Show me top users_predict'],
    )

    metrics = [s.metric for s in samples]
    self.assertEqual(metrics.count('edw_raw_query_time'), 8)
    self.assertEqual(metrics.count('edw_aggregated_query_time'), 4)
    self.assertEqual(metrics.count('edw_iteration_geomean_time'), 4)
    self.assertEqual(metrics.count('edw_aggregated_geomean'), 2)
    for sample in samples:
      self.assertEqual(
          sample.metadata.get('agent'), 'my-snowflake-semantic-view'
      )


if __name__ == '__main__':
  unittest.main()
