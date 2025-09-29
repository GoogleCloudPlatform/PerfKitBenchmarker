# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
import threading
import time
import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
import freezegun
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import edw_service
from perfkitbenchmarker.linux_benchmarks import edw_index_ingestion_benchmark
from perfkitbenchmarker.providers.gcp import bigquery
from perfkitbenchmarker.providers.snowflake import snowflake
from tests import pkb_common_test_case

_NO_SEARCHES_STEPS = [
    'INITIAL_LOAD',
    'INITIAL_INDEX_WAIT',
    'MAIN',
    'FINAL_INDEX_WAIT',
]
_NO_WAITS_STEPS = [
    'INITIAL_LOAD',
    'INITIAL_SEARCH',
    'MAIN',
    'FINAL_SEARCH',
]


class EdwIndexIngestionBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.mock_service = mock.create_autospec(edw_service.EdwService)
    self.event = threading.Event()
    self.frozen_time = self.enter_context(freezegun.freeze_time('2025-01-01'))

    def advance_time(seconds):
      self.frozen_time.tick(seconds)

    self.mock_sleep = self.enter_context(
        mock.patch.object(time, 'sleep', side_effect=advance_time)
    )
    self.submitter = edw_index_ingestion_benchmark._IndexSearchQuerySubmitter(
        self.mock_service, 'test_table', 'test_index'
    )
    self.mock_run_parallel = self.enter_context(
        mock.patch.object(
            edw_index_ingestion_benchmark.background_tasks,
            'RunParallelProcesses',
            return_value=([], [], []),
        )
    )

  def _mock_background_tasks(self):
    self.enter_context(
        mock.patch.object(
            edw_index_ingestion_benchmark,
            'background_tasks',
            RunParallelProcesses=mock.Mock(return_value=([], [], [])),
        )
    )

  def _mock_bigquery_client_interface(self):
    mock_client_interface = mock.Mock()
    mock_client_interface.ExecuteQuery.return_value = (0.0, {})
    self.enter_context(
        mock.patch.object(
            bigquery,
            'GetBigQueryClientInterface',
            return_value=mock_client_interface,
        )
    )
    return mock_client_interface

  def _mock_index_search_query_submitter(self):
    mock_query_submitter = mock.Mock(
        ExecuteSearchQueryNTimes=mock.Mock(return_value=[]),
    )
    self.enter_context(
        mock.patch.object(
            edw_index_ingestion_benchmark,
            '_IndexSearchQuerySubmitter',
            return_value=mock_query_submitter,
        )
    )
    return mock_query_submitter

  def _mock_benchmark_spec(self, mock_edw_service):
    return mock.create_autospec(
        benchmark_spec.BenchmarkSpec, edw_service=mock_edw_service
    )

  def _mock_wait_for_index_completion(self):
    return self.enter_context(
        mock.patch.object(
            edw_index_ingestion_benchmark,
            '_WaitForIndexCompletion',
            return_value=[],
        )
    )

  @mock.patch('multiprocessing.current_process')
  def test_execute_data_load(self, mock_current_process):
    # Configure mocks
    mock_current_process.return_value.name = 'not_main'
    self.mock_service.GetTableRowCount.side_effect = [
        (0, {}),
        (100, {}),
        (200, {}),
    ]
    self.mock_service.InsertSearchData.return_value = (1.0, {})

    # Call the function
    samples = edw_index_ingestion_benchmark._ExecuteDataLoad(
        self.mock_service,
        'test_table',
        'test_path',
        target_row_count=200,
        interval=0.01,
        ingestion_finished=self.event,
        ingestion_warehouse=None,
    )

    # Assertions
    self.assertTrue(self.mock_service.InsertSearchData.called)
    self.assertLen(samples, 2)
    self.assertTrue(self.event.is_set())

  @mock.patch('multiprocessing.current_process')
  def test_execute_data_load_with_snowflake_warehouse(
      self, mock_current_process
  ):
    # Configure mocks
    mock_current_process.return_value.name = 'not_main'
    mock_snowflake_service = mock.create_autospec(snowflake.Snowflake)
    mock_snowflake_service.GetTableRowCount.side_effect = [
        (0, {}),
        (200, {}),
    ]
    mock_snowflake_service.InsertSearchData.return_value = (1.0, {})

    # Call the function
    edw_index_ingestion_benchmark._ExecuteDataLoad(
        mock_snowflake_service,
        'test_table',
        'test_path',
        target_row_count=200,
        interval=0.01,
        ingestion_finished=self.event,
        ingestion_warehouse='test_warehouse',
    )
    mock_snowflake_service.SetWarehouse.assert_called_once_with(
        'test_warehouse'
    )

  def test_execute_search_query_n_times(self):
    # Configure mocks
    self.mock_service.TextSearchQuery.return_value = (0.5, {'meta': 'data'})
    search_query = edw_index_ingestion_benchmark._SearchQuery(
        name='test_name', term='test_term'
    )

    # Call the function
    samples = self.submitter.ExecuteSearchQueryNTimes(search_query, 3)

    # Assertions
    self.assertEqual(self.mock_service.TextSearchQuery.call_count, 3)
    self.assertLen(samples, 3)

  def test_execute_search_query_until_event(self):
    # Configure mocks
    self.mock_service.TextSearchQuery.return_value = (0.5, {'meta': 'data'})
    event = threading.Event()
    search_query = edw_index_ingestion_benchmark._SearchQuery(
        name='test_name', term='test_term'
    )

    # The mock will be called 3 times and then the event will be set
    def set_event(*args, **kwargs):
      del args, kwargs
      if self.mock_service.TextSearchQuery.call_count == 3:
        event.set()
      return (0.5, {'meta': 'data'})

    self.mock_service.TextSearchQuery.side_effect = set_event

    # Call the function
    samples = self.submitter.ExecuteSearchQueryUntilEvent(
        [search_query], event, 1
    )

    # Assertions
    self.assertEqual(self.mock_service.TextSearchQuery.call_count, 3)
    self.assertLen(samples, 3)
    self.assertTrue(self.mock_sleep.called)

  def test_wait_for_index_completion(self):
    # Configure mocks
    self.mock_service.GetSearchIndexCompletionPercentage.side_effect = [
        (0, {}),
        (50, {}),
        (100, {}),
    ]
    start_time = time.time()

    # Call the function
    samples = edw_index_ingestion_benchmark._WaitForIndexCompletion(
        self.mock_service,
        'test_table',
        'test_index',
        start_time=start_time,
        timeout=30,
        bench_meta={'meta': 'data', 'edw_index_current_step': 'test_stage'},
    )
    # Assertions
    self.assertEqual(
        self.mock_service.GetSearchIndexCompletionPercentage.call_count, 3
    )
    self.assertLen(samples, 4)  # 0, 50, 100% samples + completion bool

    expected_metrics = {
        0: 'time_to_index_percentage',
        50: 'time_to_index_percentage',
        100: 'time_to_index_percentage',
        -1: 'index_build_completed_before_timeout',
    }
    expected_values = {0: 0, 50: 10, 100: 20, -1: True}

    sample_map = {s.metadata.get('index_percentage', -1): s for s in samples}

    for percentage, metric in expected_metrics.items():
      self.assertIn(percentage, sample_map)
      result_sample = sample_map[percentage]
      self.assertEqual(result_sample.metric, metric)
      self.assertEqual(result_sample.value, expected_values[percentage])
      self.assertDictContainsSubset(
          {
              'meta': 'data',
              'index_completion_timeout': 30,
              'edw_index_current_step': 'test_stage',
          },
          result_sample.metadata,
      )

  def test_wait_for_index_completion_timeout(self):
    # Configure mocks
    self.mock_service.GetSearchIndexCompletionPercentage.return_value = (0, {})
    start_time = time.time()

    # Call the function
    samples = edw_index_ingestion_benchmark._WaitForIndexCompletion(
        self.mock_service,
        'test_table',
        'test_index',
        start_time=start_time,
        timeout=10,
        bench_meta={'meta': 'data'},
    )
    # Assertions
    self.assertLen(samples, 2)  # 0% sample + completion bool

    completion_sample = samples[1]
    self.assertEqual(
        completion_sample.metric, 'index_build_completed_before_timeout'
    )
    self.assertEqual(completion_sample.value, False)

  @parameterized.named_parameters(
      ('partitioned', True, 'table_init_partitioned.sql.j2'),
      ('unpartitioned', False, 'table_init.sql.j2'),
  )
  def test_run_initializes_table_with_correct_partitioning(
      self,
      partitioned,
      expected_template,
  ):
    # Arrange
    self.enter_context(mock.patch('multiprocessing.Manager'))
    self.enter_context(
        flagsaver.flagsaver(
            (bigquery.INITIALIZE_SEARCH_TABLE_PARTITIONED, partitioned),
        )
    )
    self._mock_background_tasks()
    self._mock_index_search_query_submitter()
    mock_client_interface = self._mock_bigquery_client_interface()
    mock_edw_spec = mock.Mock(cluster_identifier='project.dataset')
    bq_service = bigquery.Bigquery(mock_edw_spec)
    bq_service.DropSearchIndex = mock.Mock(return_value=(0.0, {}))
    bq_service.InsertSearchData = mock.Mock(return_value=(0.0, {}))
    bq_service.CreateSearchIndex = mock.Mock(return_value=(0.0, {}))
    bq_service.GetSearchIndexCompletionPercentage = mock.Mock(
        return_value=(100.0, {})
    )
    mock_spec = self._mock_benchmark_spec(bq_service)

    # Act
    samples = edw_index_ingestion_benchmark.Run(mock_spec)

    # Assert
    self.assertNotEmpty(samples)
    for sample in samples:
      self.assertEqual(
          sample.metadata['edw_index_table_partitioned'], partitioned
      )
    mock_client_vm = mock_client_interface.client_vm
    mock_client_vm.RenderTemplate.assert_called_once()
    template_path = mock_client_vm.RenderTemplate.call_args[0][0]
    self.assertIn(expected_template, template_path)

  @parameterized.named_parameters(
      dict(
          testcase_name='only_mandatory_steps',
          steps=['INITIAL_LOAD', 'MAIN'],
          drop_search_index_called=True,
          initialize_search_starter_table_called=True,
          insert_search_data_called=True,
          create_search_index_called=True,
          wait_for_index_completion_called=False,
          execute_search_query_n_times_called=False,
          run_parallel_processes_called=True,
      ),
      dict(
          testcase_name='all_steps',
          steps=[s.value for s in edw_index_ingestion_benchmark._Steps],
          drop_search_index_called=True,
          initialize_search_starter_table_called=True,
          insert_search_data_called=True,
          create_search_index_called=True,
          wait_for_index_completion_called=True,
          execute_search_query_n_times_called=True,
          run_parallel_processes_called=True,
      ),
      dict(
          testcase_name='no_searches',
          steps=_NO_SEARCHES_STEPS,
          drop_search_index_called=True,
          initialize_search_starter_table_called=True,
          insert_search_data_called=True,
          create_search_index_called=True,
          wait_for_index_completion_called=True,
          execute_search_query_n_times_called=False,
          run_parallel_processes_called=True,
      ),
      dict(
          testcase_name='no_waits',
          steps=_NO_WAITS_STEPS,
          drop_search_index_called=True,
          initialize_search_starter_table_called=True,
          insert_search_data_called=True,
          create_search_index_called=True,
          wait_for_index_completion_called=False,
          execute_search_query_n_times_called=True,
          run_parallel_processes_called=True,
      ),
  )
  def test_run_with_steps(
      self,
      steps,
      drop_search_index_called,
      initialize_search_starter_table_called,
      insert_search_data_called,
      create_search_index_called,
      wait_for_index_completion_called,
      execute_search_query_n_times_called,
      run_parallel_processes_called,
  ):
    # Arrange
    self.enter_context(mock.patch('multiprocessing.Manager'))
    self.enter_context(
        flagsaver.flagsaver(
            (edw_index_ingestion_benchmark._BENCHMARK_STEPS, steps),
            (edw_index_ingestion_benchmark._SEARCH_QUERIES, ['common:api']),
        )
    )
    mock_query_submitter = self._mock_index_search_query_submitter()
    mock_wait_for_index = self._mock_wait_for_index_completion()
    mock_spec = self._mock_benchmark_spec(self.mock_service)

    # Act
    edw_index_ingestion_benchmark.Run(mock_spec)

    # Assert
    self.assertEqual(
        self.mock_service.DropSearchIndex.called, drop_search_index_called
    )
    self.assertEqual(
        self.mock_service.InitializeSearchStarterTable.called,
        initialize_search_starter_table_called,
    )
    self.assertEqual(
        self.mock_service.InsertSearchData.called, insert_search_data_called
    )
    self.assertEqual(
        self.mock_service.CreateSearchIndex.called,
        create_search_index_called,
    )
    self.assertEqual(
        mock_wait_for_index.called, wait_for_index_completion_called
    )
    self.assertEqual(
        mock_query_submitter.ExecuteSearchQueryNTimes.called,
        execute_search_query_n_times_called,
    )
    self.assertEqual(
        self.mock_run_parallel.called, run_parallel_processes_called
    )

  @parameterized.named_parameters(
      dict(testcase_name='one_query', search_queries=['q1:t1']),
      dict(testcase_name='two_queries', search_queries=['q1:t1', 'q2:t2']),
  )
  def test_run_with_search_queries(self, search_queries):
    # Arrange
    self.enter_context(mock.patch('multiprocessing.Manager'))
    self.enter_context(
        flagsaver.flagsaver(
            (edw_index_ingestion_benchmark._SEARCH_QUERIES, search_queries),
            (
                edw_index_ingestion_benchmark._BENCHMARK_STEPS,
                [s.value for s in edw_index_ingestion_benchmark._Steps],
            ),
        )
    )
    mock_query_submitter = self._mock_index_search_query_submitter()
    self._mock_wait_for_index_completion()
    mock_spec = self._mock_benchmark_spec(self.mock_service)
    parsed_queries = edw_index_ingestion_benchmark._ParseSearchQueries(
        search_queries
    )

    # Act
    edw_index_ingestion_benchmark.Run(mock_spec)

    # Assert
    self.assertEqual(
        mock_query_submitter.ExecuteSearchQueryNTimes.call_count,
        2 * len(search_queries),
    )
    initial_queries_called = [
        c.args[0]
        for c in mock_query_submitter.ExecuteSearchQueryNTimes.call_args_list
        if c.kwargs['bench_meta']['edw_index_current_step'] == 'INITIAL_SEARCH'
    ]
    final_queries_called = [
        c.args[0]
        for c in mock_query_submitter.ExecuteSearchQueryNTimes.call_args_list
        if c.kwargs['bench_meta']['edw_index_current_step'] == 'FINAL_SEARCH'
    ]
    self.assertCountEqual(initial_queries_called, parsed_queries)
    self.assertCountEqual(final_queries_called, parsed_queries)

    # Check call for MAIN step
    self.mock_run_parallel.assert_called_once()
    tasks = self.mock_run_parallel.call_args[0][0]
    # The second task is ExecuteSearchQueryUntilEvent
    self.assertEqual(
        tasks[1][0], mock_query_submitter.ExecuteSearchQueryUntilEvent
    )
    search_task_args = tasks[1][1]
    self.assertEqual(search_task_args[0], parsed_queries)


if __name__ == '__main__':
  unittest.main()
