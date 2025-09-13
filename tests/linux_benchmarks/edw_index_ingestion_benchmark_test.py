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

import freezegun
from perfkitbenchmarker import edw_service
from perfkitbenchmarker.linux_benchmarks import edw_index_ingestion_benchmark
from perfkitbenchmarker.providers.snowflake import snowflake
from tests import pkb_common_test_case


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
        self.mock_service, 'test_table', 'test_index', 'test_query'
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

    # Call the function
    samples = self.submitter.ExecuteSearchQueryNTimes(3)

    # Assertions
    self.assertEqual(self.mock_service.TextSearchQuery.call_count, 3)
    self.assertLen(samples, 3)

  def test_execute_search_query_until_event(self):
    # Configure mocks
    self.mock_service.TextSearchQuery.return_value = (0.5, {'meta': 'data'})
    event = threading.Event()

    # The mock will be called 3 times and then the event will be set
    def set_event(*args, **kwargs):
      del args, kwargs
      if self.mock_service.TextSearchQuery.call_count == 3:
        event.set()
      return (0.5, {'meta': 'data'})

    self.mock_service.TextSearchQuery.side_effect = set_event

    # Call the function
    samples = self.submitter.ExecuteSearchQueryUntilEvent(event, 1)

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
        stage_label='test_stage',
        bench_meta={'meta': 'data'},
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
              'benchmark_stage': 'test_stage',
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
        stage_label='test_stage',
        bench_meta={'meta': 'data'},
    )
    # Assertions
    self.assertLen(samples, 2)  # 0% sample + completion bool

    completion_sample = samples[1]
    self.assertEqual(
        completion_sample.metric, 'index_build_completed_before_timeout'
    )
    self.assertEqual(completion_sample.value, False)


if __name__ == '__main__':
  unittest.main()
