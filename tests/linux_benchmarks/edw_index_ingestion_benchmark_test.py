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


import multiprocessing
import time
import unittest
from unittest import mock

from absl.testing import parameterized
from perfkitbenchmarker import edw_service
from perfkitbenchmarker.linux_benchmarks import edw_index_ingestion_benchmark
from tests import pkb_common_test_case


class EdwIndexIngestionBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.mock_service = mock.create_autospec(edw_service.EdwService)
    self.manager = multiprocessing.Manager()
    self.results = self.manager.list()
    self.synchro = multiprocessing.Event()

  def _MockExecuteDataLoadQuery(self, service, *args, **kwargs):
    service.GetTableRowCount.return_value = (
        service.GetTableRowCount.return_value[0] + 100,
        {},
    )

  @mock.patch("multiprocessing.Process.start", _MockExecuteDataLoadQuery)
  def TestExecuteDataLoad(self):
    # Configure mocks
    self.mock_service.GetTableRowCount.side_effect = [(0, {}), (1, {}), (2, {})]
    self.mock_service.InsertSearchData.return_value = (1.0, {})
    self.mock_service.GetTableRowCount.return_value = (0, {})

    # Call the function
    edw_index_ingestion_benchmark._ExecuteDataLoad(
        self.mock_service,
        "test_table",
        "test_path",
        self.results,
        self.synchro,
        target_row_count=200,
        interval=0.01,
    )

    # Assertions
    self.assertTrue(self.mock_service.InsertSearchData.called)
    self.assertLen(self.results, 2)
    self.assertTrue(self.synchro.is_set())
    self.assertEqual(self.mock_service.GetTableRowCount.return_value[0], 200)

  @parameterized.parameters((0, True), (100, True), (200, False), (300, False))
  def TestExecuteDataLoadQuery(self, current_rows, under_length):
    # Configure mocks
    self.mock_service.InsertSearchData.return_value = (1.0, {"meta": "data"})
    self.mock_service.GetTableRowCount.return_value = (100, {})
    mock_current_rows = mock.MagicMock()
    mock_current_rows.value = current_rows

    # Call the function
    edw_index_ingestion_benchmark._ExecuteDataLoadQuery(
        self.mock_service,
        "test_table",
        "test_path",
        self.results,
        mock_current_rows,
        target_row_count=200,
        load_iter=0,
    )

    # Assertions
    if under_length:
      self.mock_service.InsertSearchData.assert_called_once_with(
          "test_table", "test_path"
      )
    else:
      self.mock_service.InsertSearchData.assert_not_called()

  def TestExecuteIndexQueries(self):
    # Configure mocks
    self.mock_service.TextSearchQuery.return_value = (0.5, {"meta": "data"})

    # Call the function
    edw_index_ingestion_benchmark._ExecuteIndexQueries(
        self.mock_service,
        "test_table",
        "test_index",
        "test_query",
        self.results,
        num_queries=3,
        interval=0,
    )

    # Assertions
    self.assertEqual(self.mock_service.TextSearchQuery.call_count, 3)
    self.assertLen(self.results, 3)

  def _MockExecuteIndexQuery(self, service, *args, **kwargs):
    return (0.5, {"meta": "data"})

  @mock.patch.object(time, "sleep", return_value=None)
  @mock.patch("multiprocessing.Process.start", _MockExecuteIndexQuery)
  def TestExecuteIndexQueriesSynchro(self, mock_sleep):

    # Call the function
    edw_index_ingestion_benchmark._ExecuteIndexQueriesSynchro(
        self.mock_service,
        "test_table",
        "test_index",
        "test_query",
        self.results,
        self.synchro,
        interval=0.01,
    )

    self.synchro.set()

    # Assertions
    self.assertLen(self.results, 3)
    self.assertTrue(mock_sleep.called)

  def test_execute_index_query(self):
    # Configure mocks
    self.mock_service.TextSearchQuery.return_value = (0.5, {"meta": "data"})

    # Call the function
    edw_index_ingestion_benchmark._ExecuteIndexQuery(
        self.mock_service,
        "test_table",
        "test_index",
        "test_query",
        self.results,
        bench_meta={"block": "test"},
    )

    # Assertions
    self.mock_service.TextSearchQuery.assert_called_once_with(
        "test_table", "test_query", "test_index"
    )
    self.assertLen(self.results, 1)

  @mock.patch.object(time, "time", side_effect=[0, 1, 11, 21])
  @mock.patch.object(time, "sleep", return_value=None)
  def TestWaitForIndexCompletion(self):
    # Configure mocks
    self.mock_service.GetSearchIndexCompletionPercentage.side_effect = [
        (0, {}),
        (50, {}),
        (100, {}),
    ]

    # Call the function
    edw_index_ingestion_benchmark._WaitForIndexCompletion(
        self.mock_service,
        "test_table",
        "test_index",
        self.results,
        start_time=0,
        timeout=30,
        stage_label="test_stage",
        bench_meta={"meta": "data"},
    )

    # Assertions
    self.assertEqual(
        self.mock_service.GetSearchIndexCompletionPercentage.call_count, 3
    )
    samples = list(self.results)
    self.assertLen(samples, 4)  # 0, 50, 100% samples + completion bool

    expected_metrics = {
        0: "time_to_index_percentage",
        50: "time_to_index_percentage",
        100: "time_to_index_percentage",
        -1: "index_build_completed_before_timeout",
    }
    expected_values = {0: 1, 50: 11, 100: 21, -1: True}

    sample_map = {s.metadata.get("index_percentage", -1): s for s in samples}

    for percentage, metric in expected_metrics.items():
      self.assertIn(percentage, sample_map)
      result_sample = sample_map[percentage]
      self.assertEqual(result_sample.metric, metric)
      self.assertEqual(result_sample.value, expected_values[percentage])
      self.assertDictContainsSubset(
          {
              "meta": "data",
              "index_completion_timeout": 30,
              "benchmark_stage": "test_stage",
          },
          result_sample.metadata,
      )

  @mock.patch.object(time, "time", side_effect=[0, 1, 12])
  @mock.patch.object(time, "sleep", return_value=None)
  def TestWaitForIndexCompletionTimeout(self):
    # Configure mocks
    self.mock_service.GetSearchIndexCompletionPercentage.return_value = (0, {})

    # Call the function
    edw_index_ingestion_benchmark._WaitForIndexCompletion(
        self.mock_service,
        "test_table",
        "test_index",
        self.results,
        start_time=0,
        timeout=10,
        stage_label="test_stage",
        bench_meta={"meta": "data"},
    )

    # Assertions
    self.assertLen(self.results, 2)
    samples = list(self.results)
    self.assertLen(samples, 2)  # 0% sample + completion bool

    completion_sample = samples[1]
    self.assertEqual(
        completion_sample.metric, "index_build_completed_before_timeout"
    )
    self.assertEqual(completion_sample.value, False)


if __name__ == "__main__":
  unittest.main()
