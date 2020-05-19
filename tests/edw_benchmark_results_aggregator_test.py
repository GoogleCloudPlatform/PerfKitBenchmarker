# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for edw_benchmark_results_aggregator.py."""

import unittest


from perfkitbenchmarker import edw_benchmark_results_aggregator as agg
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from tests import pkb_common_test_case

SECS = 'seconds'

METADATA_EMPTY = {}

Q1_NAME = 'q1'
Q1_PERFORMANCE = 1.0
Q2_NAME = 'q2'
Q2_PERFORMANCE = 2.0
QFAIL_NAME = 'qfail'
QFAIL_PERFORMANCE = -1.0
QJOB_NAME = 'qjob'
QJOB_PERFORMANCE = 1.0
QJOB_ID = 'qjob_id'
QJOB_METADATA = {'job_id': QJOB_ID}
SUITE_METATDATA = {'suite_scale': 1}

FLAGS = flags.FLAGS


class EdwQueryPerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_get_performance_simple(self):
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    actual_sample = q_p.get_performance_sample(METADATA_EMPTY)
    expected_sample = sample.Sample('edw_raw_query_time', Q1_PERFORMANCE, SECS,
                                    METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, expected_sample.metric)
    self.assertEqual(actual_sample.value, expected_sample.value)
    self.assertEqual(actual_sample.unit, expected_sample.unit)

  def test_get_performance_value(self):
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    self.assertEqual(q_p.get_performance_value(), Q1_PERFORMANCE)

  def test_get_performance_metadata(self):
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    actual_md = q_p.get_performance_metadata()
    self.assertDictEqual(actual_md, METADATA_EMPTY)

  def test_is_successful(self):
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    self.assertTrue(q_p.is_successful())
    q_p = agg.EdwQueryPerformance(QFAIL_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    self.assertFalse(q_p.is_successful())

  def test_get_performance_failed_query(self):
    q_p = agg.EdwQueryPerformance(QFAIL_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    actual_sample = q_p.get_performance_sample(METADATA_EMPTY)
    expected_sample = sample.Sample('edw_raw_query_time', QFAIL_PERFORMANCE,
                                    SECS, METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, expected_sample.metric)
    self.assertEqual(actual_sample.value, expected_sample.value)
    self.assertEqual(actual_sample.unit, expected_sample.unit)

  def test_get_performance_with_no_metadata(self):
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    actual_sample = q_p.get_performance_sample(METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, 'edw_raw_query_time')
    self.assertEqual(actual_sample.value, Q1_PERFORMANCE)
    self.assertEqual(actual_sample.unit, SECS)
    expected_metadata = {
        'query': Q1_NAME,
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)

  def test_get_performance_with_query_metadata(self):
    q_p = agg.EdwQueryPerformance(QJOB_NAME, QJOB_PERFORMANCE, QJOB_METADATA)
    actual_sample = q_p.get_performance_sample(METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, 'edw_raw_query_time')
    self.assertEqual(actual_sample.value, QJOB_PERFORMANCE)
    self.assertEqual(actual_sample.unit, SECS)
    expected_metadata = {
        'query': QJOB_NAME,
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL,
        'job_id': QJOB_ID
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)

  def test_get_performance_with_suite_metadata(self):
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    actual_sample = q_p.get_performance_sample(SUITE_METATDATA)
    self.assertEqual(actual_sample.metric, 'edw_raw_query_time')
    self.assertEqual(actual_sample.value, Q1_PERFORMANCE)
    self.assertEqual(actual_sample.unit, SECS)
    expected_metadata = {
        'query': Q1_NAME,
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL,
        'suite_scale': 1
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)

  def test_get_performance_with_query_and_suite_metadata(self):
    q_p = agg.EdwQueryPerformance(QJOB_NAME, QJOB_PERFORMANCE, QJOB_METADATA)
    actual_sample = q_p.get_performance_sample(SUITE_METATDATA)
    self.assertEqual(actual_sample.metric, 'edw_raw_query_time')
    self.assertEqual(actual_sample.value, QJOB_PERFORMANCE)
    self.assertEqual(actual_sample.unit, SECS)
    expected_metadata = {
        'query': QJOB_NAME,
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL,
        'job_id': QJOB_ID,
        'suite_scale': 1
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)


class EdwSuitePerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_add_query_performance(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 10)
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q_p)
    actual_suite_performance = s_p.performance
    expected_suite_performance = {Q1_NAME: q_p}
    self.assertDictEqual(actual_suite_performance, expected_suite_performance)
    self.assertEqual(s_p.total_count, 10)
    self.assertEqual(s_p.successful_count, 1)
    self.assertEqual(s_p.failure_count, 0)

  def test_has_query_performance(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 10)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q1_p)
    self.assertTrue(s_p.has_query_performance(Q1_NAME))
    q2_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    self.assertFalse(s_p.has_query_performance(Q2_NAME))
    s_p.add_query_performance(q2_p)
    self.assertTrue(s_p.has_query_performance(Q2_NAME))

  def test_is_query_successful(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 10)
    q_pass = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q_pass)
    self.assertTrue(s_p.is_query_successful(Q1_NAME))
    q_fail = agg.EdwQueryPerformance(QFAIL_NAME, QFAIL_PERFORMANCE,
                                     METADATA_EMPTY)
    s_p.add_query_performance(q_fail)
    self.assertFalse(s_p.is_query_successful(QFAIL_NAME))

  def test_get_query_performance(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 10)
    q_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q_p)
    actual_query_performance = s_p.get_query_performance(Q1_NAME)
    self.assertEqual(actual_query_performance.name, Q1_NAME)
    self.assertEqual(actual_query_performance.performance, Q1_PERFORMANCE)
    self.assertEqual(actual_query_performance.execution_status,
                     agg.EdwQueryExecutionStatus.SUCCESSFUL)
    self.assertDictEqual(actual_query_performance.metadata, METADATA_EMPTY)

  def test_get_all_query_performance(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 10)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q1_p)
    q2_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q2_p)
    actual_all_query_performance = s_p.get_all_query_performance_samples(
        METADATA_EMPTY)
    self.assertEqual(len(actual_all_query_performance), 2)
    self.assertListEqual([x.metric for x in actual_all_query_performance],
                         ['edw_raw_query_time', 'edw_raw_query_time'])
    self.assertSameElements([x.value for x in actual_all_query_performance],
                            [Q1_PERFORMANCE, Q2_PERFORMANCE])
    self.assertListEqual([x.unit for x in actual_all_query_performance],
                         [SECS, SECS])
    self.assertSameElements(
        [x.metadata['query'] for x in actual_all_query_performance],
        [Q1_NAME, Q2_NAME])
    self.assertSameElements(
        [x.metadata['execution_status'] for x in actual_all_query_performance],
        [
            agg.EdwQueryExecutionStatus.SUCCESSFUL,
            agg.EdwQueryExecutionStatus.SUCCESSFUL
        ])

  def test_is_successful(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 2)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q1_p)
    q2_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q2_p)
    self.assertTrue(s_p.is_successful())
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 2)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q1_p)
    qfail_p = agg.EdwQueryPerformance(QFAIL_NAME, QFAIL_PERFORMANCE,
                                      METADATA_EMPTY)
    s_p.add_query_performance(qfail_p)
    self.assertFalse(s_p.is_successful())

  def test_get_wall_time_performance(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 10)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q1_p)
    q2_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q2_p)
    suite_wall_time_performance_sample = s_p.get_wall_time_performance_sample(
        METADATA_EMPTY)
    self.assertEqual(suite_wall_time_performance_sample.metric,
                     'edw_raw_wall_time')
    self.assertEqual(suite_wall_time_performance_sample.value,
                     Q1_PERFORMANCE + Q2_PERFORMANCE)
    self.assertEqual(suite_wall_time_performance_sample.unit, SECS)
    self.assertDictEqual(suite_wall_time_performance_sample.metadata,
                         METADATA_EMPTY)

  def test_get_queries_geomean_performance(self):
    s_p = agg.EdwSuitePerformance('suite_name', 'suite_seq', 2)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q1_p)
    q2_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s_p.add_query_performance(q2_p)
    suite_wall_time_performance_sample = s_p.get_queries_geomean_performance_sample(
        METADATA_EMPTY)
    self.assertEqual(suite_wall_time_performance_sample.metric,
                     'edw_raw_geomean_time')
    self.assertEqual(suite_wall_time_performance_sample.value,
                     agg.geometric_mean([Q1_PERFORMANCE, Q2_PERFORMANCE]))
    self.assertEqual(suite_wall_time_performance_sample.unit, SECS)
    self.assertDictEqual(suite_wall_time_performance_sample.metadata,
                         METADATA_EMPTY)


class EdwBenchmarkPerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_add_suite_performance(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 1)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 1)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertEqual(len(b_p.suite_performances), 2)
    self.assertSameElements(b_p.suite_performances.keys(),
                            ['suite_seq_1', 'suite_seq_2'])

  def test_is_successful_all_query_success(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 2)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    q12_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q12_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 2)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    q22_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q22_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertEqual(len(b_p.suite_performances), 2)
    self.assertTrue(b_p.is_successful())
    self.assertSameElements(b_p.suite_performances.keys(),
                            ['suite_seq_1', 'suite_seq_2'])

  def test_is_successful_not_all_query_success(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 2)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    q12_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q12_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 2)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    q22_p = agg.EdwQueryPerformance(Q2_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q22_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertEqual(len(b_p.suite_performances), 2)
    self.assertFalse(b_p.is_successful())
    self.assertSameElements(b_p.suite_performances.keys(),
                            ['suite_seq_1', 'suite_seq_2'])

  def test_aggregated_query_status_passing(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 2)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    q12_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q12_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 2)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    q22_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q22_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertTrue(b_p.aggregated_query_status(Q1_NAME))
    self.assertTrue(b_p.aggregated_query_status(Q2_NAME))

  def test_aggregated_query_status_look_for_missing_query(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 2)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    q12_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q12_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 2)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    q22_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q22_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertTrue(b_p.aggregated_query_status(Q1_NAME))
    self.assertFalse(b_p.aggregated_query_status(QFAIL_NAME))

  def test_aggregated_query_status_look_for_failing_query(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 2)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    q12_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    s1_p.add_query_performance(q12_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 2)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    q22_p = agg.EdwQueryPerformance(Q2_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    s2_p.add_query_performance(q22_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertTrue(b_p.aggregated_query_status(Q1_NAME))
    self.assertFalse(b_p.aggregated_query_status(Q2_NAME))

  def test_aggregated_query_execution_time(self):
    b_p = agg.EdwBenchmarkPerformance(total_iterations=2)
    s1_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_1', 2)
    q11_p = agg.EdwQueryPerformance(Q1_NAME, 1.0, METADATA_EMPTY)
    s1_p.add_query_performance(q11_p)
    q12_p = agg.EdwQueryPerformance(Q2_NAME, 2.0, METADATA_EMPTY)
    s1_p.add_query_performance(q12_p)
    b_p.add_suite_performance('suite_seq_1', s1_p)
    s2_p = agg.EdwSuitePerformance('suite_name', 'suite_seq_2', 2)
    q21_p = agg.EdwQueryPerformance(Q1_NAME, 3.0, METADATA_EMPTY)
    s2_p.add_query_performance(q21_p)
    q22_p = agg.EdwQueryPerformance(Q2_NAME, 4.0, METADATA_EMPTY)
    s2_p.add_query_performance(q22_p)
    qf_p = agg.EdwQueryPerformance(QFAIL_NAME, -1.0, METADATA_EMPTY)
    s2_p.add_query_performance(qf_p)
    b_p.add_suite_performance('suite_seq_2', s2_p)
    self.assertEqual(
        b_p.aggregated_query_execution_time(Q1_NAME), (1.0 + 3.0) / 2)
    self.assertEqual(
        b_p.aggregated_query_execution_time(Q2_NAME), (2.0 + 4.0) / 2)

  def test_geometric_mean_valid_values(self):
    performance_iterable = [1.0, 2.0, 3.0]
    expected_geometric_mean = agg.geometric_mean(performance_iterable)
    self.assertEqual('%.2f' % expected_geometric_mean, '1.82')

  def test_geometric_mean_include_zero_value(self):
    performance_iterable = [1.0, 2.0, 0.0, 3.0]
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      agg.geometric_mean(performance_iterable)

  def test_geometric_mean_include_negative_value(self):
    performance_iterable = [1.0, -2.0, 3.0]
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      agg.geometric_mean(performance_iterable)

  def test_geometric_mean_no_values(self):
    performance_iterable = []
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      agg.geometric_mean(performance_iterable)


if __name__ == '__main__':
  unittest.main()
