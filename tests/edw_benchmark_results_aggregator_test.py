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

from absl import flags
from perfkitbenchmarker import edw_benchmark_results_aggregator as agg
from perfkitbenchmarker import sample
from tests import pkb_common_test_case

METADATA_EMPTY = {}
SECS = 'seconds'
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
ITERATION_METATDATA = {'iteration': '1'}

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

  def test_get_performance_with_iteration_metadata(self):
    q_p = agg.EdwQueryPerformance(QJOB_NAME, QJOB_PERFORMANCE, QJOB_METADATA)
    actual_sample = q_p.get_performance_sample(ITERATION_METATDATA)
    self.assertEqual(actual_sample.metric, 'edw_raw_query_time')
    self.assertEqual(actual_sample.value, QJOB_PERFORMANCE)
    self.assertEqual(actual_sample.unit, SECS)
    expected_metadata = {
        'query': QJOB_NAME,
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL,
        'job_id': QJOB_ID,
        'iteration': '1'
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)


class EdwPowerIterationPerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_add_query_performance(self):
    i_p = agg.EdwPowerIterationPerformance('1', 2)
    q1_p = agg.EdwQueryPerformance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    q2_p = agg.EdwQueryPerformance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    actual_iteration_performance = i_p.performance
    expected_iteration_performance = {Q1_NAME: q1_p, Q2_NAME: q2_p}
    self.assertSameElements(actual_iteration_performance.keys(),
                            expected_iteration_performance.keys())
    self.assertEqual(actual_iteration_performance[Q1_NAME].performance,
                     expected_iteration_performance[Q1_NAME].performance)
    self.assertDictEqual(actual_iteration_performance[Q1_NAME].metadata,
                         expected_iteration_performance[Q1_NAME].metadata)
    self.assertEqual(actual_iteration_performance[Q2_NAME].performance,
                     expected_iteration_performance[Q2_NAME].performance)
    self.assertDictEqual(actual_iteration_performance[Q2_NAME].metadata,
                         expected_iteration_performance[Q2_NAME].metadata)
    self.assertEqual(i_p.total_count, 2)
    self.assertEqual(i_p.successful_count, 2)

  def test_has_query_performance(self):
    i_p = agg.EdwPowerIterationPerformance('1', 2)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    self.assertTrue(i_p.has_query_performance(Q1_NAME))
    self.assertFalse(i_p.has_query_performance(Q2_NAME))
    i_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    self.assertTrue(i_p.has_query_performance(Q2_NAME))

  def test_is_query_successful(self):
    i_p = agg.EdwPowerIterationPerformance('1', 2)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    self.assertTrue(i_p.is_query_successful(Q1_NAME))
    i_p.add_query_performance(QFAIL_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    self.assertFalse(i_p.is_query_successful(QFAIL_NAME))

  def test_get_query_performance(self):
    i_p = agg.EdwPowerIterationPerformance('1', 2)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    actual_query_performance = i_p.get_query_performance(Q1_NAME)
    self.assertEqual(actual_query_performance, Q1_PERFORMANCE)

  def test_get_query_metadata(self):
    i_p = agg.EdwPowerIterationPerformance('1', 4)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE,
                              {'job_id': 'q1_i1_job_id'})
    i_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, {})
    actual_aggregated_query_metadata_q1 = i_p.get_query_metadata(
        Q1_NAME)
    expected_aggregated_query_metadata_q1 = {
        'job_id': 'q1_i1_job_id',
    }
    self.assertDictEqual(actual_aggregated_query_metadata_q1,
                         expected_aggregated_query_metadata_q1)
    actual_aggregated_query_metadata_q2 = i_p.get_query_metadata(
        Q2_NAME)
    expected_aggregated_query_metadata_q2 = {}
    self.assertDictEqual(actual_aggregated_query_metadata_q2,
                         expected_aggregated_query_metadata_q2)

  def test_get_all_query_performance_samples(self):
    i_p = agg.EdwPowerIterationPerformance('1', 10)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    actual_all_query_performance = i_p.get_all_query_performance_samples(
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
    i_p = agg.EdwPowerIterationPerformance('1', 2)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    self.assertTrue(i_p.is_successful(expected_queries=[Q1_NAME, Q2_NAME]))
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i_p.add_query_performance(QFAIL_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    self.assertFalse(
        i2_p.is_successful(expected_queries=[Q1_NAME, Q2_NAME, QFAIL_NAME]))

  def test_get_queries_geomean_performance(self):
    i_p = agg.EdwPowerIterationPerformance('1', 2)
    i_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    iteration_geomean_performance_sample = i_p.get_queries_geomean_performance_sample(
        expected_queries=[Q1_NAME, Q2_NAME], metadata=METADATA_EMPTY)
    self.assertEqual(iteration_geomean_performance_sample.metric,
                     'edw_iteration_geomean_time')
    self.assertEqual(iteration_geomean_performance_sample.value,
                     agg.geometric_mean([Q1_PERFORMANCE, Q2_PERFORMANCE]))
    self.assertEqual(iteration_geomean_performance_sample.unit, SECS)
    self.assertDictEqual(iteration_geomean_performance_sample.metadata,
                         METADATA_EMPTY)


class EdwBenchmarkPerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_add_iteration_performance(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 1)
    i2_p = agg.EdwPowerIterationPerformance('2', 1)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    b_p.add_iteration_performance(i2_p)
    self.assertEqual(len(b_p.iteration_performances), 2)
    self.assertSameElements(b_p.iteration_performances.keys(), ['1', '2'])

  def test_add_iteration_performance_duplicate_iteration(self):
    """Testing the scenario where a iteration with missing query is added."""
    # Creating the bechmark performance
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 1)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('1', 1)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    # Expecting an error to be raised due duplicate iteration ID.
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      b_p.add_iteration_performance(i2_p)

  def test_is_successful_all_query_success(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    self.assertEqual(len(b_p.iteration_performances), 2)
    self.assertTrue(b_p.is_successful())
    self.assertSameElements(b_p.iteration_performances.keys(), ['1', '2'])

  def test_is_successful_not_all_query_success(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    self.assertEqual(len(b_p.iteration_performances), 2)
    self.assertFalse(b_p.is_successful())
    self.assertSameElements(b_p.iteration_performances.keys(), ['1', '2'])

  def test_aggregated_query_status_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    self.assertTrue(b_p.aggregated_query_status(Q1_NAME))
    self.assertTrue(b_p.aggregated_query_status(Q2_NAME))

  def test_aggregated_query_status_look_for_missing_query(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    self.assertTrue(b_p.aggregated_query_status(Q1_NAME))
    self.assertFalse(b_p.aggregated_query_status(QFAIL_NAME))

  def test_aggregated_query_status_look_for_failing_query(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    self.assertTrue(b_p.aggregated_query_status(Q1_NAME))
    self.assertFalse(b_p.aggregated_query_status(Q2_NAME))

  def test_aggregated_query_execution_time_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, 3.0, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, 4.0, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    self.assertEqual(
        b_p.aggregated_query_execution_time(Q1_NAME), (1.0 + 3.0) / 2)
    self.assertEqual(
        b_p.aggregated_query_execution_time(Q2_NAME), (2.0 + 4.0) / 2)

  def test_aggregated_query_execution_time_missing_query(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      b_p.aggregated_query_execution_time(QFAIL_NAME)

  def test_aggregated_query_execution_time_failing_query(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      b_p.aggregated_query_execution_time(Q2_NAME)

  def test_aggregated_query_metadata_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, 1.0, {'job_id': 'q1_i1_job_id'})
    i1_p.add_query_performance(Q2_NAME, 2.0, {'job_id': 'q2_i1_job_id'})
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, 3.0, {'job_id': 'q1_i2_job_id'})
    i2_p.add_query_performance(Q2_NAME, 4.0, {})
    b_p.add_iteration_performance(i2_p)
    actual_aggregated_query_metadata_q1 = b_p.aggregated_query_metadata(Q1_NAME)
    expected_aggregated_query_metadata_q1 = {
        '1' + '_job_id': 'q1_i1_job_id',
        '2' + '_job_id': 'q1_i2_job_id'
    }
    self.assertDictEqual(expected_aggregated_query_metadata_q1,
                         actual_aggregated_query_metadata_q1)
    actual_aggregated_query_metadata_q2 = b_p.aggregated_query_metadata(Q2_NAME)
    expected_aggregated_query_metadata_q2 = {
        '1' + '_job_id': 'q2_i1_job_id',
    }
    self.assertDictEqual(expected_aggregated_query_metadata_q2,
                         actual_aggregated_query_metadata_q2)

  def test_aggregated_query_metadata_missing_query(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      b_p.aggregated_query_metadata(QFAIL_NAME)

  def test_aggregated_query_metadata_failing_query(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i1_p.add_query_performance(Q2_NAME, Q2_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, Q1_PERFORMANCE, METADATA_EMPTY)
    i2_p.add_query_performance(Q2_NAME, QFAIL_PERFORMANCE, METADATA_EMPTY)
    b_p.add_iteration_performance(i2_p)
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      b_p.aggregated_query_metadata(Q2_NAME)

  def test_get_aggregated_query_performance_sample_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, 1.0, {'job_id': 'q1_i1_job_id'})
    i1_p.add_query_performance(Q2_NAME, 2.0, {'job_id': 'q2_i1_job_id'})
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, 3.0, {'job_id': 'q1_i2_job_id'})
    i2_p.add_query_performance(Q2_NAME, 4.0, {})
    b_p.add_iteration_performance(i2_p)
    actual_sample_q1 = b_p.get_aggregated_query_performance_sample(
        Q1_NAME, {'benchmark_name': 'b_name'})
    self.assertEqual(actual_sample_q1.metric, 'edw_aggregated_query_time')
    self.assertEqual(actual_sample_q1.value, (1.0 + 3.0) / 2)
    self.assertEqual(actual_sample_q1.unit, 'seconds')
    expected_metadata_q1 = {
        '1' + '_job_id': 'q1_i1_job_id',
        '2' + '_job_id': 'q1_i2_job_id',
        'query': Q1_NAME,
        'aggregation_method': 'mean',
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL,
        'benchmark_name': 'b_name'
    }
    self.assertDictEqual(actual_sample_q1.metadata, expected_metadata_q1)
    actual_sample_q2 = b_p.get_aggregated_query_performance_sample(Q2_NAME, {})
    self.assertEqual(actual_sample_q2.metric, 'edw_aggregated_query_time')
    self.assertEqual(actual_sample_q2.value, (2.0 + 4.0) / 2)
    self.assertEqual(actual_sample_q2.unit, 'seconds')
    expected_metadata_q2 = {
        '1' + '_job_id': 'q2_i1_job_id',
        'query': Q2_NAME,
        'aggregation_method': 'mean',
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL
    }
    self.assertDictEqual(actual_sample_q2.metadata, expected_metadata_q2)

  def test_get_all_query_performance_samples_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, 1.0, {'job_id': 'q1_i1_job_id'})
    i1_p.add_query_performance(Q2_NAME, 2.0, {'job_id': 'q2_i1_job_id'})
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, 3.0, {'job_id': 'q1_i2_job_id'})
    i2_p.add_query_performance(Q2_NAME, 4.0, {})
    b_p.add_iteration_performance(i2_p)
    actual_sample_list = b_p.get_all_query_performance_samples({})
    self.assertEqual(len(actual_sample_list), 6)
    # 4 raw query samples and 2 aggregated samples
    self.assertSameElements([x.metric for x in actual_sample_list], [
        'edw_raw_query_time', 'edw_raw_query_time', 'edw_raw_query_time',
        'edw_raw_query_time', 'edw_aggregated_query_time',
        'edw_aggregated_query_time'
    ])

  def test_get_aggregated_geomean_performance_sample_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, 1.0, {'job_id': 'q1_i1_job_id'})
    i1_p.add_query_performance(Q2_NAME, 2.0, {'job_id': 'q2_i1_job_id'})
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, 3.0, {'job_id': 'q1_i2_job_id'})
    i2_p.add_query_performance(Q2_NAME, 4.0, {})
    b_p.add_iteration_performance(i2_p)
    actual_sample = b_p.get_aggregated_geomean_performance_sample(
        {'benchmark_name': 'b_name'})
    self.assertEqual(actual_sample.metric, 'edw_aggregated_geomean')
    self.assertEqual(actual_sample.value,
                     agg.geometric_mean([(1.0 + 3.0) / 2, (2.0 + 4.0) / 2]))
    self.assertEqual(actual_sample.unit, 'seconds')
    expected_metadata = {
        'benchmark_name': 'b_name',
        'intra_query_aggregation_method': 'mean',
        'inter_query_aggregation_method': 'geomean'
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)

  def test_get_queries_geomean_performance_samples_passing(self):
    b_p = agg.EdwBenchmarkPerformance(
        total_iterations=2, expected_queries=[Q1_NAME, Q2_NAME])
    i1_p = agg.EdwPowerIterationPerformance('1', 2)
    i1_p.add_query_performance(Q1_NAME, 1.0, {'job_id': 'q1_i1_job_id'})
    i1_p.add_query_performance(Q2_NAME, 2.0, {'job_id': 'q2_i1_job_id'})
    b_p.add_iteration_performance(i1_p)
    i2_p = agg.EdwPowerIterationPerformance('2', 2)
    i2_p.add_query_performance(Q1_NAME, 3.0, {'job_id': 'q1_i2_job_id'})
    i2_p.add_query_performance(Q2_NAME, 4.0, {})
    b_p.add_iteration_performance(i2_p)
    actual_sample_list = b_p.get_queries_geomean_performance_samples(
        {'benchmark_name': 'b_name'})
    self.assertEqual(len(actual_sample_list), 3)
    self.assertSameElements([x.metric for x in actual_sample_list], [
        'edw_iteration_geomean_time', 'edw_iteration_geomean_time',
        'edw_aggregated_geomean'
    ])
    raw_samples = list(
        filter(lambda x: x.metric == 'edw_iteration_geomean_time',
               actual_sample_list))
    actual_raw_samples_values = [x.value for x in raw_samples]
    expected_raw_samples_values = [
        agg.geometric_mean([1.0, 2.0]),
        agg.geometric_mean([3.0, 4.0])
    ]
    self.assertSameElements(actual_raw_samples_values,
                            expected_raw_samples_values)

    aggregated_sample = list(
        filter(lambda x: x.metric == 'edw_aggregated_geomean',
               actual_sample_list))[0]
    self.assertEqual(aggregated_sample.value,
                     agg.geometric_mean([(1.0 + 3.0) / 2, (2.0 + 4.0) / 2]))

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
