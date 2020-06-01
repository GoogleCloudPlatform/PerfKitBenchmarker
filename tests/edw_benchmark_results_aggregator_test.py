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

from typing import Dict, Text
import unittest

from perfkitbenchmarker import edw_benchmark_results_aggregator as agg
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from tests import pkb_common_test_case

METADATA_EMPTY = {}
SUITE_NAME = 'suite_name'
SUITE_SEQ_1 = 'suite_seq_1'
SUITE_SEQ_2 = 'suite_seq_2'
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
SUITE_METATDATA = {'suite_scale': 1}

FLAGS = flags.FLAGS


class EdwQueryPerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_get_query_name(self):
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    self.assertEqual(query_performance.get_query_name(), Q1_NAME)

  def test_get_performance_simple(self):
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    actual_sample = query_performance.get_performance_sample(METADATA_EMPTY)
    expected_sample = sample.Sample('edw_raw_query_time', Q1_PERFORMANCE, SECS,
                                    METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, expected_sample.metric)
    self.assertEqual(actual_sample.value, expected_sample.value)
    self.assertEqual(actual_sample.unit, expected_sample.unit)

  def test_get_performance_value(self):
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    self.assertEqual(query_performance.get_performance_value(), Q1_PERFORMANCE)

  def test_get_performance_metadata(self):
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    actual_md = query_performance.get_performance_metadata()
    self.assertDictEqual(actual_md, METADATA_EMPTY)

  def test_is_successful(self):
    query_performance_passing = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    self.assertTrue(query_performance_passing.is_successful())
    query_performance_failing = EdwQueryPerformanceBuilder()\
        .set_query_name(QFAIL_NAME)\
        .set_query_performance(QFAIL_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    self.assertFalse(query_performance_failing.is_successful())

  def test_get_performance_failed_query(self):
    query_performance_failing = EdwQueryPerformanceBuilder()\
        .set_query_name(QFAIL_NAME)\
        .set_query_performance(QFAIL_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    actual_sample = query_performance_failing.get_performance_sample(
        METADATA_EMPTY)
    expected_sample = sample.Sample('edw_raw_query_time', QFAIL_PERFORMANCE,
                                    SECS, METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, expected_sample.metric)
    self.assertEqual(actual_sample.value, expected_sample.value)
    self.assertEqual(actual_sample.unit, expected_sample.unit)

  def test_get_performance_with_no_metadata(self):
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    actual_sample = query_performance.get_performance_sample(METADATA_EMPTY)
    self.assertEqual(actual_sample.metric, 'edw_raw_query_time')
    self.assertEqual(actual_sample.value, Q1_PERFORMANCE)
    self.assertEqual(actual_sample.unit, SECS)
    expected_metadata = {
        'query': Q1_NAME,
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL
    }
    self.assertDictEqual(actual_sample.metadata, expected_metadata)

  def test_get_performance_with_query_metadata(self):
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(QJOB_NAME)\
        .set_query_performance(QJOB_PERFORMANCE)\
        .set_metadata(QJOB_METADATA).build()
    actual_sample = query_performance.get_performance_sample(METADATA_EMPTY)
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
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    actual_sample = query_performance.get_performance_sample(SUITE_METATDATA)
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
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(QJOB_NAME)\
        .set_query_performance(QJOB_PERFORMANCE)\
        .set_metadata(QJOB_METADATA).build()
    actual_sample = query_performance.get_performance_sample(SUITE_METATDATA)
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

  def test_get_suite_sequence(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10).build()
    self.assertEqual(suite_performance.get_suite_sequence(), SUITE_SEQ_1)

  def test_add_query_performance(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10).build()
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    suite_performance.add_query_performance(query_performance)
    actual_suite_performance = suite_performance.performance
    expected_suite_performance = {Q1_NAME: query_performance}
    self.assertDictEqual(actual_suite_performance, expected_suite_performance)
    self.assertEqual(suite_performance.total_count, 10)
    self.assertEqual(suite_performance.successful_count, 1)

  def test_add_query_performance_duplicate_query(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    self.assertTrue(suite_performance.has_query_performance(Q1_NAME))
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q1_NAME)\
        .set_query_performance(Q1_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    # Attempting to add duplicate query performance
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      suite_performance.add_query_performance(query_performance)

  def test_has_query_performance(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    self.assertTrue(suite_performance.has_query_performance(Q1_NAME))
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(Q2_NAME)\
        .set_query_performance(Q2_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    self.assertFalse(suite_performance.has_query_performance(Q2_NAME))
    suite_performance.add_query_performance(query_performance)
    self.assertTrue(suite_performance.has_query_performance(Q2_NAME))

  def test_is_query_successful(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    self.assertTrue(suite_performance.is_query_successful(Q1_NAME))
    query_performance = EdwQueryPerformanceBuilder()\
        .set_query_name(QFAIL_NAME)\
        .set_query_performance(QFAIL_PERFORMANCE)\
        .set_metadata(METADATA_EMPTY).build()
    suite_performance.add_query_performance(query_performance)
    self.assertFalse(suite_performance.is_query_successful(QFAIL_NAME))

  def test_get_query_performance(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    actual_query_performance = suite_performance.get_query_performance(Q1_NAME)
    self.assertEqual(actual_query_performance.name, Q1_NAME)
    self.assertEqual(actual_query_performance.performance, Q1_PERFORMANCE)
    self.assertEqual(actual_query_performance.execution_status,
                     agg.EdwQueryExecutionStatus.SUCCESSFUL)
    self.assertDictEqual(actual_query_performance.metadata, METADATA_EMPTY)

  def test_get_all_query_performance(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(10)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q2_NAME)
            .set_query_performance(Q2_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    actual_all_query_performance = suite_performance\
        .get_all_query_performance_samples(METADATA_EMPTY)
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
    suite_performance_1 = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(2)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q2_NAME)
            .set_query_performance(Q2_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    self.assertTrue(suite_performance_1.is_successful())
    suite_performance_2 = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(2)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(QFAIL_NAME)
            .set_query_performance(QFAIL_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    self.assertFalse(suite_performance_2.is_successful())

  def test_get_wall_time_performance(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(2)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q2_NAME)
            .set_query_performance(Q2_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    wall_time_sample = suite_performance.get_wall_time_performance_sample(
        METADATA_EMPTY)
    self.assertEqual(wall_time_sample.metric, 'edw_raw_wall_time')
    self.assertEqual(wall_time_sample.value, Q1_PERFORMANCE + Q2_PERFORMANCE)
    self.assertEqual(wall_time_sample.unit, SECS)
    self.assertDictEqual(wall_time_sample.metadata, METADATA_EMPTY)

  def test_get_queries_geomean_performance(self):
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_1)\
        .set_total_count(2)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q2_NAME)
            .set_query_performance(Q2_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()

    geomean_sample = suite_performance.get_queries_geomean_performance_sample(
        METADATA_EMPTY)
    self.assertEqual(geomean_sample.metric, 'edw_raw_geomean_time')
    self.assertEqual(geomean_sample.value,
                     agg.geometric_mean([Q1_PERFORMANCE, Q2_PERFORMANCE]))
    self.assertEqual(geomean_sample.unit, SECS)
    self.assertDictEqual(geomean_sample.metadata, METADATA_EMPTY)


class EdwQueryPerformanceBuilder(object):
  """Helper Class to build EdwQueryPerformance objects.

  Attributes:
    query_name: A string name of the query that was executed
    query_performance: A Float value set to the query's completion time in secs.
    metadata: A dictionary of query execution attributes (job_id, etc.)
  """

  def __init__(self):
    self.query_name = ''
    self.query_performance = -1.0
    self.metadata = {}

  def set_query_name(self, query_name: Text):
    """Sets the query name attribute value."""
    self.query_name = query_name
    return self

  def set_query_performance(self, query_performance: float):
    """Sets the query performance attribute value."""
    self.query_performance = query_performance
    return self

  def set_metadata(self, metadata: Dict[str, str]):
    """Sets the metadata attribute value."""
    self.metadata = metadata
    return self

  def build(self) -> agg.EdwQueryPerformance:
    """Builds an instance of agg.EdwQueryPerformance."""
    return agg.EdwQueryPerformance(self.query_name, self.query_performance,
                                   self.metadata)


class EdwSuitePerformanceBuilder(object):
  """Helper Class to build EdwSuitePerformance objects.

  Attributes:
    suite_name: A string name of the suite that was executed
    suite_sequence: The sequence of suite's execution
    total_count: An integer count of the total number of queries in the suite
    performance: A list of EdwQueryPerformances.
  """

  def __init__(self):
    self.suite_name = ''
    self.suite_sequence = ''
    self.total_count = 0
    self.performance = []

  def set_suite_name(self, suite_name: Text):
    """Sets the suite name attribute value."""
    self.suite_name = suite_name
    return self

  def set_suite_sequence(self, suite_sequence: Text):
    """Sets the suite sequence attribute value."""
    self.suite_sequence = suite_sequence
    return self

  def set_total_count(self, total_count: int):
    """Sets the total count attribute value."""
    self.total_count = total_count
    return self

  def add_query_performance(self, query_performance):
    """Adds a EdwQueryPerformance object to the performance attribute."""
    self.performance.append(query_performance)
    return self

  def build(self) -> agg.EdwSuitePerformance:
    """Builds an instance of agg.EdwSuitePerformance."""
    suite_performance = agg.EdwSuitePerformance(self.suite_name,
                                                self.suite_sequence,
                                                self.total_count)
    for x in self.performance:
      suite_performance.add_query_performance(x)
    return suite_performance


class EdwBenchmarkPerformanceBuilder(object):
  """Helper Class to build  EdwBenchmarkPerformance objects.

  Attributes:
    total_iterations: An integer variable set to total of number of iterations
    expected_suite_queries: A list of query names expected in the suite
    performance: A dictionary of suite's execution sequence index (String value)
      to its execution performance (an instance of EdwSuitePerformance)
  """

  def __init__(self):
    self.total_iterations = -1
    self.expected_suite_queries = []
    self.performance = {}

  def set_total_iterations(self, total_iterations: int):
    """Sets the total iterations attribute value."""
    self.total_iterations = total_iterations
    return self

  def set_expected_suite_queries(self, expected_suite_queries):
    """Sets the expected suite queries attribute value."""
    self.expected_suite_queries = expected_suite_queries
    return self

  def add_suite_performance(self, suite_sequence, suite_performance):
    """Adds a EdwSuitePerformance object to the performance attribute."""
    self.performance[suite_sequence] = suite_performance
    return self

  def build(self) -> agg.EdwBenchmarkPerformance:
    """Builds an instance of agg.EdwBenchmarkPerformance."""
    benchmark_performance = agg.EdwBenchmarkPerformance(
        self.total_iterations, self.expected_suite_queries)
    for _, v in self.performance.items():
      benchmark_performance.add_suite_performance(v)
    return benchmark_performance


class EdwBenchmarkPerformanceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_add_suite_performance(self):
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(1)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(1)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertEqual(len(benchmark_performance.suite_performances), 2)
    self.assertSameElements(benchmark_performance.suite_performances.keys(),
                            ['suite_seq_1', 'suite_seq_2'])

  def test_add_suite_performance_missing_suite_query(self):
    """Testing the scenario where a suite with missing query is added."""
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    # Building the second suite performance, which does not have Q2 performance
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_2)\
        .set_total_count(2)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    # Expecting an error to be raised due to missing Q2 in SUITE_SEQ_2
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.add_suite_performance(suite_performance)

  def test_add_suite_performance_non_expected_suite_query(self):
    """Testing the scenario where a suite with extra query is added."""
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    # Building the second suite performance, which has extra q3 performance
    suite_performance = EdwSuitePerformanceBuilder()\
        .set_suite_name(SUITE_NAME)\
        .set_suite_sequence(SUITE_SEQ_2)\
        .set_total_count(2)\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q2_NAME)
            .set_query_performance(Q2_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build())\
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name('Q3')
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    # Expecting an error to be raised due to extra q3 in SUITE_SEQ_2
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.add_suite_performance(suite_performance)

  def test_add_suite_performance_duplicate_suite_performance(self):
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(1)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    suite_performance = EdwSuitePerformanceBuilder() \
        .set_suite_name(SUITE_NAME) \
        .set_suite_sequence(SUITE_SEQ_1) \
        .set_total_count(1) \
        .add_query_performance(
            EdwQueryPerformanceBuilder()
            .set_query_name(Q1_NAME)
            .set_query_performance(Q1_PERFORMANCE)
            .set_metadata(METADATA_EMPTY).build()).build()
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.add_suite_performance(suite_performance)

  def test_is_successful_all_query_success(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertEqual(len(benchmark_performance.suite_performances), 2)
    self.assertTrue(benchmark_performance.is_successful())
    self.assertSameElements(benchmark_performance.suite_performances.keys(),
                            ['suite_seq_1', 'suite_seq_2'])

  def test_is_successful_not_all_query_success(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(QFAIL_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertEqual(len(benchmark_performance.suite_performances), 2)
    self.assertFalse(benchmark_performance.is_successful())
    self.assertSameElements(benchmark_performance.suite_performances.keys(),
                            ['suite_seq_1', 'suite_seq_2'])

  def test_aggregated_query_status_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertTrue(benchmark_performance.aggregated_query_status(Q1_NAME))
    self.assertTrue(benchmark_performance.aggregated_query_status(Q2_NAME))

  def test_aggregated_query_status_look_for_missing_query(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertTrue(benchmark_performance.aggregated_query_status(Q1_NAME))
    self.assertFalse(benchmark_performance.aggregated_query_status(QFAIL_NAME))

  def test_aggregated_query_status_look_for_failing_query(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(QFAIL_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertTrue(benchmark_performance.aggregated_query_status(Q1_NAME))
    self.assertFalse(benchmark_performance.aggregated_query_status(Q2_NAME))

  def test_aggregated_query_execution_time_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    self.assertEqual(
        benchmark_performance.aggregated_query_execution_time(Q1_NAME),
        (1.0 + 3.0) / 2)
    self.assertEqual(
        benchmark_performance.aggregated_query_execution_time(Q2_NAME),
        (2.0 + 4.0) / 2)

  def test_aggregated_query_execution_time_missing_query(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.aggregated_query_execution_time(QFAIL_NAME)

  def test_aggregated_query_execution_time_failing_query(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(QFAIL_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.aggregated_query_execution_time(Q2_NAME)

  def test_aggregated_query_metadata_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    actual_metadata_q1 = benchmark_performance.aggregated_query_metadata(
        Q1_NAME)
    expected_aggregated_query_metadata_q1 = {
        'suite_seq_1' + '_runtime': 1.0,
        'suite_seq_1' + '_job_id': 'q1_s1_job_id',
        'suite_seq_2' + '_runtime': 3.0,
        'suite_seq_2' + '_job_id': 'q1_s2_job_id'
    }
    self.assertDictEqual(actual_metadata_q1,
                         expected_aggregated_query_metadata_q1)
    actual_metadata_q2 = benchmark_performance.aggregated_query_metadata(
        Q2_NAME)
    expected_aggregated_query_metadata_q2 = {
        'suite_seq_1' + '_runtime': 2.0,
        'suite_seq_1' + '_job_id': 'q2_s1_job_id',
        'suite_seq_2' + '_runtime': 4.0
    }
    self.assertDictEqual(actual_metadata_q2,
                         expected_aggregated_query_metadata_q2)

  def test_aggregated_query_metadata_missing_query(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.aggregated_query_metadata(QFAIL_NAME)

  def test_aggregated_query_metadata_failing_query(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(Q2_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(Q1_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(QFAIL_PERFORMANCE)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    with self.assertRaises(agg.EdwPerformanceAggregationError):
      benchmark_performance.aggregated_query_metadata(Q2_NAME)

  def test_get_aggregated_query_performance_sample_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    sample_q1 = benchmark_performance.get_aggregated_query_performance_sample(
        Q1_NAME, {'benchmark_name': 'b_name'})
    self.assertEqual(sample_q1.metric, 'edw_aggregated_query_time')
    self.assertEqual(sample_q1.value, (1.0 + 3.0) / 2)
    self.assertEqual(sample_q1.unit, 'seconds')
    expected_metadata_q1 = {
        'suite_seq_1' + '_runtime': 1.0,
        'suite_seq_1' + '_job_id': 'q1_s1_job_id',
        'suite_seq_2' + '_runtime': 3.0,
        'suite_seq_2' + '_job_id': 'q1_s2_job_id',
        'query': Q1_NAME,
        'aggregation_method': 'mean',
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL,
        'benchmark_name': 'b_name'
    }
    self.assertDictEqual(sample_q1.metadata, expected_metadata_q1)
    sample_q2 = benchmark_performance.get_aggregated_query_performance_sample(
        Q2_NAME, {})
    self.assertEqual(sample_q2.metric, 'edw_aggregated_query_time')
    self.assertEqual(sample_q2.value, (2.0 + 4.0) / 2)
    self.assertEqual(sample_q2.unit, 'seconds')
    expected_metadata_q2 = {
        'suite_seq_1' + '_runtime': 2.0,
        'suite_seq_1' + '_job_id': 'q2_s1_job_id',
        'suite_seq_2' + '_runtime': 4.0,
        'query': Q2_NAME,
        'aggregation_method': 'mean',
        'execution_status': agg.EdwQueryExecutionStatus.SUCCESSFUL
    }
    self.assertDictEqual(sample_q2.metadata, expected_metadata_q2)

  def test_get_all_query_performance_samples_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    sample_list = benchmark_performance.get_all_query_performance_samples({})
    self.assertEqual(len(sample_list), 6)
    # 4 raw query samples and 2 aggregated samples
    self.assertSameElements([x.metric for x in sample_list], [
        'edw_raw_query_time', 'edw_raw_query_time', 'edw_raw_query_time',
        'edw_raw_query_time', 'edw_aggregated_query_time',
        'edw_aggregated_query_time'
    ])

  def test_get_aggregated_wall_time_performance_sample_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    a_sample = benchmark_performance.get_aggregated_wall_time_performance_sample(
        {'benchmark_name': 'b_name'})
    self.assertEqual(a_sample.metric, 'edw_aggregated_wall_time')
    self.assertEqual(a_sample.value, (1.0 + 3.0) / 2 + (2.0 + 4.0) / 2)
    self.assertEqual(a_sample.unit, 'seconds')
    expected_metadata = {
        'benchmark_name': 'b_name',
        'aggregation_method': 'mean'
    }
    self.assertDictEqual(a_sample.metadata, expected_metadata)

  def test_get_wall_time_performance_samples_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    sample_list = benchmark_performance.get_wall_time_performance_samples(
        {'benchmark_name': 'b_name'})
    self.assertEqual(len(sample_list), 3)
    self.assertSameElements(
        [x.metric for x in sample_list],
        ['edw_raw_wall_time', 'edw_raw_wall_time', 'edw_aggregated_wall_time'])
    raw_samples = list(
        filter(lambda x: x.metric == 'edw_raw_wall_time', sample_list))
    actual_raw_samples_values = [x.value for x in raw_samples]
    expected_raw_samples_values = [(1.0 + 2.0), (3.0 + 4.0)]
    self.assertSameElements(actual_raw_samples_values,
                            expected_raw_samples_values)
    aggregated_sample = list(
        filter(lambda x: x.metric == 'edw_aggregated_wall_time',
               sample_list))[0]
    self.assertEqual(aggregated_sample.value, (1.0 + 3.0) / 2 + (2.0 + 4.0) / 2)

  def test_get_aggregated_geomean_performance_sample_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    a_sample = benchmark_performance.get_aggregated_geomean_performance_sample(
        {'benchmark_name': 'b_name'})
    self.assertEqual(a_sample.metric, 'edw_aggregated_geomean')
    self.assertEqual(a_sample.value,
                     agg.geometric_mean([(1.0 + 3.0) / 2, (2.0 + 4.0) / 2]))
    self.assertEqual(a_sample.unit, 'seconds')
    expected_metadata = {
        'benchmark_name': 'b_name',
        'intra_query_aggregation_method': 'mean',
        'inter_query_aggregation_method': 'geomean'
    }
    self.assertDictEqual(a_sample.metadata, expected_metadata)

  def test_get_queries_geomean_performance_samples_passing(self):
    # Creating the bechmark performance
    benchmark_performance = EdwBenchmarkPerformanceBuilder()\
        .set_total_iterations(2)\
        .set_expected_suite_queries([Q1_NAME, Q2_NAME])\
        .add_suite_performance(
            SUITE_SEQ_1,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_1)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(1.0)
                .set_metadata({'job_id': 'q1_s1_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(2.0)
                .set_metadata({'job_id': 'q2_s1_job_id'}).build()).build())\
        .add_suite_performance(
            SUITE_SEQ_2,
            EdwSuitePerformanceBuilder()
            .set_suite_name(SUITE_NAME)
            .set_suite_sequence(SUITE_SEQ_2)
            .set_total_count(2)
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q1_NAME)
                .set_query_performance(3.0)
                .set_metadata({'job_id': 'q1_s2_job_id'}).build())
            .add_query_performance(
                EdwQueryPerformanceBuilder()
                .set_query_name(Q2_NAME)
                .set_query_performance(4.0)
                .set_metadata(METADATA_EMPTY).build()).build()).build()
    sample_list = benchmark_performance.get_queries_geomean_performance_samples(
        {'benchmark_name': 'b_name'})
    self.assertEqual(len(sample_list), 3)
    self.assertSameElements([x.metric for x in sample_list], [
        'edw_raw_geomean_time', 'edw_raw_geomean_time', 'edw_aggregated_geomean'
    ])
    raw_samples = list(
        filter(lambda x: x.metric == 'edw_raw_geomean_time',
               sample_list))
    actual_raw_samples_values = [x.value for x in raw_samples]
    expected_raw_samples_values = [
        agg.geometric_mean([1.0, 2.0]),
        agg.geometric_mean([3.0, 4.0])
    ]
    self.assertSameElements(actual_raw_samples_values,
                            expected_raw_samples_values)

    aggregated_sample = list(
        filter(lambda x: x.metric == 'edw_aggregated_geomean',
               sample_list))[0]
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
