"""Tests for dpb_sparksql_benchmark."""

import unittest
from unittest import mock

from absl.testing import flagsaver
import freezegun
from perfkitbenchmarker import dpb_sparksql_benchmark_helper
from perfkitbenchmarker.linux_benchmarks import dpb_sparksql_benchmark
from tests import pkb_common_test_case


class DpbSparksqlBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.benchmark_spec_mock = mock.MagicMock()
    self.benchmark_spec_mock.dpb_service = mock.Mock(base_dir='gs://test2')

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(dpb_sparksql_order=['1', '2', '3'])
  def testRunQueriesPower(self):
    self.benchmark_spec_mock.query_dir = 'gs://test'
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-scripts-dir',
            'gs://test',
            '--sql-scripts',
            '1,2,3',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--enable-hive',
            'True',
        ],
    )

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2', '3'], dpb_sparksql_simultaneous=True
  )
  def testRunQueriesThroughput(self):
    self.benchmark_spec_mock.query_dir = 'gs://test'
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-scripts-dir',
            'gs://test',
            '--sql-scripts',
            '1',
            '--sql-scripts',
            '2',
            '--sql-scripts',
            '3',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--enable-hive',
            'True',
        ],
    )

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(dpb_sparksql_streams=['1,2,3', '2,1,3', '3,1,2'])
  def testRunQueriesSimultaneous(self):
    self.benchmark_spec_mock.query_dir = 'gs://test'
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-scripts-dir',
            'gs://test',
            '--sql-scripts',
            '1,2,3',
            '--sql-scripts',
            '2,1,3',
            '--sql-scripts',
            '3,1,2',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--enable-hive',
            'True',
        ],
    )


if __name__ == '__main__':
  unittest.main()
