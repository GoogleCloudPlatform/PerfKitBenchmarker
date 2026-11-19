from collections.abc import Mapping, Sequence
import dataclasses
import functools
import json
import sys
import types
from typing import Any
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import freezegun
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import dpb_sparksql_benchmark_helper
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker.linux_benchmarks import dpb_sparksql_benchmark
from tests import pkb_common_test_case

PY4J_MOCK = mock.Mock()
PYSPARK_MOCK = mock.Mock()
sys.modules['py4j'] = PY4J_MOCK
sys.modules['pyspark'] = PYSPARK_MOCK

from perfkitbenchmarker.scripts.spark_sql_test_scripts import spark_sql_runner

FLAGS = flags.FLAGS
_TPCH_TABLES = [
    'customer',
    'lineitem',
    'nation',
    'orders',
    'part',
    'partsupp',
    'region',
    'supplier',
]


class _FakeDpbService:
  """Minimal fake DPB service recording submissions and cluster costs."""

  def __init__(
      self,
      job_results: Sequence[dpb_service.JobResult],
      job_costs: Sequence[float | None] | None = None,
      create_time: float | None = 100.0,
      cost_per_second: float | None = None,
      cluster_costs: Sequence[float | None] | None = None,
  ):
    self.base_dir = 'gs://test2'
    self.storage_service = None
    self.cluster_duration: float | None = None
    self.metadata: dict[str, Any] = {}
    self.submitted_job_args: list[list[str]] = []
    self.observed_cost_durations: list[float | None] = []
    # Number of jobs submitted when each job's costs were fetched.
    self.jobs_submitted_at_cost_fetches: list[int] = []
    self._job_results = iter(job_results)
    self._job_costs = iter(
        dpb_service.JobCosts(total_cost=cost) for cost in job_costs or ()
    )
    self._create_time = create_time
    self._cost_per_second = cost_per_second
    self._cluster_costs = (
        iter(cluster_costs) if cluster_costs is not None else None
    )

  def SubmitJob(
      self, *, job_arguments, **unused_kwargs
  ) -> dpb_service.JobResult:
    self.submitted_job_args.append(list(job_arguments))
    costs = next(self._job_costs, dpb_service.JobCosts())
    return dataclasses.replace(
        next(self._job_results),
        fetch_costs_fn=functools.partial(self._FetchCosts, costs),
    )

  def _FetchCosts(self, costs: dpb_service.JobCosts) -> dpb_service.JobCosts:
    self.jobs_submitted_at_cost_fetches.append(len(self.submitted_job_args))
    return costs

  def GetResourceMetadata(self) -> dict[str, str]:
    return {'cluster_name': 'test_cluster'}

  def GetClusterCreateTime(self) -> float | None:
    return self._create_time

  def GetClusterCost(self) -> float | None:
    self.observed_cost_durations.append(self.cluster_duration)
    if self._cluster_costs is not None:
      return next(self._cluster_costs)
    if self._cost_per_second is None or self.cluster_duration is None:
      return None
    return self.cluster_duration * self._cost_per_second


def _BaseMetadata(
    run_type: str = 'POWER',
    total_iterations: str = '1',
    total_warmup_iterations: str = '0',
) -> dict[str, str]:
  return {
      'cluster_name': 'test_cluster',
      'benchmark': 'TPC-DS',
      'run_type': run_type,
      'total_iterations': total_iterations,
      'total_warmup_iterations': total_warmup_iterations,
  }


def _Agg(
    *,
    cum: float | None = None,
    wall: float | None = None,
    run_cost: float | None = None,
    cluster_cost: float | None = None,
) -> dict[str, float]:
  metrics = {
      'sparksql_aggregated_cumulative_run_time': cum,
      'sparksql_aggregated_total_wall_time': wall,
      'sparksql_aggregated_run_cost': run_cost,
      'dpb_aggregated_cluster_total_cost': cluster_cost,
  }
  return {k: v for k, v in metrics.items() if v is not None}


def _Summary(
    *,
    clean_iterations: str,
    quorum_met: str,
    failing_queries: str = '',
    warmup_failing_queries: str = '',
    metrics_below_quorum: str = '',
) -> dict[str, str]:
  return {
      'failing_queries': failing_queries,
      'warmup_failing_queries': warmup_failing_queries,
      'clean_iterations': clean_iterations,
      'quorum_met': quorum_met,
      'quorum_criterion': 'strict_majority',
      'metrics_below_quorum': metrics_below_quorum,
  }


def _MakeJobResult(
    query_results: Mapping[str, float] | Sequence[Mapping[str, float]],
    run_time: float,
    pending_time: float = 5.0,
) -> dpb_service.JobResult:
  streams = (
      [query_results] if isinstance(query_results, Mapping) else query_results
  )
  rows = []
  for stream_id, stream in enumerate(streams):
    for query_id, duration in stream.items():
      rows.append(
          {'stream': stream_id, 'query_id': query_id, 'duration': duration}
      )
  stdout = (
      '----@spark_sql_runner:results_start@----\n'
      f'{json.dumps(rows)}\n'
      '----@spark_sql_runner:results_end@----\n'
  )
  return dpb_service.JobResult(
      run_time=run_time, pending_time=pending_time, stdout=stdout
  )


def _MakeBenchmarkSpec(
    dpb_svc: _FakeDpbService,
    queries_and_script_upload_time: float | None = None,
    hive_tables_creation_time: float | None = None,
) -> types.SimpleNamespace:
  return types.SimpleNamespace(
      dpb_service=dpb_svc,
      table_subdirs=list(_TPCH_TABLES),
      query_streams=dpb_sparksql_benchmark_helper.GetStreams(),
      data_dir='gs://datasetbucket',
      queries_and_script_upload_time=queries_and_script_upload_time,
      hive_tables_creation_time=hive_tables_creation_time,
  )


class DpbSparksqlBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.benchmark_spec_mock = mock.MagicMock()
    self.benchmark_spec_mock.dpb_service = mock.Mock(base_dir='gs://test2')
    self.benchmark_spec_mock.dpb_service.GetClusterCost.return_value = None
    self.benchmark_spec_mock.data_dir = 'gs://datasetbucket'
    FLAGS.run_uri = 'fakeru'
    temp_dir.CreateTemporaryDirectories()

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(dpb_sparksql_order=['1', '2', '3'])
  def testRunQueriesPower(self):
    self.benchmark_spec_mock.table_subdirs = list(_TPCH_TABLES)
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-queries',
            '1,2,3',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--table-base-dir',
            'gs://datasetbucket',
            '--table-names',
            *_TPCH_TABLES,
            '--run-uri',
            'fakeru',
        ],
    )

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2', '3'], dpb_sparksql_simultaneous=True
  )
  def testRunQueriesThroughput(self):
    self.benchmark_spec_mock.table_subdirs = list(_TPCH_TABLES)
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-queries',
            '1',
            '--sql-queries',
            '2',
            '--sql-queries',
            '3',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--table-base-dir',
            'gs://datasetbucket',
            '--table-names',
            *_TPCH_TABLES,
            '--run-uri',
            'fakeru',
        ],
    )

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(dpb_sparksql_streams=['1,2,3', '2,1,3', '3,1,2'])
  def testRunQueriesSimultaneous(self):
    self.benchmark_spec_mock.table_subdirs = list(_TPCH_TABLES)
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-queries',
            '1,2,3',
            '--sql-queries',
            '2,1,3',
            '--sql-queries',
            '3,1,2',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--table-base-dir',
            'gs://datasetbucket',
            '--table-names',
            *_TPCH_TABLES,
            '--run-uri',
            'fakeru',
        ],
    )

  @freezegun.freeze_time('2023-03-29')
  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2', '3'], dpb_sparksql_database='tpcds_1t_delta'
  )
  def testRunQueriesFromDatabase(self):
    self.benchmark_spec_mock.data_dir = None
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    _, kwargs = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args
    self.assertEqual(
        kwargs['job_arguments'],
        [
            '--sql-queries',
            '1,2,3',
            '--report-dir',
            'gs://test2/report-1680048000000',
            '--database',
            'tpcds_1t_delta',
            '--run-uri',
            'fakeru',
        ],
    )

  @parameterized.named_parameters(
      dict(testcase_name='Default', extra_flags={}, want_format='parquet'),
      dict(
          testcase_name='OrcFormat',
          extra_flags={'dpb_sparksql_data_format': 'orc'},
          want_format='orc',
      ),
  )
  @flagsaver.flagsaver(dpb_sparksql_order=['1', '2', '3'])
  def testRunnerScriptGetTableMetadata(self, extra_flags, want_format):
    # Arrange
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    self.benchmark_spec_mock.table_subdirs = list(_TPCH_TABLES)
    if extra_flags:
      self.enter_context(flagsaver.flagsaver(**extra_flags))

    # Act
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    job_args = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args.kwargs[
        'job_arguments'
    ]
    parsed_args = spark_sql_runner.parse_args(job_args)
    table_metadata = spark_sql_runner.get_table_metadata(parsed_args)

    # Assert
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    self.assertEqual(
        table_metadata,
        {
            'customer': (want_format, {'path': 'gs://datasetbucket/customer'}),
            'lineitem': (want_format, {'path': 'gs://datasetbucket/lineitem'}),
            'nation': (want_format, {'path': 'gs://datasetbucket/nation'}),
            'orders': (want_format, {'path': 'gs://datasetbucket/orders'}),
            'part': (want_format, {'path': 'gs://datasetbucket/part'}),
            'partsupp': (want_format, {'path': 'gs://datasetbucket/partsupp'}),
            'region': (want_format, {'path': 'gs://datasetbucket/region'}),
            'supplier': (want_format, {'path': 'gs://datasetbucket/supplier'}),
        },
    )

  @parameterized.named_parameters(
      dict(testcase_name='Default', extra_flags={}, want_delim=','),
      dict(
          testcase_name='PipeDelim',
          extra_flags={'dpb_sparksql_csv_delimiter': '|'},
          want_delim='|',
      ),
  )
  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2', '3'], dpb_sparksql_data_format='csv'
  )
  def testRunnerScriptGetTableMetadataCsv(self, extra_flags, want_delim):
    # Arrange
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )
    self.benchmark_spec_mock.table_subdirs = list(_TPCH_TABLES)
    if extra_flags:
      self.enter_context(flagsaver.flagsaver(**extra_flags))

    # Act
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    job_args = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args.kwargs[
        'job_arguments'
    ]
    parsed_args = spark_sql_runner.parse_args(job_args)
    table_metadata = spark_sql_runner.get_table_metadata(parsed_args)

    # Assert
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()

    self.assertEqual(
        table_metadata,
        {
            'customer': (
                'csv',
                {
                    'path': 'gs://datasetbucket/customer',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'lineitem': (
                'csv',
                {
                    'path': 'gs://datasetbucket/lineitem',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'nation': (
                'csv',
                {
                    'path': 'gs://datasetbucket/nation',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'orders': (
                'csv',
                {
                    'path': 'gs://datasetbucket/orders',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'part': (
                'csv',
                {
                    'path': 'gs://datasetbucket/part',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'partsupp': (
                'csv',
                {
                    'path': 'gs://datasetbucket/partsupp',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'region': (
                'csv',
                {
                    'path': 'gs://datasetbucket/region',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
            'supplier': (
                'csv',
                {
                    'path': 'gs://datasetbucket/supplier',
                    'header': 'true',
                    'delimiter': want_delim,
                },
            ),
        },
    )

  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2', '3'],
      dpb_sparksql_bigquery_dataset='tpcds_1t',
      dpb_sparksql_bigquery_tables=list(_TPCH_TABLES),
      bigquery_record_format='ARROW',
      dpb_sparksql_data_format='com.google.cloud.spark.bigquery',
  )
  def testRunnerScriptGetTableMetadataBigQuery(self):
    # Arrange
    self.benchmark_spec_mock.query_streams = (
        dpb_sparksql_benchmark_helper.GetStreams()
    )

    # Act
    dpb_sparksql_benchmark._RunQueries(self.benchmark_spec_mock)
    job_args = self.benchmark_spec_mock.dpb_service.SubmitJob.call_args.kwargs[
        'job_arguments'
    ]
    parsed_args = spark_sql_runner.parse_args(job_args)
    table_metadata = spark_sql_runner.get_table_metadata(parsed_args)

    # Assert
    self.benchmark_spec_mock.dpb_service.SubmitJob.assert_called_once()
    self.assertEqual(
        table_metadata,
        {
            'customer': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.customer', 'readDataFormat': 'ARROW'},
            ),
            'lineitem': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.lineitem', 'readDataFormat': 'ARROW'},
            ),
            'nation': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.nation', 'readDataFormat': 'ARROW'},
            ),
            'orders': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.orders', 'readDataFormat': 'ARROW'},
            ),
            'part': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.part', 'readDataFormat': 'ARROW'},
            ),
            'partsupp': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.partsupp', 'readDataFormat': 'ARROW'},
            ),
            'region': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.region', 'readDataFormat': 'ARROW'},
            ),
            'supplier': (
                'com.google.cloud.spark.bigquery',
                {'table': 'tpcds_1t.supplier', 'readDataFormat': 'ARROW'},
            ),
        },
    )

  def testRenderRunnerScriptWithQueries(self):
    # Arrange
    queries = {
        '1': 'SELECT * FROM hello;',
        '10': "SELECT '\\n' AS newline_repr;",
        '42': (
            'WITH t AS (\n'
            '  SELECT timestamp, value\n'
            '  FROM pkb_data'
            ') SELECT * FROM T LIMIT 1;'
        ),
    }

    # Act
    rendered_script_path = (
        dpb_sparksql_benchmark_helper._RenderRunnerScriptWithQueries(queries)
    )

    # Assert
    with open(rendered_script_path) as f:
      rendered_script = f.read()
    expected_substr = (
        '\nQUERIES = {\n'
        "  '1': 'SELECT * FROM hello;',\n"
        "  '10': \"SELECT '\\\\n' AS newline_repr;\",\n"
        "  '42': 'WITH t AS (\\n  SELECT timestamp, value\\n  FROM pkb_data)"
        " SELECT * FROM T LIMIT 1;',\n"
        '}\n'
    )
    self.assertIn(expected_substr, rendered_script)

  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2', '3', '4'],
      dpb_sparksql_fetch_results_from_logs=True,
  )
  def testSampleFailingQueries(self):
    # Arrange
    self.benchmark_spec_mock.table_subdirs = list(_TPCH_TABLES)
    self.benchmark_spec_mock.query_streams = [['1', '2', '3']]
    self.benchmark_spec_mock.queries_and_script_upload_time = None
    self.benchmark_spec_mock.hive_tables_creation_time = None

    job_result_mock = mock.Mock(
        wall_time=42.0,
        pending_time=5.0,
        stdout=(
            '----@spark_sql_runner:results_start@----\n'
            '[{"query_id": "1", "duration": 10.0},\n'
            ' {"query_id": "2", "duration": 10.0},\n'
            ' {"query_id": "3", "duration": 10.0}]\n'
            '----@spark_sql_runner:results_end@----\n'
        ),
        stderr='',
    )
    self.benchmark_spec_mock.dpb_service.SubmitJob.return_value = (
        job_result_mock
    )
    self.benchmark_spec_mock.dpb_service.GetResourceMetadata.return_value = {
        'cluster_name': 'test_cluster'
    }
    self.benchmark_spec_mock.dpb_service.GetClusterCreateTime.return_value = (
        100.0
    )
    self.benchmark_spec_mock.dpb_service.metadata = {}

    # Act
    samples = dpb_sparksql_benchmark.Run(self.benchmark_spec_mock)

    # Assert
    query_sample = next(s for s in samples if s.metric == 'sparksql_run_time')
    global_sample = next(
        s for s in samples if s.metric == 'sparksql_total_wall_time'
    )
    self.assertIn('failing_queries', global_sample.metadata)
    self.assertEqual(global_sample.metadata['failing_queries'], '4')
    self.assertNotIn('failing_queries', query_sample.metadata)

  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2'],
      dpb_sparksql_warmup_iterations=1,
      dpb_sparksql_iterations=3,
      dpb_sparksql_fetch_results_from_logs=True,
  )
  def testRunWarmupAndMetadataIsolation(self):
    dpb_svc = _FakeDpbService(
        job_results=[
            _MakeJobResult({'1': 10.0, '2': 20.0}, 45.0),
            _MakeJobResult({'1': 40.0, '2': 60.0}, 105.0),
            _MakeJobResult({'1': 80.0, '2': 120.0}, 215.0),
            _MakeJobResult({'1': 100.0, '2': 140.0}, 255.0),
        ],
        create_time=100.0,
        cost_per_second=0.5,
    )
    spec = _MakeBenchmarkSpec(
        dpb_svc,
        queries_and_script_upload_time=12.5,
        hive_tables_creation_time=25.0,
    )

    samples = dpb_sparksql_benchmark.Run(spec)

    self.assertEqual(
        [args[-2:] for args in dpb_svc.submitted_job_args],
        [
            ['--iteration', 'warmup1'],
            ['--iteration', 'iter1'],
            ['--iteration', 'iter2'],
            ['--iteration', 'iter3'],
        ],
    )
    self.assertEqual(
        dpb_svc.observed_cost_durations, [130.0, 200.0, 300.0, 340.0]
    )
    base_metadata = _BaseMetadata(
        total_iterations='3', total_warmup_iterations='1'
    )
    self.assertEqual(dpb_svc.cluster_duration, 670.0)
    self.assertDictEqual(
        dpb_svc.metadata, {**base_metadata, 'failing_queries': ''}
    )

    cum_samples = [
        s for s in samples if s.metric == 'sparksql_cumulative_run_time'
    ]
    self.assertEqual(
        [s.value for s in cum_samples], [30.0, 100.0, 200.0, 240.0]
    )
    self.assertDictEqual(
        cum_samples[0].metadata,
        {**base_metadata, 'warmup_iteration': '1', 'failing_queries': ''},
    )
    self.assertDictEqual(
        cum_samples[1].metadata,
        {**base_metadata, 'iteration': '1', 'failing_queries': ''},
    )
    self.assertEqual(
        [
            s.value
            for s in samples
            if s.metric == 'dpb_iteration_cluster_total_cost'
        ],
        [65.0, 100.0, 150.0, 170.0],
    )
    self.assertDictEqual(
        {s.metric: s.value for s in samples if 'aggregated' in s.metric},
        _Agg(cum=200.0, wall=220.0, cluster_cost=150.0),
    )
    agg_cost = next(
        s for s in samples if s.metric == 'dpb_aggregated_cluster_total_cost'
    )
    self.assertDictEqual(
        agg_cost.metadata,
        {
            **base_metadata,
            'aggregation': 'median',
            'failing_queries': '',
            'clean_aggregated_iterations': '3',
            'quorum_met': 'true',
            'quorum_criterion': 'strict_majority',
        },
    )

    upload_sample = next(
        s for s in samples if s.metric == 'queries_and_script_upload_time'
    )
    hive_sample = next(
        s for s in samples if s.metric == 'hive_tables_creation_time'
    )
    self.assertEqual(upload_sample.value, 12.5)
    self.assertDictEqual(upload_sample.metadata, base_metadata)
    self.assertEqual(hive_sample.value, 25.0)
    self.assertDictEqual(hive_sample.metadata, base_metadata)

  @parameterized.named_parameters(
      dict(
          testcase_name='SingleIterationNoneCreateTime',
          order=['1', '2'],
          job_results=[_MakeJobResult({'1': 15.0, '2': 25.0}, 50.0)],
          create_time=None,
          cost_per_second=0.5,
          expected_observed_durations=[40.0],
          expected_cluster_duration=40.0,
          expected_iter_cluster_costs=[20.0],
          expected_aggregated=_Agg(cum=40.0, wall=55.0, cluster_cost=20.0),
      ),
      dict(
          testcase_name='ClusterCostUsesDefinitionB',
          order=['1'],
          job_results=[
              _MakeJobResult({'1': 100.0}, 105.0),
              _MakeJobResult({'1': 100.0}, 105.0),
          ],
          create_time=100.0,
          cost_per_second=None,
          cluster_costs=[1.5, 4.5],
          expected_observed_durations=[200.0, 200.0],
          expected_cluster_duration=300.0,
          expected_iter_cluster_costs=[1.5, 4.5],
          expected_aggregated=_Agg(cum=100.0, wall=110.0, cluster_cost=3.0),
      ),
      dict(
          testcase_name='ExportJobStatsWithClusterCost',
          order=['1'],
          export_job_stats=True,
          job_results=[
              _MakeJobResult({'1': 100.0}, 105.0),
              _MakeJobResult({'1': 200.0}, 205.0),
              _MakeJobResult({'1': 300.0}, 305.0),
          ],
          job_costs=[1.0, 3.0, 2.0],
          create_time=100.0,
          cost_per_second=0.5,
          expected_observed_durations=[200.0, 300.0, 400.0],
          expected_cluster_duration=700.0,
          expected_iter_run_costs=[1.0, 3.0, 2.0],
          expected_iter_cluster_costs=[100.0, 150.0, 200.0],
          expected_aggregated=_Agg(
              cum=200.0, wall=210.0, run_cost=2.0, cluster_cost=150.0
          ),
      ),
      dict(
          testcase_name='ServerlessJobCostsOnly',
          order=['1'],
          export_job_stats=True,
          job_results=[
              _MakeJobResult({'1': 100.0}, 105.0),
              _MakeJobResult({'1': 200.0}, 205.0),
              _MakeJobResult({'1': 300.0}, 305.0),
          ],
          job_costs=[1.0, 3.0, 2.0],
          create_time=None,
          cost_per_second=None,
          expected_observed_durations=[100.0, 200.0, 300.0],
          expected_cluster_duration=600.0,
          expected_iter_run_costs=[1.0, 3.0, 2.0],
          expected_aggregated=_Agg(cum=200.0, wall=210.0, run_cost=2.0),
      ),
  )
  @flagsaver.flagsaver(dpb_sparksql_fetch_results_from_logs=True)
  def testRunCostModes(
      self,
      *,
      order: list[str],
      job_results: list[dpb_service.JobResult],
      create_time: float | None,
      cost_per_second: float | None,
      expected_observed_durations: list[float],
      expected_cluster_duration: float,
      expected_aggregated: dict[str, float],
      export_job_stats: bool = False,
      job_costs: Sequence[float | None] = (),
      cluster_costs: Sequence[float | None] | None = None,
      expected_iter_run_costs: Sequence[float] = (),
      expected_iter_cluster_costs: Sequence[float] = (),
  ):
    with flagsaver.flagsaver(
        dpb_sparksql_order=order,
        dpb_sparksql_iterations=len(job_results),
        dpb_export_job_stats=export_job_stats,
    ):
      dpb_svc = _FakeDpbService(
          job_results=job_results,
          job_costs=job_costs,
          create_time=create_time,
          cost_per_second=cost_per_second,
          cluster_costs=cluster_costs,
      )
      samples = dpb_sparksql_benchmark.Run(_MakeBenchmarkSpec(dpb_svc))

    self.assertEqual(
        dpb_svc.observed_cost_durations, expected_observed_durations
    )
    self.assertEqual(dpb_svc.cluster_duration, expected_cluster_duration)
    self.assertEqual(
        [s.value for s in samples if s.metric == 'sparksql_run_cost'],
        list(expected_iter_run_costs),
    )
    self.assertEqual(
        [
            s.value
            for s in samples
            if s.metric == 'dpb_iteration_cluster_total_cost'
        ],
        list(expected_iter_cluster_costs),
    )
    self.assertDictEqual(
        {s.metric: s.value for s in samples if 'aggregated' in s.metric},
        expected_aggregated,
    )

  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2'],
      dpb_sparksql_warmup_iterations=1,
      dpb_sparksql_iterations=3,
      dpb_sparksql_fetch_results_from_logs=True,
      dpb_export_job_stats=True,
  )
  def testRunFetchesJobCostsAfterAllIterations(self):
    dpb_svc = _FakeDpbService(
        job_results=[
            _MakeJobResult({'1': 10.0, '2': 20.0}, 35.0) for _ in range(4)
        ],
        job_costs=[0.5, 1.0, 3.0, 2.0],
    )

    samples = dpb_sparksql_benchmark.Run(_MakeBenchmarkSpec(dpb_svc))

    self.assertEqual(dpb_svc.jobs_submitted_at_cost_fetches, [4, 4, 4, 4])
    self.assertEqual(
        [
            (
                s.value,
                s.metadata.get('warmup_iteration'),
                s.metadata.get('iteration'),
            )
            for s in samples
            if s.metric == 'sparksql_run_cost'
        ],
        [
            (0.5, '1', None),
            (1.0, None, '1'),
            (3.0, None, '2'),
            (2.0, None, '3'),
        ],
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='AllMissingJobCostsBelowQuorum',
          job_results=[
              _MakeJobResult({'1': 10.0, '2': 20.0}, 50.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 80.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 120.0),
          ],
          job_costs=[None, None, None],
          expected_cluster_duration=210.0,
          expected_aggregated=_Agg(cum=70.0, wall=85.0),
      ),
      dict(
          testcase_name='TwoOfThreeMissingJobCostsBelowQuorum',
          job_results=[
              _MakeJobResult({'1': 10.0, '2': 20.0}, 50.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 80.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 120.0),
          ],
          job_costs=[5.0, None, None],
          expected_cluster_duration=210.0,
          expected_iter_run_costs=[5.0],
          expected_aggregated=_Agg(cum=70.0, wall=85.0),
      ),
      dict(
          testcase_name='OneFailingOneMissingJobCostBelowQuorum',
          job_results=[
              _MakeJobResult({'1': 10.0}, 50.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 80.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 120.0),
          ],
          job_costs=[9.0, None, 5.0],
          expected_cluster_duration=190.0,
          expected_iter_run_costs=[9.0, 5.0],
          expected_cluster_failing='iter1:2',
          expected_aggregated=_Agg(cum=90.0, wall=105.0),
      ),
      dict(
          testcase_name='OneMissingJobCostMeetingQuorum',
          job_results=[
              _MakeJobResult({'1': 10.0, '2': 20.0}, 50.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 80.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 120.0),
          ],
          job_costs=[4.0, None, 6.0],
          expected_cluster_duration=210.0,
          expected_iter_run_costs=[4.0, 6.0],
          expected_aggregated=_Agg(cum=70.0, wall=85.0, run_cost=5.0),
      ),
      dict(
          testcase_name='MinorityFailingIterationExcludedFromMedian',
          job_results=[
              _MakeJobResult({'1': 100.0, '2': 100.0}, 205.0),
              _MakeJobResult({'1': 10.0}, 15.0),
              _MakeJobResult({'1': 120.0, '2': 120.0}, 245.0),
          ],
          create_time=100.0,
          cost_per_second=0.5,
          expected_cluster_duration=550.0,
          expected_iter_cluster_costs=[150.0, 55.0, 170.0],
          expected_cluster_failing='iter2:2',
          expected_aggregated=_Agg(cum=220.0, wall=230.0, cluster_cost=160.0),
      ),
      dict(
          testcase_name='TwoOfThreeFailingOmitAggregatedMetrics',
          job_results=[
              _MakeJobResult({'1': 10.0}, 20.0),
              _MakeJobResult({'1': 50.0, '2': 50.0}, 110.0),
              _MakeJobResult({'2': 20.0}, 30.0),
          ],
          create_time=100.0,
          cost_per_second=0.5,
          expected_cluster_duration=230.0,
          expected_iter_cluster_costs=[55.0, 100.0, 60.0],
          expected_cluster_failing='iter1:2;iter3:1',
          expected_aggregated=_Agg(),
      ),
      dict(
          testcase_name='SingleFailingIterationOmitsAggregatedMetrics',
          job_results=[_MakeJobResult({'1': 10.0}, 20.0)],
          create_time=100.0,
          cost_per_second=0.5,
          expected_cluster_duration=110.0,
          expected_iter_cluster_costs=[55.0],
          expected_cluster_failing='iter1:2',
          expected_aggregated=_Agg(),
      ),
  )
  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2'], dpb_sparksql_fetch_results_from_logs=True
  )
  def testRunQuorumAndFailureGating(
      self,
      *,
      job_results: list[dpb_service.JobResult],
      expected_cluster_duration: float,
      expected_aggregated: dict[str, float],
      job_costs: Sequence[float | None] | None = None,
      create_time: float | None = None,
      cost_per_second: float | None = None,
      expected_iter_run_costs: Sequence[float] = (),
      expected_iter_cluster_costs: Sequence[float] = (),
      expected_cluster_failing: str = '',
  ):
    with flagsaver.flagsaver(
        dpb_sparksql_iterations=len(job_results),
        dpb_export_job_stats=job_costs is not None,
    ):
      dpb_svc = _FakeDpbService(
          job_results=job_results,
          job_costs=job_costs,
          create_time=create_time,
          cost_per_second=cost_per_second,
      )
      samples = dpb_sparksql_benchmark.Run(_MakeBenchmarkSpec(dpb_svc))

    agg_samples = [s for s in samples if 'aggregated' in s.metric]
    self.assertEqual(dpb_svc.cluster_duration, expected_cluster_duration)
    self.assertEqual(
        dpb_svc.metadata['failing_queries'], expected_cluster_failing
    )
    self.assertEqual(
        [s.value for s in samples if s.metric == 'sparksql_run_cost'],
        list(expected_iter_run_costs),
    )
    self.assertEqual(
        [
            s.value
            for s in samples
            if s.metric == 'dpb_iteration_cluster_total_cost'
        ],
        list(expected_iter_cluster_costs),
    )
    self.assertDictEqual(
        {s.metric: s.value for s in agg_samples}, expected_aggregated
    )
    self.assertEqual(
        [
            (s.metadata.get('aggregation'), s.metadata.get('failing_queries'))
            for s in agg_samples
        ],
        [('median', '')] * len(agg_samples),
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='WarmupFailureDoesNotCount',
          warmup_iterations=1,
          job_results=[
              _MakeJobResult({'1': 10.0}, 15.0),
              _MakeJobResult({'1': 10.0, '2': 20.0}, 35.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 75.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 115.0),
          ],
          expected_clean_aggregated_iterations=_Agg(cum=3, wall=3),
          expected_failing_iterations=0,
          expected_summary=_Summary(
              clean_iterations='3',
              quorum_met='true',
              warmup_failing_queries='warmup1:2',
          ),
      ),
      dict(
          testcase_name='CostMissingOnOneCleanIteration',
          job_results=[
              _MakeJobResult({'1': 10.0, '2': 20.0}, 35.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 75.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 115.0),
          ],
          job_costs=[4.0, None, 6.0],
          expected_clean_aggregated_iterations=_Agg(cum=3, wall=3, run_cost=2),
          expected_failing_iterations=0,
          expected_summary=_Summary(clean_iterations='3', quorum_met='true'),
      ),
      dict(
          testcase_name='OneFailingIterationAndOneMissingCost',
          job_results=[
              _MakeJobResult({'1': 10.0}, 15.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 75.0),
              _MakeJobResult({'1': 50.0, '2': 60.0}, 115.0),
          ],
          job_costs=[9.0, None, 5.0],
          expected_clean_aggregated_iterations=_Agg(cum=2, wall=2),
          expected_failing_iterations=1,
          expected_summary=_Summary(
              failing_queries='iter1:2',
              clean_iterations='2',
              quorum_met='true',
              metrics_below_quorum='sparksql_aggregated_run_cost:1/3',
          ),
      ),
      dict(
          testcase_name='MostIterationsFail',
          job_results=[
              _MakeJobResult({'1': 10.0}, 15.0),
              _MakeJobResult({'1': 30.0, '2': 40.0}, 75.0),
              _MakeJobResult({'2': 60.0}, 65.0),
          ],
          job_costs=[9.0, None, 5.0],
          expected_clean_aggregated_iterations=_Agg(),
          expected_failing_iterations=2,
          expected_summary=_Summary(
              failing_queries='iter1:2;iter3:1',
              clean_iterations='1',
              quorum_met='false',
          ),
      ),
  )
  @flagsaver.flagsaver(
      dpb_sparksql_order=['1', '2'],
      dpb_sparksql_iterations=3,
      dpb_sparksql_fetch_results_from_logs=True,
  )
  def testRunReportsQuorumOutcome(
      self,
      *,
      job_results: list[dpb_service.JobResult],
      expected_clean_aggregated_iterations: dict[str, float],
      expected_failing_iterations: int,
      expected_summary: dict[str, str],
      warmup_iterations: int = 0,
      job_costs: Sequence[float | None] | None = None,
  ):
    with flagsaver.flagsaver(
        dpb_sparksql_warmup_iterations=warmup_iterations,
        dpb_export_job_stats=job_costs is not None,
    ):
      dpb_svc = _FakeDpbService(job_results=job_results, job_costs=job_costs)
      samples = dpb_sparksql_benchmark.Run(_MakeBenchmarkSpec(dpb_svc))

    summary = next(
        s for s in samples if s.metric == 'sparksql_run_failure_summary'
    )
    self.assertDictEqual(
        {
            s.metric: int(s.metadata['clean_aggregated_iterations'])
            for s in samples
            if 'aggregated' in s.metric
        },
        expected_clean_aggregated_iterations,
    )
    self.assertEqual(summary.value, expected_failing_iterations)
    self.assertDictEqual(
        {key: summary.metadata[key] for key in expected_summary},
        expected_summary,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='PowerWarmupAndMeasured',
          flag_overrides=dict(
              dpb_sparksql_order=['1', '2', '3'],
              dpb_sparksql_warmup_iterations=1,
              dpb_sparksql_iterations=3,
          ),
          job_results=[
              _MakeJobResult({'1': 5.0}, 10.0),
              _MakeJobResult({'1': 10.0, '2': 20.0, '3': 30.0}, 65.0),
              _MakeJobResult({'1': 15.0, '2': 25.0}, 45.0),
              _MakeJobResult({'1': 20.0, '2': 30.0, '3': 50.0}, 105.0),
          ],
          expected_iterations=['warmup1', 'iter1', 'iter2', 'iter3'],
          expected_iter_failing=['2,3', '', '3', ''],
          expected_first_iter_metadata={
              **_BaseMetadata('POWER', '3', '1'),
              'warmup_iteration': '1',
              'failing_queries': '2,3',
          },
          expected_agg_cum=[(80.0, '')],
          expected_cluster_failing='warmup1:2,3;iter2:3',
      ),
      dict(
          testcase_name='MultipleStreamsWarmupAndMeasured',
          flag_overrides=dict(
              dpb_sparksql_streams=['1,2,3', '4,5'],
              dpb_sparksql_warmup_iterations=1,
              dpb_sparksql_iterations=2,
          ),
          job_results=[
              _MakeJobResult([{'1': 10.0}], 15.0),
              _MakeJobResult(
                  [{'1': 10.0, '2': 10.0, '3': 10.0}, {'4': 10.0, '5': 10.0}],
                  55.0,
              ),
              _MakeJobResult(
                  [{'1': 10.0, '2': 10.0}, {'4': 10.0, '5': 10.0}], 45.0
              ),
          ],
          expected_iterations=['warmup1', 'iter1', 'iter2'],
          expected_iter_failing=['/0:2,3;/1:4,5', '', '/0:3'],
          expected_first_iter_metadata={
              **_BaseMetadata('THROUGHPUT', '2', '1'),
              'warmup_iteration': '1',
              'failing_queries': '/0:2,3;/1:4,5',
          },
          expected_agg_cum=[],
          expected_cluster_failing='warmup1/0:2,3;warmup1/1:4,5;iter2/0:3',
      ),
      dict(
          testcase_name='SingleStreamFlagSetIncludesZeroPrefix',
          flag_overrides=dict(
              dpb_sparksql_streams=['1,2'],
              dpb_sparksql_iterations=1,
          ),
          job_results=[_MakeJobResult([{'1': 10.0}], 15.0)],
          expected_iterations=['iter1'],
          expected_iter_failing=['/0:2'],
          expected_first_iter_metadata={
              **_BaseMetadata('THROUGHPUT'),
              'iteration': '1',
              'failing_queries': '/0:2',
          },
          expected_agg_cum=[],
          expected_cluster_failing='iter1/0:2',
      ),
  )
  @flagsaver.flagsaver(dpb_sparksql_fetch_results_from_logs=True)
  def testRunFailingQueriesFormatting(
      self,
      *,
      flag_overrides: dict[str, Any],
      job_results: list[dpb_service.JobResult],
      expected_iterations: list[str],
      expected_iter_failing: list[str],
      expected_first_iter_metadata: dict[str, str],
      expected_agg_cum: list[tuple[float, str]],
      expected_cluster_failing: str,
  ):
    with flagsaver.flagsaver(**flag_overrides):
      dpb_svc = _FakeDpbService(job_results=job_results)
      samples = dpb_sparksql_benchmark.Run(_MakeBenchmarkSpec(dpb_svc))

    cum_samples = [
        s for s in samples if s.metric == 'sparksql_cumulative_run_time'
    ]
    agg_cum_samples = [
        s
        for s in samples
        if s.metric == 'sparksql_aggregated_cumulative_run_time'
    ]
    self.assertEqual(
        [args[-2:] for args in dpb_svc.submitted_job_args],
        [['--iteration', label] for label in expected_iterations],
    )
    self.assertEqual(
        [s.metadata['failing_queries'] for s in cum_samples],
        expected_iter_failing,
    )
    self.assertDictEqual(cum_samples[0].metadata, expected_first_iter_metadata)
    self.assertEqual(
        [(s.value, s.metadata['failing_queries']) for s in agg_cum_samples],
        expected_agg_cum,
    )
    self.assertEqual(
        dpb_svc.metadata['failing_queries'], expected_cluster_failing
    )


if __name__ == '__main__':
  unittest.main()
