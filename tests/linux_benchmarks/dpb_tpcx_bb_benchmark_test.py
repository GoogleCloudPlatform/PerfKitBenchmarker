"""Tests for the TPCx-BB benchmark."""

import os
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import dpb_tpcx_bb_benchmark
from perfkitbenchmarker.linux_packages import tpcx_bb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_KIT_DIR = '/opt/pkb/tpcx_bb/TPCX-BB_Tools_1.7.0'


def _Bbqpm(t_tt: float) -> float:
  return tpcx_bb.ComputeBbqpm(100, 10.0, 900.0, t_tt)


def _DriverLog(
    t_tt: float = 500.0,
    verdict: str = 'VALID',
    bbqpm: float | None = None,
    scale_factor: int = 100,
) -> str:
  """Returns a driver log whose BBQpm is consistent with its times."""
  if bbqpm is None:
    bbqpm = _Bbqpm(t_tt)
  return (
      'BigBench preparation: Data generation                   finished. '
      'Time:          0h:01m:00s:500ms\r\r\n'
      'T_LOAD = 100.0\r\r\n'
      'T_LD = 0.1 * T_LOAD: 10.0\r\r\n'
      'T_PT = 900.0\r\r\n'
      f'T_T_PUT = {2 * t_tt}\r\r\n'
      f'T_TT = {t_tt}\r\r\n'
      f'{verdict} BBQpm@{scale_factor} = {bbqpm}\r\r\n'
  )


_TIMES_CSV = (
    'STARTDATE_EPOCH|STOPDATE_EPOCH|DURATION_MS|STARTDATE|STOPDATE|DURATION|'
    'QUERY|SCALE_FACTOR|BENCHMARK_PHASE|STREAM|SUCCESSFUL|EXIT_CODE|HAS_RESULT|'
    'HDFS_SIZE\n'
    '1|2|29000|a|b|c|q01|100|power_test|0|SUCCESS|0|HAS_RESULT|1636\n'
    '1|2|25000|a|b|c|q01|100|throughput_test_1|1|FAILED|1|HAS_RESULT|32\n'
)


def _MakeConfig(service_type: str) -> mock.Mock:
  config = mock.Mock()
  config.dpb_service.service_type = service_type
  return config


class DpbTpcxBbBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'fakeru'
    self.leader = mock.Mock(
        internal_ip='10.0.0.2', total_memory_kb=128000 * 1024
    )
    self.workers = []
    for _ in range(3):
      vm = mock.Mock(total_memory_kb=128000 * 1024)
      vm.NumCpusForBenchmark.return_value = 16
      self.workers.append(vm)
    self.service = mock.Mock(spec=dpb_service.UnmanagedDpbServiceYarnCluster)
    self.service.leader = self.leader
    self.service.vms = {'worker_group': self.workers}
    self.service.metadata = {}
    self.service.GetResourceMetadata.return_value = {
        'dpb_service': dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER
    }
    self.service.resource_ready_time = None
    self.service.create_start_time = None
    self.benchmark_spec = mock.Mock()
    self.benchmark_spec.dpb_service = self.service
    self.benchmark_spec.tpcx_bb_kit_dir = _KIT_DIR
    self.benchmark_spec.tpcx_bb_spark_properties = {'spark.master': 'yarn'}
    self.enter_context(
        mock.patch.object(
            dpb_tpcx_bb_benchmark.vm_util,
            'GetTempDir',
            return_value=self.create_tempdir().full_path,
        )
    )

  # --- CheckPrerequisites -------------------------------------------------

  @flagsaver.flagsaver(openjdk_version=8, os_type='ubuntu2404')
  def testCheckPrerequisitesOk(self):
    dpb_tpcx_bb_benchmark.CheckPrerequisites(
        _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
    )

  @flagsaver.flagsaver(openjdk_version=8, os_type='ubuntu2204')
  def testCheckPrerequisitesRejectsUnsupportedBackend(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'Invalid backend'):
      dpb_tpcx_bb_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.DATAPROC)
      )

  @flagsaver.flagsaver(openjdk_version=None, os_type='ubuntu2404')
  def testCheckPrerequisitesRequiresJavaVersion(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'openjdk_version'):
      dpb_tpcx_bb_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
      )

  @flagsaver.flagsaver(openjdk_version=11, os_type='ubuntu2404')
  def testCheckPrerequisitesRejectsJava11(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'needs Java 8'):
      dpb_tpcx_bb_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
      )

  @flagsaver.flagsaver(openjdk_version=8, os_type='rhel9')
  def testCheckPrerequisitesRejectsNonDebian(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'Debian'):
      dpb_tpcx_bb_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
      )

  @flagsaver.flagsaver(
      openjdk_version=8, os_type='ubuntu2404', dpb_tpcx_bb_scale_factor=10
  )
  def testCheckPrerequisitesWarnsForNonSpecScaleFactor(self):
    with self.assertLogs(level='WARNING') as logs:
      dpb_tpcx_bb_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
      )
    self.assertIn('not compliant', logs.output[0])

  @flagsaver.flagsaver
  def testFlagBounds(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_bb_scale_factor = 0
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_bb_streams = 0
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_bb_iterations = 0
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_bb_map_tasks = 0
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_bb_run_timeout = 60

  @flagsaver.flagsaver
  def testSparkPropertiesFlagRejectsMissingValue(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_bb_spark_properties = ['spark.executor.cores']

  @flagsaver.flagsaver(
      dpb_tpcx_bb_spark_properties=[
          'spark.executor.cores=8',
          ' spark.a = b=c ',
      ]
  )
  def testGetSparkPropertyOverrides(self):
    self.assertEqual(
        dpb_tpcx_bb_benchmark.GetSparkPropertyOverrides(),
        {'spark.executor.cores': '8', 'spark.a': 'b=c'},
    )

  # --- Compliance ---------------------------------------------------------

  @flagsaver.flagsaver(dpb_tpcx_bb_scale_factor=1000, dpb_tpcx_bb_iterations=2)
  def testNoncomplianceReasonsSpecRun(self):
    self.assertEqual(
        dpb_tpcx_bb_benchmark.GetNoncomplianceReasons(all_valid=True),
        [
            dpb_tpcx_bb_benchmark.REASON_KIT_MODIFIED,
            dpb_tpcx_bb_benchmark.REASON_NO_ENGINE_VALIDATION,
        ],
    )

  @flagsaver.flagsaver(dpb_tpcx_bb_scale_factor=10, dpb_tpcx_bb_iterations=1)
  def testNoncomplianceReasonsDevRun(self):
    self.assertEqual(
        dpb_tpcx_bb_benchmark.GetNoncomplianceReasons(all_valid=False),
        [
            dpb_tpcx_bb_benchmark.REASON_KIT_MODIFIED,
            dpb_tpcx_bb_benchmark.REASON_NO_ENGINE_VALIDATION,
            dpb_tpcx_bb_benchmark.REASON_SCALE_FACTOR,
            dpb_tpcx_bb_benchmark.REASON_ITERATIONS,
            dpb_tpcx_bb_benchmark.REASON_INVALID_RESULT,
        ],
    )

  @flagsaver.flagsaver(dpb_tpcx_bb_scale_factor=1000, hadoop_hdfs_replication=2)
  def testNoncomplianceReasonsLowReplication(self):
    self.assertIn(
        dpb_tpcx_bb_benchmark.REASON_REPLICATION,
        dpb_tpcx_bb_benchmark.GetNoncomplianceReasons(all_valid=True),
    )
    with flagsaver.flagsaver(hadoop_hdfs_replication=3):
      self.assertNotIn(
          dpb_tpcx_bb_benchmark.REASON_REPLICATION,
          dpb_tpcx_bb_benchmark.GetNoncomplianceReasons(all_valid=True),
      )

  def testGetMapTasks(self):
    self.assertEqual(dpb_tpcx_bb_benchmark.GetMapTasks(self.workers), 48)
    with flagsaver.flagsaver(dpb_tpcx_bb_map_tasks=7):
      self.assertEqual(dpb_tpcx_bb_benchmark.GetMapTasks(self.workers), 7)

  # --- Prepare ------------------------------------------------------------

  @flagsaver.flagsaver(
      dpb_tpcx_bb_streams=2,
      dpb_tpcx_bb_spark_properties=['spark.sql.shuffle.partitions=10'],
  )
  def testPrepare(self):
    manager = mock.Mock()
    for name in (
        'InstallSpark',
        'ConfigureYarn',
        'InstallMetastore',
        'ConfigureSpark',
        'SmokeTestSpark',
        'ConfigureKit',
    ):
      manager.attach_mock(
          self.enter_context(mock.patch.object(tpcx_bb, name)), name
      )
    self.enter_context(
        mock.patch.object(tpcx_bb, 'GetKitDir', return_value=_KIT_DIR)
    )
    self.enter_context(
        mock.patch.object(
            dpb_tpcx_bb_benchmark.background_tasks,
            'RunThreaded',
            side_effect=lambda fn, items: [fn(i) for i in items],
        )
    )
    manager.attach_mock(self.leader.Install, 'Install')
    dpb_tpcx_bb_benchmark.Prepare(self.benchmark_spec)
    self.assertEqual(
        [c[0] for c in manager.mock_calls],
        [
            'InstallSpark',
            'InstallSpark',
            'InstallSpark',
            'InstallSpark',
            'ConfigureYarn',
            'InstallMetastore',
            'ConfigureSpark',
            'SmokeTestSpark',
            'Install',
            'ConfigureKit',
        ],
    )
    manager.ConfigureYarn.assert_called_once_with(self.leader, self.workers)
    self.leader.Install.assert_called_once_with('tpcx_bb')
    manager.ConfigureKit.assert_called_once_with(self.leader, _KIT_DIR)
    properties = manager.ConfigureSpark.call_args.args[1]
    self.assertEqual(properties['spark.sql.shuffle.partitions'], '10')
    # 128000 MB * 0.9 YARN memory per worker, 4 executors of 4 cores.
    self.assertEqual(properties['spark.executor.cores'], '4')
    self.assertEqual(self.benchmark_spec.tpcx_bb_kit_dir, _KIT_DIR)
    self.assertIs(self.benchmark_spec.tpcx_bb_spark_properties, properties)

  def testPrepareWithoutLeaderRaises(self):
    self.service.leader = None
    with self.assertRaisesRegex(
        errors.Benchmarks.PrepareException, 'leader VM'
    ):
      dpb_tpcx_bb_benchmark.Prepare(self.benchmark_spec)

  # --- Run ----------------------------------------------------------------

  def _SetUpRun(self, logs: list[str], exit_code: int = 0) -> None:
    self.leader.RobustRemoteCommand.return_value = (
        f'{tpcx_bb.DRIVER_RC_MARKER}{exit_code}\n',
        '',
    )
    logs_iter = iter(logs)

    def Respond(cmd, **_):
      if cmd.startswith('cat ') and 'driver_' in cmd:
        return (next(logs_iter), '')
      if cmd.startswith('cat ') and 'times.csv' in cmd:
        return (_TIMES_CSV, '')
      return ('', '')

    self.leader.RemoteCommand.side_effect = Respond

  def _Samples(self, samples, metric):
    return [s for s in samples if s.metric == metric]

  @flagsaver.flagsaver(
      dpb_tpcx_bb_scale_factor=100,
      dpb_tpcx_bb_streams=2,
      dpb_tpcx_bb_iterations=2,
      dpb_tpcx_bb_run_timeout=3600,
  )
  def testRun(self):
    self._SetUpRun([_DriverLog(500.0), _DriverLog(600.0)])
    clock = self.enter_context(
        mock.patch.object(dpb_tpcx_bb_benchmark, 'time', autospec=True)
    )
    clock.time.side_effect = [1000.0, 3500.0, 4000.0, 6750.0]
    with self.assertNoLogs(level='WARNING'):
      samples = dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)

    wall_times = self._Samples(samples, 'tpcx_bb_driver_wall_time')
    self.assertEqual([s.value for s in wall_times], [2500.0, 2750.0])
    self.assertEqual(wall_times[0].unit, 'seconds')
    self.assertEqual(wall_times[1].metadata['iteration'], 2)
    self.assertEqual(self.leader.RobustRemoteCommand.call_count, 2)
    command = self.leader.RobustRemoteCommand.call_args_list[0].args[0]
    self.assertIn('runBenchmark -f 100 -s 2 -m 48 -i', command)
    self.assertIn('timeout --kill-after=60 3600', command)
    self.assertEqual(
        self.leader.RobustRemoteCommand.call_args_list[0].kwargs,
        {'timeout': 4200},
    )
    self.assertEqual(
        [s.value for s in self._Samples(samples, 'tpcx_bb_bbqpm')],
        [_Bbqpm(500.0), _Bbqpm(600.0)],
    )
    reported = self._Samples(samples, 'tpcx_bb_bbqpm_reported')
    self.assertLen(reported, 1)
    self.assertEqual(reported[0].value, _Bbqpm(600.0))
    self.assertEqual(reported[0].unit, 'BBQpm@SF')
    t_tt = self._Samples(samples, 'tpcx_bb_t_tt')
    self.assertEqual([s.value for s in t_tt], [500.0, 600.0])
    self.assertEqual(t_tt[1].metadata['iteration'], 2)
    self.assertTrue(t_tt[1].metadata['tpcx_bb_result_valid'])
    phase = self._Samples(samples, 'tpcx_bb_phase_time')[0]
    self.assertEqual(phase.value, 60.5)
    self.assertEqual(
        phase.metadata['tpcx_bb_phase'], 'preparation: Data generation'
    )
    queries = self._Samples(samples, 'tpcx_bb_query_time')
    self.assertLen(queries, 4)
    self.assertEqual(queries[1].metadata['tpcx_bb_stream'], 1)
    self.assertFalse(queries[1].metadata['tpcx_bb_query_successful'])
    self.assertEqual(
        [s.value for s in self._Samples(samples, 'tpcx_bb_failed_queries')],
        [1, 1],
    )
    for s in samples:
      self.assertFalse(s.metadata['tpcx_bb_compliant'])
      self.assertEqual(
          s.metadata['tpcx_bb_noncompliance_reasons'],
          'kit_modified_ml_jar_spark_3.1,engine_validation_not_run,'
          'scale_factor_not_in_spec',
      )
      self.assertEqual(s.metadata['tpcx_bb_scale_factor'], 100)
      self.assertEqual(s.metadata['tpcx_bb_map_tasks'], 48)
      self.assertEqual(
          s.metadata['tpcx_bb_spark_properties'], 'spark.master=yarn'
      )
    self.assertIn(
        'tpcx_bb_driver_2.log',
        os.listdir(dpb_tpcx_bb_benchmark.vm_util.GetTempDir()),
    )
    commands = [c.args[0] for c in self.leader.RemoteCommand.call_args_list]
    self.assertTrue(any('application -kill' in c for c in commands))
    self.assertIn(
        f'tar -czf /opt/pkb/tpcx_bb/tpcx_bb_kit_logs_1.tgz -C {_KIT_DIR} logs',
        commands,
    )
    self.leader.PullFile.assert_called_with(
        dpb_tpcx_bb_benchmark.vm_util.GetTempDir(),
        '/opt/pkb/tpcx_bb/tpcx_bb_kit_logs_2.tgz',
    )

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1)
  def testRunKitLogFailureIsNotFatal(self):
    self._SetUpRun([_DriverLog()])
    self.leader.PullFile.side_effect = errors.VirtualMachine.RemoteCommandError(
        'scp failed'
    )
    samples = dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)
    self.assertLen(self._Samples(samples, 'tpcx_bb_bbqpm_reported'), 1)

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=2)
  def testRunInvalidResultIsNotReported(self):
    self._SetUpRun([_DriverLog(500.0), _DriverLog(600.0, 'INVALID')])
    samples = dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)
    self.assertEmpty(self._Samples(samples, 'tpcx_bb_bbqpm_reported'))
    bbqpm = self._Samples(samples, 'tpcx_bb_bbqpm')
    self.assertEqual(
        [s.metadata['tpcx_bb_result_valid'] for s in bbqpm], [True, False]
    )
    self.assertIn(
        'invalid_result', bbqpm[0].metadata['tpcx_bb_noncompliance_reasons']
    )

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1)
  def testRunWithoutResultRaises(self):
    self._SetUpRun(['NOK: Some required BigBench phases were skipped.\n'])
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError, 'phases were skipped'
    ):
      dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1)
  def testRunWithoutExitCodeMarkerAndResultRaises(self):
    self._SetUpRun([''])
    self.leader.RobustRemoteCommand.return_value = ('', '')
    with self.assertRaisesRegex(errors.Benchmarks.RunError, 'exit code None'):
      dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1, dpb_tpcx_bb_scale_factor=100)
  def testRunWarnsOnInconsistentResult(self):
    self._SetUpRun([_DriverLog(bbqpm=1.0, scale_factor=10)])
    with self.assertLogs(level='WARNING') as logs:
      samples = dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)
    output = '\n'.join(logs.output)
    self.assertIn('BBQpm@10 but scale factor 100', output)
    self.assertIn('differs from the one computed', output)
    self.assertEqual(self._Samples(samples, 'tpcx_bb_bbqpm')[0].value, 1.0)

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1)
  def testRunWithoutTimesCsvSkipsQuerySamples(self):
    self._SetUpRun([_DriverLog()])
    respond = self.leader.RemoteCommand.side_effect
    self.leader.RemoteCommand.side_effect = lambda cmd, **kw: (
        ('', '') if 'times.csv' in cmd else respond(cmd, **kw)
    )
    samples = dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)
    self.assertEmpty(self._Samples(samples, 'tpcx_bb_query_time'))
    self.assertEmpty(self._Samples(samples, 'tpcx_bb_failed_queries'))
    self.assertLen(self._Samples(samples, 'tpcx_bb_bbqpm_reported'), 1)

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1)
  def testRunTimeoutKillsYarnAppsAndRaises(self):
    self._SetUpRun([''], exit_code=124)
    with self.assertRaisesRegex(errors.Benchmarks.RunError, 'timed out'):
      dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)
    commands = [c.args[0] for c in self.leader.RemoteCommand.call_args_list]
    self.assertTrue(any('application -kill' in c for c in commands))

  @flagsaver.flagsaver(dpb_tpcx_bb_iterations=1)
  def testRunReportsClusterCreateTime(self):
    self.service.resource_ready_time = 2.0
    self.service.create_start_time = 1.0
    self.service.GetClusterCreateTime.return_value = 300.0
    self._SetUpRun([_DriverLog()])
    samples = dpb_tpcx_bb_benchmark.Run(self.benchmark_spec)
    self.assertEqual(self._Samples(samples, 'Time to Create')[0].value, 300.0)

  def testCleanup(self):
    dpb_tpcx_bb_benchmark.Cleanup(self.benchmark_spec)


if __name__ == '__main__':
  unittest.main()
