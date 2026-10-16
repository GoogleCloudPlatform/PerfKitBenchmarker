import os
import unittest
from unittest import mock
import zipfile

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker.linux_benchmarks import dpb_tpcx_hs_benchmark
from perfkitbenchmarker.linux_packages import tpcx_hs
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_JAR = '/opt/pkb/tpcx_hs/TPCx-HS-master_MR2.jar'
_FS_SHELL = 'org.apache.hadoop.fs.FsShell'


def _MakeConfig(service_type: str) -> mock.Mock:
  config = mock.Mock()
  config.dpb_service.service_type = service_type
  return config


def _DeleteCall(path: str):
  return mock.call(
      classname=_FS_SHELL,
      job_arguments=['-rm', '-r', '-f', '-skipTrash', path],
      job_type=dpb_constants.HADOOP_JOB_TYPE,
  )


def _StageCall(job_arguments: list[str]):
  return mock.call(
      jarfile=_JAR,
      job_arguments=job_arguments,
      job_type=dpb_constants.HADOOP_JOB_TYPE,
  )


class DpbTpcxHsBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'fakeru'
    self.service = self._MakeService(
        dpb_service.UnmanagedDpbServiceYarnCluster,
        dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
    )
    self.service.vms = {'worker_group': []}
    self.service.leader = mock.Mock()
    self.benchmark_spec = mock.Mock()
    self.benchmark_spec.dpb_service = self.service
    self.benchmark_spec.tpcx_hs_jar = _JAR

  def _MakeService(self, cls, service_type: str) -> mock.Mock:
    service = mock.Mock(spec=cls)
    service.dpb_service_type = service_type
    service.base_dir = 'gs://pkb-fakeru'
    service.storage_service = mock.Mock()
    service.metadata = {}
    service.GetResourceMetadata.return_value = {'dpb_service': service_type}
    service.resource_ready_time = None
    service.create_start_time = None
    service.SubmitJob.return_value = dpb_service.JobResult(run_time=600)
    return service

  def _UseManagedService(self) -> None:
    self.service = self._MakeService(
        dpb_service.BaseDpbService, dpb_constants.DATAPROC
    )
    self.benchmark_spec.dpb_service = self.service

  def _SetWorkers(self, count: int, vcpus: int) -> None:
    workers = []
    for _ in range(count):
      vm = mock.Mock()
      vm.NumCpusForBenchmark.return_value = vcpus
      workers.append(vm)
    self.service.vms = {'worker_group': workers}

  def _WriteKitZip(self, names: list[str]) -> str:
    """Writes a fake kit zip with the given member names; returns its path."""
    kit_path = os.path.join(self.create_tempdir().full_path, 'kit.zip')
    with zipfile.ZipFile(kit_path, 'w') as kit:
      for name in names:
        kit.writestr(name, b'jar bytes')
    return kit_path

  # --- CheckPrerequisites -------------------------------------------------

  def testCheckPrerequisitesDefaultsOnUnmanagedYarn(self):
    dpb_tpcx_hs_benchmark.CheckPrerequisites(
        _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
    )

  def testCheckPrerequisitesRejectsUnsupportedBackend(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'Invalid backend'):
      dpb_tpcx_hs_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.UNMANAGED_SPARK_CLUSTER)
      )

  def testInvalidScaleFactorFlagRaises(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_hs_scale_factor = 5

  @flagsaver.flagsaver(dpb_tpcx_hs_num_records=10)
  def testCheckPrerequisitesNumRecordsValid(self):
    dpb_tpcx_hs_benchmark.CheckPrerequisites(
        _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
    )

  def testNumRecordsFlagRejectsNonPositive(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.dpb_tpcx_hs_num_records = 0

  @flagsaver.flagsaver(dpb_tpcx_hs_jar_uri='gs://kits/TPCx-HS-master_MR2.jar')
  def testCheckPrerequisitesRejectsJarUriOnUnmanaged(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'jar_uri'):
      dpb_tpcx_hs_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER)
      )

  @flagsaver.flagsaver(dpb_tpcx_hs_jar_uri='gs://kits/TPCx-HS-master_MR2.jar')
  def testCheckPrerequisitesManagedWithJarUri(self):
    dpb_tpcx_hs_benchmark.CheckPrerequisites(
        _MakeConfig(dpb_constants.DATAPROC)
    )

  @mock.patch.object(
      dpb_tpcx_hs_benchmark.data, 'ResourceExists', return_value=False
  )
  def testCheckPrerequisitesManagedRequiresLocalKit(self, _):
    with self.assertRaisesRegex(errors.Config.InvalidValue, 'not found'):
      dpb_tpcx_hs_benchmark.CheckPrerequisites(
          _MakeConfig(dpb_constants.EMR)
      )

  @mock.patch.object(
      dpb_tpcx_hs_benchmark.data, 'ResourceExists', return_value=True
  )
  def testCheckPrerequisitesManagedWithLocalKit(self, resource_exists):
    dpb_tpcx_hs_benchmark.CheckPrerequisites(
        _MakeConfig(dpb_constants.DATAPROC)
    )
    resource_exists.assert_called_once_with('TPCX-HS-Tool.zip')

  # --- Helpers ------------------------------------------------------------

  def testGetNumRecordsFromScaleFactor(self):
    with flagsaver.flagsaver(dpb_tpcx_hs_scale_factor=3):
      self.assertEqual(dpb_tpcx_hs_benchmark.GetNumRecords(), 3 * 10**10)
      self.assertEqual(dpb_tpcx_hs_benchmark.GetScaleFactor(), 3)
      self.assertTrue(dpb_tpcx_hs_benchmark.IsCompliantRun())

  @flagsaver.flagsaver(dpb_tpcx_hs_num_records=10**9)
  def testGetNumRecordsOverride(self):
    self.assertEqual(dpb_tpcx_hs_benchmark.GetNumRecords(), 10**9)
    self.assertAlmostEqual(dpb_tpcx_hs_benchmark.GetScaleFactor(), 0.1)
    self.assertFalse(dpb_tpcx_hs_benchmark.IsCompliantRun())

  @flagsaver.flagsaver(dpb_tpcx_hs_iterations=1)
  def testSingleIterationIsNotCompliant(self):
    self.assertFalse(dpb_tpcx_hs_benchmark.IsCompliantRun())

  @flagsaver.flagsaver(dpb_tpcx_hs_iterations=3)
  def testMoreThanTwoIterationsIsNotCompliant(self):
    self.assertFalse(dpb_tpcx_hs_benchmark.IsCompliantRun())

  @flagsaver.flagsaver(dpb_tpcx_hs_storage_type='persistent')
  def testPersistentStorageIsNotCompliant(self):
    self.assertFalse(dpb_tpcx_hs_benchmark.IsCompliantRun())

  # --- Prepare ------------------------------------------------------------

  def testPrepareUnmanagedInstallsKitOnLeader(self):
    leader = self.service.leader
    with mock.patch.object(
        tpcx_hs, 'GetJarPath', return_value='/opt/pkb/tpcx_hs/x.jar'
    ) as get_jar:
      dpb_tpcx_hs_benchmark.Prepare(self.benchmark_spec)
    leader.Install.assert_called_once_with('tpcx_hs')
    get_jar.assert_called_once_with(leader)
    self.assertEqual(self.benchmark_spec.tpcx_hs_jar, '/opt/pkb/tpcx_hs/x.jar')

  @flagsaver.flagsaver(dpb_tpcx_hs_jar_uri='gs://kits/TPCx-HS-master_MR2.jar')
  def testPrepareManagedUsesJarUri(self):
    self._UseManagedService()
    dpb_tpcx_hs_benchmark.Prepare(self.benchmark_spec)
    self.assertEqual(
        self.benchmark_spec.tpcx_hs_jar, 'gs://kits/TPCx-HS-master_MR2.jar'
    )
    self.service.storage_service.Copy.assert_not_called()

  def testPrepareManagedStagesJarFromLocalKit(self):
    self._UseManagedService()
    kit_path = self._WriteKitZip([
        'TPCx-HS_Kit/TPCx-HS-Runtime-Suite/TPCx-HS-master.sh',
        'TPCx-HS_Kit/TPCx-HS-Runtime-Suite/TPCx-HS-master_Spark.jar',
        'TPCx-HS_Kit/TPCx-HS-Runtime-Suite/TPCx-HS-master_MR2.jar',
    ])
    run_dir = self.create_tempdir().full_path
    self.enter_context(
        mock.patch.object(
            dpb_tpcx_hs_benchmark.data, 'ResourcePath', return_value=kit_path
        )
    )
    self.enter_context(
        mock.patch.object(temp_dir, 'GetRunDirPath', return_value=run_dir)
    )

    dpb_tpcx_hs_benchmark.Prepare(self.benchmark_spec)

    expected_local = os.path.join(
        run_dir,
        'tpcx_hs/TPCx-HS_Kit/TPCx-HS-Runtime-Suite/TPCx-HS-master_MR2.jar',
    )
    self.assertTrue(os.path.exists(expected_local))
    self.service.storage_service.Copy.assert_called_once_with(
        expected_local, 'gs://pkb-fakeru/TPCx-HS-master_MR2.jar'
    )
    self.assertEqual(
        self.benchmark_spec.tpcx_hs_jar,
        'gs://pkb-fakeru/TPCx-HS-master_MR2.jar',
    )

  def testPrepareManagedKitWithoutJarRaises(self):
    self._UseManagedService()
    kit_path = self._WriteKitZip(['TPCx-HS_Kit/README.txt'])
    self.enter_context(
        mock.patch.object(
            dpb_tpcx_hs_benchmark.data, 'ResourcePath', return_value=kit_path
        )
    )
    self.enter_context(
        mock.patch.object(
            temp_dir,
            'GetRunDirPath',
            return_value=self.create_tempdir().full_path,
        )
    )
    with self.assertRaisesRegex(
        errors.Benchmarks.PrepareException, 'No jar matching'
    ):
      dpb_tpcx_hs_benchmark.Prepare(self.benchmark_spec)
    self.service.storage_service.Copy.assert_not_called()

  def testPrepareManagedWithoutStorageServiceRaises(self):
    self._UseManagedService()
    self.service.storage_service = None
    with self.assertRaisesRegex(
        errors.Benchmarks.PrepareException, 'no object storage'
    ):
      dpb_tpcx_hs_benchmark.Prepare(self.benchmark_spec)

  # --- Job properties -----------------------------------------------------

  def testJobPropertiesSizedFromWorkerVcpus(self):
    # 3 x 16 vCPU workers: 8 maps and 2 reducers per vCPU.
    self._SetWorkers(3, 16)
    total_vcpus = dpb_tpcx_hs_benchmark._GetTotalWorkerVcpus(self.service)
    self.assertEqual(total_vcpus, 48)
    self.assertEqual(
        dpb_tpcx_hs_benchmark.GetJobProperties(total_vcpus),
        {'mapreduce.job.maps': '384', 'mapreduce.job.reduces': '96'},
    )

  def testTotalWorkerVcpusUnknownForManagedService(self):
    self._UseManagedService()
    self.assertIsNone(
        dpb_tpcx_hs_benchmark._GetTotalWorkerVcpus(self.service)
    )

  def testJobPropertiesUnknownVcpusUsesClusterDefaultsAndWarns(self):
    with self.assertLogs(level='WARNING') as logs:
      self.assertEqual(dpb_tpcx_hs_benchmark.GetJobProperties(None), {})
    self.assertIn('--dpb_tpcx_hs_num_maps', logs.output[0])

  @flagsaver.flagsaver(
      dpb_tpcx_hs_num_maps=128, dpb_tpcx_hs_reducers_per_vcore=0
  )
  def testJobPropertiesExplicitMapsAndNoReducerOverride(self):
    self.assertEqual(
        dpb_tpcx_hs_benchmark.GetJobProperties(48),
        {'mapreduce.job.maps': '128'},
    )

  @flagsaver.flagsaver(dpb_tpcx_hs_num_maps=128, dpb_tpcx_hs_num_reducers=32)
  def testJobPropertiesExplicitWithoutWorkers(self):
    self.assertEqual(
        dpb_tpcx_hs_benchmark.GetJobProperties(None),
        {'mapreduce.job.maps': '128', 'mapreduce.job.reduces': '32'},
    )

  # --- RunStage -----------------------------------------------------------

  def testRunStage(self):
    dpb_tpcx_hs_benchmark.RunStage(
        self.benchmark_spec,
        tpcx_hs.HSGEN,
        ['100', 'gs://b/in'],
        {'mapreduce.job.maps': '384', 'mapreduce.job.reduces': '96'},
    )
    # Same shape as the kit driver: program name, -D sizing, program args.
    self.service.SubmitJob.assert_called_once_with(
        jarfile=_JAR,
        job_arguments=[
            'HSGen',
            '-Dmapreduce.job.maps=384',
            '-Dmapreduce.job.reduces=96',
            '100',
            'gs://b/in',
        ],
        job_type=dpb_constants.HADOOP_JOB_TYPE,
    )

  def testRunStageWithoutSizing(self):
    dpb_tpcx_hs_benchmark.RunStage(
        self.benchmark_spec, tpcx_hs.HSGEN, ['100', 'gs://b/in'], {}
    )
    self.service.SubmitJob.assert_called_once_with(
        jarfile=_JAR,
        job_arguments=['HSGen', '100', 'gs://b/in'],
        job_type=dpb_constants.HADOOP_JOB_TYPE,
    )

  # --- Run ----------------------------------------------------------------

  @flagsaver.flagsaver(
      dpb_tpcx_hs_scale_factor=1,
      dpb_tpcx_hs_iterations=2,
  )
  def testRunProducesSamples(self):
    self._SetWorkers(3, 16)
    # Make the second iteration slower so the reported metric is the min.
    fast = dpb_service.JobResult(run_time=600)
    slow = dpb_service.JobResult(run_time=1200)
    delete = dpb_service.JobResult(run_time=1)
    self.service.SubmitJob.side_effect = [delete] + [fast] * 3 + [delete] + [
        slow
    ] * 3

    samples = dpb_tpcx_hs_benchmark.Run(self.benchmark_spec)

    by_name = {}
    for s in samples:
      by_name.setdefault(s.metric, []).append(s)

    self.assertLen(by_name['hsgen_run_time'], 2)
    self.assertLen(by_name['hssort_run_time'], 2)
    self.assertLen(by_name['hsvalidate_run_time'], 2)
    self.assertLen(by_name['hsgen_wall_time'], 2)

    totals = by_name['tpcx_hs_total_run_time']
    self.assertEqual([s.value for s in totals], [1800, 3600])
    self.assertEqual([s.metadata['iteration'] for s in totals], [1, 2])

    hsph = by_name['tpcx_hs_hsph']
    self.assertAlmostEqual(hsph[0].value, 2.0)  # 1 TB in 0.5 h
    self.assertAlmostEqual(hsph[1].value, 1.0)  # 1 TB in 1 h
    self.assertEqual(hsph[0].unit, 'HSph@SF')

    reported = by_name['tpcx_hs_hsph_reported']
    self.assertLen(reported, 1)
    self.assertAlmostEqual(reported[0].value, 1.0)
    self.assertTrue(reported[0].metadata['tpcx_hs_compliant'])
    self.assertEqual(reported[0].metadata['tpcx_hs_scale_factor'], 1)
    self.assertEqual(reported[0].metadata['tpcx_hs_num_records'], 10**10)
    self.assertEqual(reported[0].metadata['tpcx_hs_dataset_size_gb'], 1000)
    self.assertEqual(
        reported[0].metadata['dpb_service'],
        dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
    )
    self.assertEqual(reported[0].metadata['tpcx_hs_num_maps'], '384')
    self.assertEqual(reported[0].metadata['tpcx_hs_num_reducers'], '96')
    self.assertEqual(reported[0].metadata['tpcx_hs_total_worker_vcpus'], 48)
    self.assertEqual(reported[0].metadata['base_dir'], '/tpcx_hs-fakeru')
    self.assertEqual(self.service.metadata, reported[0].metadata)

    # Every iteration deletes and reuses the kit's directory layout on HDFS.
    base = '/tpcx_hs-fakeru'
    sizing = ['-Dmapreduce.job.maps=384', '-Dmapreduce.job.reduces=96']
    iteration_calls = [
        _DeleteCall(base),
        _StageCall(
            ['HSGen'] + sizing + [str(10**10), f'{base}/HSsort-input']
        ),
        _StageCall(
            ['HSSort']
            + sizing
            + [f'{base}/HSsort-input', f'{base}/HSsort-output']
        ),
        # Like the kit driver, HSValidate gets no -D sizing.
        _StageCall(
            ['HSValidate', f'{base}/HSsort-output', f'{base}/HSValidate']
        ),
    ]
    self.assertEqual(
        self.service.SubmitJob.call_args_list, iteration_calls * 2
    )

  @flagsaver.flagsaver(dpb_tpcx_hs_iterations=1)
  def testRunHsphUsesWallTimeIncludingPendingTime(self):
    # 3 stages x (1000 s run + 200 s pending) = 1 h of wall time for 1 TB.
    self.service.SubmitJob.return_value = dpb_service.JobResult(
        run_time=1000, pending_time=200
    )
    samples = {
        s.metric: s for s in dpb_tpcx_hs_benchmark.Run(self.benchmark_spec)
    }
    self.assertEqual(samples['tpcx_hs_total_run_time'].value, 3000)
    self.assertEqual(samples['tpcx_hs_total_wall_time'].value, 3600)
    self.assertAlmostEqual(samples['tpcx_hs_hsph'].value, 1.0)
    self.assertAlmostEqual(samples['tpcx_hs_hsph_reported'].value, 1.0)

  @flagsaver.flagsaver(
      dpb_tpcx_hs_num_records=10**9,
      dpb_tpcx_hs_iterations=1,
      dpb_tpcx_hs_storage_type='persistent',
  )
  def testRunDevOverrideOnPersistentStorage(self):
    self._UseManagedService()
    self.service.SubmitJob.return_value = dpb_service.JobResult(run_time=1200)
    samples = dpb_tpcx_hs_benchmark.Run(self.benchmark_spec)
    reported = [s for s in samples if s.metric == 'tpcx_hs_hsph_reported'][0]
    # 0.1 TB in 1 h (3 x 1200 s) -> 0.1 HSph.
    self.assertAlmostEqual(reported.value, 0.1)
    self.assertFalse(reported.metadata['tpcx_hs_compliant'])
    self.assertAlmostEqual(reported.metadata['tpcx_hs_scale_factor'], 0.1)
    self.assertEqual(reported.metadata['tpcx_hs_dataset_size_gb'], 100)
    self.assertEqual(reported.metadata['base_dir'], 'gs://pkb-fakeru/tpcx_hs')
    # No worker VMs are known for managed services: cluster defaults.
    self.assertEqual(reported.metadata['tpcx_hs_num_maps'], 'default')
    self.assertEqual(reported.metadata['tpcx_hs_num_reducers'], 'default')
    self.assertIsNone(reported.metadata['tpcx_hs_total_worker_vcpus'])
    self.assertEqual(
        self.service.SubmitJob.call_args_list[:2],
        [
            _DeleteCall('gs://pkb-fakeru/tpcx_hs'),
            _StageCall([
                'HSGen',
                str(10**9),
                'gs://pkb-fakeru/tpcx_hs/HSsort-input',
            ]),
        ],
    )

  def testRunIncludesCreateTimeWhenAvailable(self):
    self.service.resource_ready_time = 100
    self.service.create_start_time = 10
    self.service.GetClusterCreateTime.return_value = 90
    with flagsaver.flagsaver(dpb_tpcx_hs_iterations=1):
      samples = dpb_tpcx_hs_benchmark.Run(self.benchmark_spec)
    create = [s for s in samples if s.metric == 'Time to Create']
    self.assertLen(create, 1)
    self.assertEqual(create[0].value, 90)


if __name__ == '__main__':
  unittest.main()
