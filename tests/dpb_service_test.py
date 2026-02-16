"""Tests for perfkitbenchmarker.dpb_service module."""

import datetime
from typing import Any
import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
import freezegun
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from tests import pkb_common_test_case

CLUSTER_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    worker_count=2,
    version='fake-version',
    applications=['foo-component', 'bar-component'],
    worker_group=mock.Mock(
        vm_spec=mock.Mock(machine_type='fake-machine-type', num_local_ssds=2),
        disk_spec=mock.Mock(disk_type='pd-ssd', disk_size=42),
    ),
)
TEST_RUN_URI = 'fakeru'
FAKE_DATETIME_NOW = datetime.datetime(2010, 1, 1)
JOB_RUN_TIME = 1
JOB_STDOUT = 'stdout'
JOB_STDERR = 'stderr'


class MockDpbService(dpb_service.BaseDpbService):

  @property
  def persistent_fs_prefix(self) -> str | None:
    return 'gs://'

  def __init__(
      self,
      dpb_service_spec: Any,
      cluster_create_time: float | None = None,
      cluster_duration: float | None = None,
  ):
    super().__init__(dpb_service_spec)
    self._cluster_create_time = cluster_create_time
    self.cluster_duration = cluster_duration
    self.metadata = {'foo': 42}

  def GetClusterCreateTime(self) -> float | None:
    return self._cluster_create_time

  def SubmitJob(self, *args, **kwargs) -> dpb_service.JobResult:
    return dpb_service.JobResult(
        run_time=JOB_RUN_TIME, stdout=JOB_STDOUT, stderr=JOB_STDERR
    )


class NoDynallocSupportingMockDpbService(MockDpbService):
  SUPPORTS_NO_DYNALLOC = True


class DpbServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri=TEST_RUN_URI))
    self.enter_context(
        mock.patch.object(resource.BaseResource, '__init__', return_value=None)
    )
    self.dpb_service = MockDpbService(CLUSTER_SPEC, cluster_duration=1800)

  @parameterized.named_parameters(
      dict(testcase_name='FlagUnset', flag_value=None, hardware_cost=None),
      dict(testcase_name='FlagSet', flag_value=3.14, hardware_cost=1.57),
  )
  def testGetClusterHardwareCost(self, flag_value, hardware_cost):
    with flagsaver.flagsaver((dpb_service._HARDWARE_HOURLY_COST, flag_value)):
      self.assertEqual(self.dpb_service.GetClusterHardwareCost(), hardware_cost)

  @parameterized.named_parameters(
      dict(testcase_name='FlagUnset', flag_value=None, premium_cost=None),
      dict(testcase_name='FlagSet', flag_value=4.2, premium_cost=2.1),
  )
  def testGetClusterPremiumCost(self, flag_value, premium_cost):
    with flagsaver.flagsaver(
        (dpb_service._SERVICE_PREMIUM_HOURLY_COST, flag_value)
    ):
      self.assertEqual(self.dpb_service.GetClusterPremiumCost(), premium_cost)

  @parameterized.named_parameters(
      dict(
          testcase_name='BothCostFlagsUnset',
          hardware_hourly_cost=None,
          premium_hourly_cost=None,
          expected_cluster_cost=None,
      ),
      dict(
          testcase_name='HardwareCostFlagUnset',
          hardware_hourly_cost=10.0,
          premium_hourly_cost=None,
          expected_cluster_cost=None,
      ),
      dict(
          testcase_name='PremiumCostFlagUnset',
          hardware_hourly_cost=None,
          premium_hourly_cost=1.5,
          expected_cluster_cost=None,
      ),
      dict(
          testcase_name='BothCostFlagsSet',
          hardware_hourly_cost=8.1,
          premium_hourly_cost=1.9,
          expected_cluster_cost=5.0,
      ),
  )
  def testGetClusterCost(
      self, hardware_hourly_cost, premium_hourly_cost, expected_cluster_cost
  ):
    with flagsaver.flagsaver(
        (dpb_service._HARDWARE_HOURLY_COST, hardware_hourly_cost),
        (dpb_service._SERVICE_PREMIUM_HOURLY_COST, premium_hourly_cost),
    ):
      self.assertEqual(self.dpb_service.GetClusterCost(), expected_cluster_cost)

  @parameterized.named_parameters(
      dict(
          testcase_name='NoClusterCreateTime',
          cluster_create_time=None,
          cluster_duration=900,
          hardware_hourly_cost=6.0,
          premium_hourly_cost=4.0,
          expected_datapoints=[
              ('dpb_cluster_duration', 900.0),
              ('dpb_cluster_hardware_cost', 1.5),
              ('dpb_cluster_premium_cost', 1.0),
              ('dpb_cluster_total_cost', 2.5),
              ('dpb_cluster_hardware_hourly_cost', 6.0),
              ('dpb_cluster_premium_hourly_cost', 4.0),
          ],
          unexpected_metrics=[
              'dpb_cluster_create_time',
          ],
      ),
      dict(
          testcase_name='NoClusterDuration',
          cluster_create_time=42,
          cluster_duration=None,
          hardware_hourly_cost=6.0,
          premium_hourly_cost=4.0,
          expected_datapoints=[
              ('dpb_cluster_create_time', 42),
              ('dpb_cluster_hardware_hourly_cost', 6.0),
              ('dpb_cluster_premium_hourly_cost', 4.0),
          ],
          unexpected_metrics=[
              'dpb_cluster_duration',
              'dpb_cluster_hardware_cost',
              'dpb_cluster_premium_cost',
              'dpb_cluster_total_cost',
          ],
      ),
      dict(
          testcase_name='NoHardwareHourlyCost',
          cluster_create_time=42,
          cluster_duration=900,
          hardware_hourly_cost=None,
          premium_hourly_cost=4.0,
          expected_datapoints=[
              ('dpb_cluster_create_time', 42.0),
              ('dpb_cluster_duration', 900.0),
              ('dpb_cluster_premium_cost', 1.0),
              ('dpb_cluster_premium_hourly_cost', 4.0),
          ],
          unexpected_metrics=[
              'dpb_cluster_hardware_cost',
              'dpb_cluster_total_cost',
              'dpb_cluster_hardware_hourly_cost',
          ],
      ),
      dict(
          testcase_name='NoPremiumHourlyCost',
          cluster_create_time=42,
          cluster_duration=900,
          hardware_hourly_cost=6.0,
          premium_hourly_cost=None,
          expected_datapoints=[
              ('dpb_cluster_create_time', 42.0),
              ('dpb_cluster_duration', 900.0),
              ('dpb_cluster_hardware_cost', 1.5),
              ('dpb_cluster_hardware_hourly_cost', 6.0),
          ],
          unexpected_metrics=[
              'dpb_cluster_total_cost',
              'dpb_cluster_premium_cost',
              'dpb_cluster_premium_hourly_cost',
          ],
      ),
      dict(
          testcase_name='AllSet',
          cluster_create_time=42,
          cluster_duration=900,
          hardware_hourly_cost=6.0,
          premium_hourly_cost=4.0,
          expected_datapoints=[
              ('dpb_cluster_create_time', 42.0),
              ('dpb_cluster_duration', 900.0),
              ('dpb_cluster_hardware_cost', 1.5),
              ('dpb_cluster_premium_cost', 1.0),
              ('dpb_cluster_total_cost', 2.5),
              ('dpb_cluster_hardware_hourly_cost', 6.0),
              ('dpb_cluster_premium_hourly_cost', 4.0),
          ],
          unexpected_metrics=[],
      ),
  )
  def testGetSamples(
      self,
      cluster_create_time: float,
      cluster_duration: float,
      hardware_hourly_cost: float,
      premium_hourly_cost: float,
      expected_datapoints: list[tuple[str, float]],
      unexpected_metrics: list[str],
  ):
    self.enter_context(
        flagsaver.flagsaver(
            (dpb_service._HARDWARE_HOURLY_COST, hardware_hourly_cost),
            (dpb_service._SERVICE_PREMIUM_HOURLY_COST, premium_hourly_cost),
        ),
    )
    mock_dpb_service = MockDpbService(
        CLUSTER_SPEC,
        cluster_create_time=cluster_create_time,
        cluster_duration=cluster_duration,
    )
    samples = mock_dpb_service.GetSamples()
    for s in samples:
      self.assertEqual(s.metadata, {'foo': 42})
    actual_datapoints = [(s.metric, s.value) for s in samples]
    actual_metrics = [s.metric for s in samples]
    self.assertContainsSubset(expected_datapoints, actual_datapoints)
    self.assertNoCommonElements(unexpected_metrics, actual_metrics)

  @flagsaver.flagsaver((dpb_service._DYNAMIC_ALLOCATION, False))
  def testDynamicAllocationNotSupported(self):
    with self.assertRaises(errors.Setup.InvalidFlagConfigurationError):
      MockDpbService(CLUSTER_SPEC)

  @parameterized.named_parameters(
      dict(testcase_name='DynallocOn', dynalloc=True, expected=[]),
      dict(
          testcase_name='DynallocOff',
          dynalloc=False,
          expected=[
              'spark:spark.executor.instances=9999',
              'spark:spark.dynamicAllocation.enabled=false',
          ],
      ),
  )
  def testGetClusterProperties(self, dynalloc, expected):
    mock_dpb_service = NoDynallocSupportingMockDpbService(CLUSTER_SPEC)
    with flagsaver.flagsaver((dpb_service._DYNAMIC_ALLOCATION, dynalloc)):
      self.assertEqual(mock_dpb_service.GetClusterProperties(), expected)

  @parameterized.named_parameters(
      dict(
          testcase_name='Empty',
          job_costs=dpb_service.JobCosts(),
          get_sample_kwargs={},
          expected_samples=[],
      ),
      dict(
          testcase_name='ComputeOnlyWithMetadata',
          job_costs=dpb_service.JobCosts(
              total_cost=42,
              compute_units_used=21,
              compute_unit_cost=2,
              compute_unit_name='CU',
          ),
          get_sample_kwargs={'metadata': {'foo': 'bar'}},
          expected_samples=[
              sample.Sample(
                  metric='total_cost',
                  value=42.0,
                  unit='$',
                  metadata={'foo': 'bar'},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='compute_units_used',
                  value=21.0,
                  unit='CU',
                  metadata={'foo': 'bar'},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='compute_unit_cost',
                  value=2.0,
                  unit='$/(CU)',
                  metadata={'foo': 'bar'},
                  timestamp=1262304000.0,
              ),
          ],
      ),
      dict(
          testcase_name='ComputeOnlyPrefixAndRename',
          job_costs=dpb_service.JobCosts(
              total_cost=42,
              compute_units_used=21,
              compute_unit_cost=2,
              compute_unit_name='CU',
          ),
          get_sample_kwargs={
              'prefix': 'sparksql_',
              'renames': {'total_cost': 'run_cost'},
          },
          expected_samples=[
              sample.Sample(
                  metric='sparksql_run_cost',
                  value=42.0,
                  unit='$',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='sparksql_compute_units_used',
                  value=21.0,
                  unit='CU',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='sparksql_compute_unit_cost',
                  value=2.0,
                  unit='$/(CU)',
                  metadata={},
                  timestamp=1262304000.0,
              ),
          ],
      ),
      dict(
          testcase_name='AllMetrics',
          job_costs=dpb_service.JobCosts(
              total_cost=33,
              compute_units_used=22,
              compute_unit_cost=0.5,
              compute_unit_name='CU',
              memory_units_used=11,
              memory_unit_cost=1,
              memory_unit_name='MU',
              storage_units_used=5.5,
              storage_unit_cost=2,
              storage_unit_name='SU',
          ),
          get_sample_kwargs={},
          expected_samples=[
              sample.Sample(
                  metric='total_cost',
                  value=33.0,
                  unit='$',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='compute_units_used',
                  value=22.0,
                  unit='CU',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='memory_units_used',
                  value=11.0,
                  unit='MU',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='storage_units_used',
                  value=5.5,
                  unit='SU',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='compute_unit_cost',
                  value=0.5,
                  unit='$/(CU)',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='memory_unit_cost',
                  value=1.0,
                  unit='$/(MU)',
                  metadata={},
                  timestamp=1262304000.0,
              ),
              sample.Sample(
                  metric='storage_unit_cost',
                  value=2.0,
                  unit='$/(SU)',
                  metadata={},
                  timestamp=1262304000.0,
              ),
          ],
      ),
  )
  @freezegun.freeze_time(FAKE_DATETIME_NOW)
  def testJobCostsGetSamples(
      self, job_costs, get_sample_kwargs, expected_samples
  ):
    actual_samples = job_costs.GetSamples(**get_sample_kwargs)
    self.assertListEqual(actual_samples, expected_samples)

  def testSubmitJob(self):
    mock_dpb_service = MockDpbService(CLUSTER_SPEC)
    mock_job = mock.Mock()
    job_result = mock_dpb_service.SubmitJob(mock_job)
    self.assertEqual(job_result.run_time, JOB_RUN_TIME)
    self.assertEqual(job_result.stdout, JOB_STDOUT)
    self.assertEqual(job_result.stderr, JOB_STDERR)


if __name__ == '__main__':
  unittest.main()
