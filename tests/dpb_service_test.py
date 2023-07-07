"""Tests for perfkitbenchmarker.dpb_service module."""

from typing import Any, Optional
import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import dpb_service
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


class MockDpbService(dpb_service.BaseDpbService):

  def __init__(
      self,
      dpb_service_spec: Any,
      cluster_create_time: Optional[float] = None,
  ):
    self.dpb_service_spec = None
    self._cluster_create_time = cluster_create_time
    self.metadata = {'foo': 42}

  def GetClusterCreateTime(self) -> Optional[float]:
    return self._cluster_create_time

  def SubmitJob(self, *args, **kwargs) -> dpb_service.JobResult:
    return dpb_service.JobResult(run_time=1)


class DpbServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri=TEST_RUN_URI))

  @parameterized.named_parameters(
      dict(
          testcase_name='NoClusterCreateTime',
          cluster_create_time=None,
          expected_datapoints=[],
          unexpected_metrics=['dpb_cluster_create_time'],
      ),
      dict(
          testcase_name='WithClusterCreateTime',
          cluster_create_time=42,
          expected_datapoints=[
              ('dpb_cluster_create_time', 42.0),
          ],
          unexpected_metrics=[],
      ),
  )
  def testGetSamples(
      self,
      cluster_create_time: float,
      expected_datapoints: list[tuple[str, float]],
      unexpected_metrics: list[str],
  ):
    mock_dpb_service = MockDpbService(
        CLUSTER_SPEC,
        cluster_create_time=cluster_create_time,
    )
    samples = mock_dpb_service.GetSamples()
    for s in samples:
      self.assertEqual(s.metadata, {'foo': 42})
    actual_datapoints = [(s.metric, s.value) for s in samples]
    actual_metrics = [s.metric for s in samples]
    self.assertContainsSubset(expected_datapoints, actual_datapoints)
    self.assertNoCommonElements(unexpected_metrics, actual_metrics)


if __name__ == '__main__':
  unittest.main()
