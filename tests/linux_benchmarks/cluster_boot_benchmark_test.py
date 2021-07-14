"""Tests for cluster_boot_delete."""

import datetime
import unittest

import freezegun
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import cluster_boot_benchmark
from tests import pkb_common_test_case


def vm_mock(index: int, timestamp: float) -> mock.Mock:
  """Creates a mock vm and Adds the needed vm attributes to the mock vm.

  Args:
    index: an integer which specifies index of vm in the list of vms.
    timestamp: a timestamp which is used to determine delete start time and end
      time.

  Returns:
    A mock vm.
  """
  return mock.Mock(
      delete_start_time=timestamp,
      delete_end_time=timestamp + index + 5,
      OS_TYPE=f'linux{index}')


class ClusterBootBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                               test_util.SamplesTestMixin):

  def testMeasureDelete(self):
    """Unit test for Measuredelete function."""
    timestamp = 1625863325.003580
    num_vms = 3

    # create mock vms
    vms_to_test = [vm_mock(i, timestamp) for i in range(num_vms)]

    # call Delete on vms
    with freezegun.freeze_time(datetime.datetime.utcfromtimestamp(timestamp)):
      actual_samples = cluster_boot_benchmark.MeasureDelete(vms_to_test)

    # for all vms create mock samples ie the expected samples
    expected_delete_times = [5, 6, 7]
    expected_samples = []

    expected_cluster_delete_metadata = {
        'num_vms': 3,
        'os_type': 'linux0,linux1,linux2',
    }
    expected_cluster_delete_time = 7
    for i in range(num_vms):
      expected_samples.append(
          sample.Sample('Delete Time', expected_delete_times[i], 'seconds', {
              'machine_instance': i,
              'num_vms': num_vms,
              'os_type': f'linux{i}',
          }))

    expected_samples.append(
        sample.Sample('Cluster Delete Time', expected_cluster_delete_time,
                      'seconds', expected_cluster_delete_metadata))

    # assert the delete function is called on vms
    for vm in vms_to_test:
      vm.Delete.assert_called()

    # assert actual and expected samples are equal
    self.assertSampleListsEqualUpToTimestamp(actual_samples, expected_samples)


if __name__ == '__main__':
  unittest.main()
