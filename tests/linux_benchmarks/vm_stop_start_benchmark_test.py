"""Tests for vm_stop_start."""

import itertools
import time
import unittest
from absl.testing import parameterized
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import vm_stop_start_benchmark
from tests import pkb_common_test_case


def vm_mock(index: int, operation_return_value: int) -> mock.Mock:
  """Creates a mock vm and Adds the needed vm attributes to the mock vm.

  Args:
    index: an integer which specifies index of vm in the list of vms.
    operation_return_value: value to return when vm operation are called

  Returns:
    A mock vm.
  """
  mock_vm = mock.Mock(
      OS_TYPE=f'linux{index}')
  mock_vm.Start.return_value = operation_return_value
  mock_vm.Stop.return_value = operation_return_value
  return mock_vm


class StopStartBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                             test_util.SamplesTestMixin):

  def setUp(self):
    super(StopStartBenchmarkTest, self).setUp()
    self.enter_context(mock.patch.object(
        time,
        'time',
        # one pair of timestamps used for Measure and each sample.append call
        side_effect=itertools.cycle([0, 7])))

  # operation should be capitalized
  @parameterized.named_parameters(
      dict(testcase_name='measure_stop', operation='Stop'),
      dict(testcase_name='measure_start', operation='Start'))
  def testMeasureOperation(self, operation: str):
    """Unit test for Stop and Start functions."""
    num_vms = 3

    # create mock vms
    vms_to_test = [vm_mock(0, 5), vm_mock(1, 6), vm_mock(2, 7)]

    # call Stop on vms
    actual_samples = getattr(vm_stop_start_benchmark,
                             f'_Measure{operation}')(vms_to_test)

    # for all vms create mock samples ie the expected samples
    expected_operation_times = [5, 6, 7]
    expected_samples = []

    expected_cluster_metadata = {
        'num_vms': num_vms,
        'os_type': 'linux0,linux1,linux2',
    }

    for i in range(num_vms):
      expected_samples.append(
          sample.Sample(f'{operation} Time', expected_operation_times[i],
                        'seconds', {
                            'machine_instance': i,
                            'num_vms': num_vms,
                            'os_type': f'linux{i}',
                        }))

    expected_cluster_time = 7
    expected_samples.append(
        sample.Sample(f'Cluster {operation} Time', expected_cluster_time,
                      'seconds', expected_cluster_metadata))

    # assert the function is called on vms
    for vm in vms_to_test:
      getattr(vm, f'{operation}').assert_called()

    # assert actual and expected samples are equal
    self.assertSampleListsEqualUpToTimestamp(actual_samples, expected_samples)

if __name__ == '__main__':
  unittest.main()
