"""Tests for suspend_resume_benchmark."""
import itertools
import time
import unittest
from absl.testing import parameterized

import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import suspend_resume_benchmark
from tests import pkb_common_test_case


def create_mock_vm(
    index: int,
    operation_value: float,
) -> mock.Mock:
  """Create mock VMs which will be used by the unit tests.

  Args:
    index: an integer which specifies index of vm in the list of vms.
    operation_value: a float which indicates the resume or suspend duration

  Returns:
    A mock VM.
  """
  vm = mock.Mock(OS_TYPE=f'linux{index}')
  vm.Suspend.return_value = operation_value
  vm.Resume.return_value = operation_value
  return vm


class SuspendResumeBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                                 test_util.SamplesTestMixin):

  def setUp(self):
    super(SuspendResumeBenchmarkTest, self).setUp()
    time.time()
    self.enter_context(
        mock.patch.object(time, 'time', side_effect=itertools.cycle([0, 52])))

  @parameterized.named_parameters(
      dict(testcase_name='get_time_to_suspend', operation='Suspend'),
      dict(testcase_name='get_time_to_resume', operation='Resume'))
  def testMeasureOperation(self, operation: str):
    """Unit test for Suspend and Resume functions."""
    num_vms = 3

    # Create mock vms
    vms_to_test = [create_mock_vm(i, 50.0 + i) for i in range(num_vms)]

    # Call suspend function on mock vms - generates actual samples for the test.
    actual_samples = getattr(suspend_resume_benchmark, f'GetTimeTo{operation}')(
        vms_to_test)

    # Create expected samples from mock vms.
    expected_times = [50, 51, 52]
    expected_samples = []

    expected_cluster_time = 52
    expected_cluster_metadata = {
        'num_vms': num_vms,
        'os_type': 'linux0,linux1,linux2',
    }

    for i in range(num_vms):
      expected_samples.append(
          sample.Sample(f'{operation} Time', expected_times[i], 'seconds', {
              'machine_instance': i,
              'num_vms': num_vms,
              'os_type': f'linux{i}',
          }))

    expected_samples.append(
        sample.Sample(f'Cluster {operation} Time', expected_cluster_time,
                      'seconds', expected_cluster_metadata))

    # Assert Suspend() function is called on vms.
    for vm in vms_to_test:
      getattr(vm, f'{operation}').assert_called()

    # Assert actual and expected samples are equal.
    self.assertSampleListsEqualUpToTimestamp(actual_samples,
                                             expected_samples)

if __name__ == '__main__':
  unittest.main()
