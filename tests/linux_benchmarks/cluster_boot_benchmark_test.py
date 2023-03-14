"""Tests for cluster_boot_delete."""

import datetime
import unittest

import freezegun
import mock
from perfkitbenchmarker import context
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import cluster_boot_benchmark
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
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

  @freezegun.freeze_time('2023-03-07')
  def testGetTimeToBoot(self):
    context.SetThreadBenchmarkSpec(
        pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    )

    vm_spec = gce_virtual_machine.GceVmSpec('cluster_boot_benchmark_test')
    vm = gce_virtual_machine.Ubuntu2204BasedGceVirtualMachine(vm_spec)
    vm.create_start_time = 1
    vm.create_return_time = 2
    vm.is_running_time = 3
    vm.ssh_internal_time = 4
    vm.ssh_external_time = 5
    vm.bootable_time = 7

    actuals = cluster_boot_benchmark.GetTimeToBoot([vm])

    metrics = {
        'Time to Create Async Return': 1.0,
        'Time to Running': 2.0,
        'Time to SSH - External': 4.0,
        'Time to SSH - Internal': 3.0,
        'Boot Time': 6.0,
    }
    expecteds = []
    for metric, value in metrics.items():
      expecteds.append(
          sample.Sample(
              metric=metric,
              value=value,
              unit='seconds',
              metadata={
                  'machine_instance': 0,
                  'num_vms': 1,
                  'os_type': 'ubuntu2204',
                  'create_delay_sec': '0.0',
              },
              timestamp=1678147200.0,
          )
      )
    expecteds.append(
        sample.Sample(
            metric='Cluster Boot Time',
            value=6.0,
            unit='seconds',
            metadata={
                'num_vms': 1,
                'os_type': 'ubuntu2204',
                'max_create_delay_sec': '0.0',
            },
            timestamp=1678147200.0,
        )
    )

    self.assertCountEqual(actuals, expecteds)


if __name__ == '__main__':
  unittest.main()
