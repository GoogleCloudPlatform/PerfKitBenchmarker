# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for background cpu workload."""


import contextlib
import itertools
import unittest

from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import os_types
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

NAME = 'ping'
UID = 'name0'

CONFIG_WITH_BACKGROUND_CPU = """
ping:
    description: Benchmarks ping latency over internal IP addresses
    vm_groups:
      vm_1:
        vm_spec:
          GCP:
            machine_type: n1-standard-1
            zone: us-central1-a
      vm_2:
        vm_spec:
          GCP:
            background_cpu_threads: 3
            machine_type: n1-standard-1
            zone: us-central1-a
"""

_GROUP_1 = 'vm_1'
_GROUP_2 = 'vm_2'
_MOCKED_VM_FUNCTIONS = 'Install', 'RemoteCommand'


class TestBackgroundWorkload(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'fake_run_uri'
    FLAGS.cloud = provider_info.GCP
    FLAGS.temp_dir = 'tmp'

    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CreateBenchmarkSpec(self, benchmark_config_yaml):
    config = configs.LoadConfig(benchmark_config_yaml, {}, NAME)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=FLAGS, **config
    )
    return benchmark_spec.BenchmarkSpec(ping_benchmark, config_spec, UID)

  def _CheckVmCallCounts(
      self,
      spec,
      working_groups,
      working_expected_counts,
      non_working_groups,
      non_working_expected_counts,
  ):
    # TODO(user): This is also used in TestBackgroundNetworkWorkload.
    # Consider moving to a shared function or base class.
    expected_call_counts = {
        group: working_expected_counts for group in working_groups
    }
    expected_call_counts.update(
        {group: non_working_expected_counts for group in non_working_groups}
    )
    for group_name, vm_expected_call_counts in (
        expected_call_counts
    ).items():
      group_vms = spec.vm_groups[group_name]
      self.assertEqual(
          len(group_vms),
          1,
          msg='VM group "{}" had {} VMs'.format(group_name, len(group_vms)),
      )
      vm = group_vms[0]
      iter_mocked_functions = itertools.zip_longest(
          _MOCKED_VM_FUNCTIONS, vm_expected_call_counts
      )
      for function_name, expected_call_count in iter_mocked_functions:
        call_count = getattr(vm, function_name).call_count
        self.assertEqual(
            call_count,
            expected_call_count,
            msg=(
                'Expected {} from VM group "{}" to be called {} times, but'
                ' it was called {} times.'.format(
                    function_name, group_name, expected_call_count, call_count
                )
            ),
        )

  def _CheckVMFromSpec(self, spec, working_groups=(), non_working_groups=()):
    with contextlib.ExitStack() as stack:
      for vm in spec.vms:
        for function_name in _MOCKED_VM_FUNCTIONS:
          stack.enter_context(mock.patch.object(vm, function_name))

      working, non_working = working_groups, non_working_groups
      self._CheckVmCallCounts(spec, working, (0, 0), non_working, (0, 0))

      spec.Prepare()
      self._CheckVmCallCounts(spec, working, (1, 0), non_working, (0, 0))

      spec.StartBackgroundWorkload()
      self._CheckVmCallCounts(spec, working, (1, 1), non_working, (0, 0))

      spec.StopBackgroundWorkload()
      self._CheckVmCallCounts(spec, working, (1, 2), non_working, (0, 0))

  def testWindowsVMCausesError(self):
    """windows vm with background_cpu_threads raises exception."""
    FLAGS['background_cpu_threads'].parse(1)
    FLAGS['os_type'].parse(os_types.WINDOWS2019_CORE)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    with self.assertRaisesRegex(Exception, 'NotImplementedError'):
      spec.Prepare()
    with self.assertRaisesRegex(Exception, 'NotImplementedError'):
      spec.StartBackgroundWorkload()
    with self.assertRaisesRegex(Exception, 'NotImplementedError'):
      spec.StopBackgroundWorkload()

  def testBackgroundWorkloadVM(self):
    """Check that the background_cpu_threads causes calls."""
    FLAGS['background_cpu_threads'].parse(1)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    self._CheckVMFromSpec(spec, working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadVanillaConfig(self):
    """Test that nothing happens with the vanilla config."""
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    for vm in spec.vms:
      self.assertIsNone(vm.background_cpu_threads)
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, non_working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadWindows(self):
    """Test that nothing happens with the vanilla config."""
    FLAGS['os_type'].parse(os_types.WINDOWS2019_CORE)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    for vm in spec.vms:
      self.assertIsNone(vm.background_cpu_threads)
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, non_working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadVanillaConfigFlag(self):
    """Check that the background_cpu_threads flags overrides the config."""
    FLAGS['background_cpu_threads'].parse(2)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    for vm in spec.vms:
      self.assertEqual(vm.background_cpu_threads, 2)
    self._CheckVMFromSpec(spec, working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadConfig(self):
    """Check that the config can be used to set background_cpu_threads."""
    spec = self._CreateBenchmarkSpec(CONFIG_WITH_BACKGROUND_CPU)
    spec.ConstructVirtualMachines()
    for vm in spec.vm_groups[_GROUP_1]:
      self.assertIsNone(vm.background_cpu_threads)
    for vm in spec.vm_groups[_GROUP_2]:
      self.assertEqual(vm.background_cpu_threads, 3)
    self._CheckVMFromSpec(
        spec, working_groups=[_GROUP_2], non_working_groups=[_GROUP_1]
    )


if __name__ == '__main__':
  unittest.main()
