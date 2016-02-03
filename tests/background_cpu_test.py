# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for background cpu workload """

import unittest

from mock import patch

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from tests import mock_flags


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
      vm_2:
        vm_spec:
          GCP:
            background_cpu_threads: 3
            machine_type: n1-standard-1
"""


class TestBackgroundWorkload(unittest.TestCase):

  def setUp(self):
    self._mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self._mocked_flags.cloud = providers.GCP
    self._mocked_flags.os_type = os_types.DEBIAN
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CreateBenchmarkSpec(self, benchmark_config_yaml):
    config = configs.LoadConfig(benchmark_config_yaml, {}, NAME)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=self._mocked_flags, **config)
    return benchmark_spec.BenchmarkSpec(config_spec, NAME, UID)

  def _CheckVMFromSpec(self, spec, num_working):
    vm0 = spec.vms[0]
    with patch.object(
        vm0.__class__, 'RemoteCommand'), patch.object(
            vm0.__class__, 'Install'):

      expected_install_post_prepare = num_working
      expected_remote_post_start = num_working
      expected_remote_post_stop = 2 * num_working
      spec.Prepare()
      self.assertEqual(vm0.Install.call_count,
                       expected_install_post_prepare)
      spec.StartBackgroundWorkload()
      self.assertEqual(vm0.RemoteCommand.call_count,
                       expected_remote_post_start)
      spec.StopBackgroundWorkload()
      self.assertEqual(vm0.RemoteCommand.call_count,
                       expected_remote_post_stop)

  def testWindowsVMCausesError(self):
    """ windows vm with background_cpu_threads raises exception """
    self._mocked_flags['background_cpu_threads'].Parse(1)
    self._mocked_flags['os_type'].Parse(os_types.WINDOWS)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.Prepare()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.StartBackgroundWorkload()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.StopBackgroundWorkload()

  def testBackgroundWorkloadVM(self):
    """ Check that the background_cpu_threads causes calls """
    self._mocked_flags['background_cpu_threads'].Parse(1)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    self._CheckVMFromSpec(spec, 2)

  def testBackgroundWorkloadVanillaConfig(self):
    """ Test that nothing happens with the vanilla config """
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    for vm in spec.vms:
      self.assertIsNone(vm.background_cpu_threads)
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, 0)

  def testBackgroundWorkloadWindows(self):
    """ Test that nothing happens with the vanilla config """
    self._mocked_flags['os_type'].Parse(os_types.WINDOWS)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    for vm in spec.vms:
      self.assertIsNone(vm.background_cpu_threads)
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, 0)

  def testBackgroundWorkloadVanillaConfigFlag(self):
    """ Check that the background_cpu_threads flags overrides the config """
    self._mocked_flags['background_cpu_threads'].Parse(2)
    spec = self._CreateBenchmarkSpec(ping_benchmark.BENCHMARK_CONFIG)
    spec.ConstructVirtualMachines()
    for vm in spec.vms:
      self.assertEqual(vm.background_cpu_threads, 2)
    self._CheckVMFromSpec(spec, 2)

  def testBackgroundWorkloadConfig(self):
    """ Check that the config can be used to set background_cpu_threads """
    spec = self._CreateBenchmarkSpec(CONFIG_WITH_BACKGROUND_CPU)
    spec.ConstructVirtualMachines()
    for vm in spec.vm_groups['vm_1']:
      self.assertIsNone(vm.background_cpu_threads)
    for vm in spec.vm_groups['vm_2']:
      self.assertEqual(vm.background_cpu_threads, 3)
    self._CheckVMFromSpec(spec, 1)


if __name__ == '__main__':
  unittest.main()
