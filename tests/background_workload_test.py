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

"""Tests for background workload"""

import unittest
import mock
import functools
from mock import patch
from tests import mock_flags
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import timing_util
from perfkitbenchmarker import pkb
from perfkitbenchmarker import providers


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
    self.last_call = 0

  def _CheckAndIncrement(self, throwaway=None, expected_last_call=None):
    self.assertEqual(self.last_call, expected_last_call)
    self.last_call += 1

  def setupCommonFlags(self, mock_flags):
    mock_flags.os_type = benchmark_spec.DEBIAN
    mock_flags.cloud = providers.GCP

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


  def testBackgroundWorkloadSpec(self):
    """ Check that the benchmark spec calls the prepare, stop, and start
    methods on the vms """

    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      mocked_flags.background_cpu_threads = 1
      collector = mock.MagicMock()
      config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      vm0 = mock.MagicMock()
      vm1 = mock.MagicMock()
      spec.ConstructVirtualMachines()
      spec.vms = [vm0, vm1]
      timer = timing_util.IntervalTimer()
      pkb.DoPreparePhase(ping_benchmark, NAME, spec, timer)
      for vm in spec.vms:
        self.assertEqual(vm.PrepareBackgroundWorkload.call_count, 1)

      with mock.patch(ping_benchmark.__name__ + '.Run'):
        ping_benchmark.Run.side_effect = functools.partial(
            self._CheckAndIncrement, expected_last_call=1)
        vm0.StartBackgroundWorkload.side_effect = functools.partial(
            self._CheckAndIncrement, expected_last_call=0)
        vm0.StopBackgroundWorkload.side_effect = functools.partial(
            self._CheckAndIncrement, expected_last_call=2)
        pkb.DoRunPhase(ping_benchmark, NAME, spec, collector, timer)
        self.assertEqual(ping_benchmark.Run.call_count, 1)
        for vm in spec.vms:
          self.assertEqual(vm.StartBackgroundWorkload.call_count, 1)
          self.assertEqual(vm.StopBackgroundWorkload.call_count, 1)

  def testWindowsVMCausesError(self):
    """ windows vm with background_cpu_threads raises exception """
    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      mocked_flags.background_cpu_threads = 1
      mocked_flags.os_type = benchmark_spec.WINDOWS
      config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      spec.ConstructVirtualMachines()
      with self.assertRaises(Exception):
        spec.Prepare()
      with self.assertRaises(Exception):
        spec.StartBackgroundWorkload()
      with self.assertRaises(Exception):
        spec.StopBackgroundWorkload()

  def testBackgroundWorkloadVM(self):
    """ Check that the vm background workload calls work """
    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      mocked_flags.background_cpu_threads = 1
      config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      spec.ConstructVirtualMachines()
      self._CheckVMFromSpec(spec, 2)

  def testBackgroundWorkloadVanillaConfig(self):
    """ Test that nothing happens with the vanilla config """
    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      mocked_flags.background_cpu_threads = None
      config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      spec.ConstructVirtualMachines()

      for vm in spec.vms:
        self.assertIsNone(vm.background_cpu_threads)
      self._CheckVMFromSpec(spec, 0)

  def testBackgroundWorkloadWindows(self):
    """ Test that nothing happens with the vanilla config """
    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      mocked_flags.os_type = benchmark_spec.WINDOWS
      mocked_flags.background_cpu_threads = None
      config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      spec.ConstructVirtualMachines()

      for vm in spec.vms:
        self.assertIsNone(vm.background_cpu_threads)
      self._CheckVMFromSpec(spec, 0)


  def testBackgroundWorkloadVanillaConfigFlag(self):
    """ Check that the flag overrides the config """
    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      mocked_flags.background_cpu_threads = 2
      config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      spec.ConstructVirtualMachines()
      for vm in spec.vms:
        self.assertEqual(vm.background_cpu_threads, 2)
      self._CheckVMFromSpec(spec, 2)


  def testBackgroundWorkloadConfig(self):
    """ Check that the config can be used to set the background cpu threads """
    with mock_flags.PatchFlags() as mocked_flags:
      self.setupCommonFlags(mocked_flags)
      config = configs.LoadConfig(CONFIG_WITH_BACKGROUND_CPU, {}, NAME)
      spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
      spec.ConstructVirtualMachines()
      for vm in spec.vm_groups['vm_1']:
        self.assertIsNone(vm.background_cpu_threads)
      for vm in spec.vm_groups['vm_2']:
        self.assertEqual(vm.background_cpu_threads, 3)
      self._CheckVMFromSpec(spec, 1)

if __name__ == '__main__':
  unittest.main()
