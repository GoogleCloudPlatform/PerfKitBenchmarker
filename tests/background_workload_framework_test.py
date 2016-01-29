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

"""Tests for background workload framework"""

import functools
import unittest

import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import os_types
from perfkitbenchmarker import pkb
from perfkitbenchmarker import providers
from perfkitbenchmarker import timing_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from tests import mock_flags


NAME = 'ping'
UID = 'name0'


class TestBackgroundWorkloadFramework(unittest.TestCase):

  def setUp(self):
    self.last_call = 0
    super(TestBackgroundWorkloadFramework, self).setUp()
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.os_type = os_types.DEBIAN
    self.mocked_flags.cloud = providers.GCP
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CheckAndIncrement(self, throwaway=None, expected_last_call=None):
    self.assertEqual(self.last_call, expected_last_call)
    self.last_call += 1

  def testBackgroundWorkloadSpec(self):
    """ Check that the benchmark spec calls the prepare, stop, and start
    methods on the vms """

    collector = mock.MagicMock()
    config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=self.mocked_flags, **config)
    spec = benchmark_spec.BenchmarkSpec(config_spec, NAME, UID)
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
        self.assertEqual(vm.PrepareBackgroundWorkload.call_count, 1)


if __name__ == '__main__':
  unittest.main()
