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

"""Tests for background workload framework."""

import functools
import unittest
from absl import flags
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import pkb
from perfkitbenchmarker import providers
from perfkitbenchmarker import timing_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

NAME = 'ping'
UID = 'name0'


class TestBackgroundWorkloadFramework(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    self.last_call = 0
    super(TestBackgroundWorkloadFramework, self).setUp()
    FLAGS.cloud = providers.GCP
    FLAGS.temp_dir = 'tmp'
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CheckAndIncrement(self, throwaway=None, expected_last_call=None):
    self.assertEqual(self.last_call, expected_last_call)
    self.last_call += 1

  def testBackgroundWorkloadSpec(self):
    """Check that benchmark spec calls the prepare, stop, and start on vm."""

    config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=FLAGS, **config)
    spec = benchmark_spec.BenchmarkSpec(ping_benchmark, config_spec, UID)
    vm0 = mock.MagicMock()
    vm1 = mock.MagicMock()
    vm0.IsInterruptible = mock.MagicMock(return_value=False)
    vm1.IsInterruptible = mock.MagicMock(return_value=False)
    spec.ConstructVirtualMachines()
    spec.vms = [vm0, vm1]
    timer = timing_util.IntervalTimer()
    pkb.DoPreparePhase(spec, timer)
    for vm in spec.vms:
      self.assertEqual(vm.PrepareBackgroundWorkload.call_count, 1)

    with mock.patch(ping_benchmark.__name__ + '.Run'):
      vm0.StopBackgroundWorkload.side_effect = functools.partial(
          self._CheckAndIncrement, expected_last_call=0)
      pkb.DoCleanupPhase(spec, timer)
      for vm in spec.vms:
        self.assertEqual(vm.StartBackgroundWorkload.call_count, 1)
        self.assertEqual(vm.StopBackgroundWorkload.call_count, 1)
        self.assertEqual(vm.PrepareBackgroundWorkload.call_count, 1)


if __name__ == '__main__':
  unittest.main()
