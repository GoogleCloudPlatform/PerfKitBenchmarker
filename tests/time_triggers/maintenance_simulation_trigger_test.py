# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.time_triggers.maintenance_simulation_trigger."""

import unittest
from unittest import mock
from absl import flags

from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case
from perfkitbenchmarker.time_triggers import maintenance_simulation_trigger

FLAGS = flags.FLAGS


class MaintenanceSimulationTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(MaintenanceSimulationTest, self).setUp()
    FLAGS.simulate_maintenance = True

  def testInitialization(self):
    FLAGS.simulate_maintenance_delay = 10
    FLAGS.capture_live_migration_timestamps = True
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    self.assertTrue(trigger.capture_live_migration_timestamps, True)
    self.assertEqual(trigger.delay, 10)

  def testTrigger(self):
    vm = mock.Mock()
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.TriggerMethod(vm)
    vm.SimulateMaintenanceEvent.assert_called_once()

  def testSetup(self):
    vm = mock.Mock()
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.vms = [vm]
    trigger.SetUp()
    vm.SetupLMNotification.assert_not_called()

    trigger.capture_live_migration_timestamps = True
    trigger.SetUp()
    vm.SetupLMNotification.assert_called_once()

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAppendSamples(self):
    vm = mock.Mock()
    time_dic = {'LM_total_time': 10}
    s = []
    vm.CollectLMNotificationsTime = mock.MagicMock(return_value=time_dic)
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.capture_live_migration_timestamps = True
    trigger.vms = [vm]
    trigger.AppendSamples(None, None, s)
    self.assertEqual(
        s, [Sample('LM Total Time', 10, 'seconds', time_dic, timestamp=0)])


if __name__ == '__main__':
  unittest.main()
