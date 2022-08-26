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

import datetime
import unittest
from unittest import mock
from absl import flags

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import sample
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
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    time_dic = {'LM_total_time': 10, 'Host_maintenance_end': 0}
    s = []
    vm.CollectLMNotificationsTime = mock.MagicMock(return_value=time_dic)
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.capture_live_migration_timestamps = True
    trigger.vms = [vm]
    trigger.AppendSamples(None, vm_spec, s)
    self.assertEqual(
        s, [Sample('LM Total Time', 10, 'seconds', time_dic, timestamp=0)])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamples(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.capture_live_migration_timestamps = False
    s = sample.CreateTimeSeriesSample([1, 1, 1, 1, 0, 0.1, 0.2, 0.3],
                                      [1000 * i for i in range(1, 9)],
                                      sample.TPM_TIME_SERIES, 'TPM', 1)
    samples = [s]
    trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    trigger.AppendSamples(None, vm_spec, samples)
    self.assertEqual(samples, [
        Sample(
            metric='TPM_time_series',
            value=0.0,
            unit='TPM',
            metadata={
                'values': [1, 1, 1, 1, 0, 0.1, 0.2, 0.3],
                'timestamps': [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000],
                'interval': 1
            },
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_0_percent',
            value=1.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_10_percent',
            value=2.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_20_percent',
            value=3.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_30_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_40_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_50_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_60_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_70_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_80_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_90_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='unresponsive_metric',
            value=2.584,
            unit='metric',
            metadata={},
            timestamp=0),
        Sample(
            metric='total_loss_seconds',
            value=3.4,
            unit='seconds',
            metadata={},
            timestamp=0)
    ])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamplesWithNotification(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.capture_live_migration_timestamps = True
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 0, 0.1, 0.2, 0.3, 0.95, 0.95, 0.95, 0.95],
        [1000 * i for i in range(1, 13)], sample.TPM_TIME_SERIES, 'TPM', 1)
    samples = [s]
    trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    vm = mock.MagicMock()
    vm.CollectLMNotificationsTime = mock.MagicMock(return_value={
        'LM_total_time': 100,
        'Host_maintenance_end': 8000
    })
    trigger.vms = [vm]
    trigger.AppendSamples(None, vm_spec, samples)
    self.assertEqual(samples, [
        Sample(
            metric='TPM_time_series',
            value=0.0,
            unit='TPM',
            metadata={
                'values':
                    [1, 1, 1, 1, 0, 0.1, 0.2, 0.3, 0.95, 0.95, 0.95, 0.95],
                'timestamps': [
                    1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
                    11000, 12000
                ],
                'interval': 1
            },
            timestamp=0),
        Sample(
            metric='LM Total Time',
            value=100.0,
            unit='seconds',
            metadata={
                'LM_total_time': 100,
                'Host_maintenance_end': 8000
            },
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_0_percent',
            value=1.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_10_percent',
            value=2.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_20_percent',
            value=3.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_30_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_40_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_50_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_60_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_70_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_80_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='seconds_dropped_below_90_percent',
            value=4.0,
            unit='s',
            metadata={},
            timestamp=0),
        Sample(
            metric='unresponsive_metric',
            value=2.584,
            unit='metric',
            metadata={},
            timestamp=0),
        Sample(
            metric='total_loss_seconds',
            value=3.4,
            unit='seconds',
            metadata={},
            timestamp=0),
        Sample(
            metric='degradation_percent',
            value=5.0,
            unit='%',
            metadata={},
            timestamp=0)
    ])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamplesContainsMetadata(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    trigger = maintenance_simulation_trigger.MaintenanceEventTrigger()
    trigger.capture_live_migration_timestamps = True
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 0, 0.1, 0.2, 0.3, 0.95, 0.95, 0.95, 0.95],
        [1000 * i for i in range(1, 13)],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
        additional_metadata={'random': 'random'})
    samples = [s]
    trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    vm = mock.MagicMock()
    vm.CollectLMNotificationsTime = mock.MagicMock(return_value={
        'LM_total_time': 100,
        'Host_maintenance_end': 8000
    })
    trigger.vms = [vm]
    trigger.AppendSamples(None, vm_spec, samples)
    self.assertEqual(samples, [
        sample.Sample(
            metric='TPM_time_series',
            value=0.0,
            unit='TPM',
            metadata={
                'values':
                    [1, 1, 1, 1, 0, 0.1, 0.2, 0.3, 0.95, 0.95, 0.95, 0.95],
                'timestamps': [
                    1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
                    11000, 12000
                ],
                'interval': 1,
                'random': 'random'
            },
            timestamp=0),
        sample.Sample(
            metric='LM Total Time',
            value=100.0,
            unit='seconds',
            metadata={
                'LM_total_time': 100,
                'Host_maintenance_end': 8000
            },
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_0_percent',
            value=1.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_10_percent',
            value=2.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_20_percent',
            value=3.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_30_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_40_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_50_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_60_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_70_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_80_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='seconds_dropped_below_90_percent',
            value=4.0,
            unit='s',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='unresponsive_metric',
            value=2.584,
            unit='metric',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='total_loss_seconds',
            value=3.4,
            unit='seconds',
            metadata={'random': 'random'},
            timestamp=0),
        sample.Sample(
            metric='degradation_percent',
            value=5.0,
            unit='%',
            metadata={'random': 'random'},
            timestamp=0)
    ])


if __name__ == '__main__':
  unittest.main()
