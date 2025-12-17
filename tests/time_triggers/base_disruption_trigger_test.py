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

import datetime
import time
import unittest
from unittest import mock
from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from tests import pkb_common_test_case
from perfkitbenchmarker.time_triggers import base_disruption_trigger

FLAGS = flags.FLAGS


class TestBaseDisruptionTrigger(base_disruption_trigger.BaseDisruptionTrigger):
  """Test class for BaseDisruptionTrigger."""

  def __init__(self):
    super().__init__(delay=10)

  def TriggerMethod(self, vm: virtual_machine.VirtualMachine):
    pass

  def SetUp(self):
    pass

  @property
  def trigger_name(self) -> str:
    return 'test_trigger'


class BaseDisruptionTriggerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.trigger = TestBaseDisruptionTrigger()
    self.maxDiff = 100000
    self.enter_context(
        mock.patch.object(self.trigger, 'WaitForDisruption', autospec=True)
    )
    self.assertEqual(self.trigger.delay, 10)

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendSamples(self):
    time_dic = {
        'LM_total_time': 10,
        'Host_maintenance_end': 10,
        'Host_maintenance_start': 4,
    }
    s = []
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            total_time=10, end_time=10, start_time=4
        )
    ]
    self.trigger.AppendSamples(None, vm_spec, s)
    self.assertEqual(
        s,
        [sample.Sample('LM Total Time', 10, 'seconds', time_dic, timestamp=0)],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionWithDegradationPercent(self):
    FLAGS.maintenance_degradation_percent = 90
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(2)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            start_time=2, end_time=8, total_time=100
        )
    ]
    s = [
        sample.CreateTimeSeriesSample(
            [100, 100, 100, 90, 90, 90],
            [1000, 2000, 3000, 4000, 5000, 6000],
            sample.TPM_TIME_SERIES,
            'TPM',
            1,
        )
    ]

    self.trigger.AppendSamples(None, vm_spec, s)
    self.assertEqual(
        s,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [100, 100, 100, 90, 90, 90],
                    'timestamps': [1000, 2000, 3000, 4000, 5000, 6000],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [100, 100, 100, 90, 90, 90],
                    'timestamps': [1000, 2000, 3000, 4000, 5000, 6000],
                    'interval': 1,
                    'LM_total_time': 100,
                    'Host_maintenance_start': 2,
                    'Host_maintenance_end': 8,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=3.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=0.0,
                unit='metric',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=0.0,
                unit='seconds',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionWithMissingTimeStampsWithRegression(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(2)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            start_time=2, end_time=8, total_time=100
        )
    ]
    s = [
        sample.CreateTimeSeriesSample(
            [100, 100, 20, 100],
            [1000, 2000, 6000, 7000],
            sample.TPM_TIME_SERIES,
            'TPM',
            1,
        )
    ]

    self.trigger.AppendSamples(None, vm_spec, s)
    self.assertEqual(
        s,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [100, 100, 20, 100],
                    'timestamps': [1000, 2000, 6000, 7000],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [100, 100, 20, 100],
                    'timestamps': [1000, 2000, 6000, 7000],
                    'interval': 1,
                    'LM_total_time': 100,
                    'Host_maintenance_start': 2,
                    'Host_maintenance_end': 8,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=3.4295,
                unit='metric',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=3.8,
                unit='seconds',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=3,
                unit='s',
                metadata={},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionWithMissingTimeStampsNoRegression(self):
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(2)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            start_time=2, end_time=8, total_time=100
        )
    ]
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    s = [
        sample.CreateTimeSeriesSample(
            [1, 1, 4, 1],
            [1000, 2000, 6000, 7000],
            sample.TPM_TIME_SERIES,
            'TPM',
            1,
        )
    ]

    self.trigger.AppendSamples(None, vm_spec, s)
    self.assertEqual(
        s,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [1, 1, 4, 1],
                    'timestamps': [1000, 2000, 6000, 7000],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [1, 1, 4, 1],
                    'timestamps': [1000, 2000, 6000, 7000],
                    'interval': 1,
                    'LM_total_time': 100,
                    'Host_maintenance_start': 2,
                    'Host_maintenance_end': 8,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=0.0,
                unit='metric',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=0.0,
                unit='seconds',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=3.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamples(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            start_time=5, end_time=8, total_time=100
        )
    ]
    self.trigger.vms = [mock.MagicMock()]
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 0, 0.1, 0.2, 0.3],
        [1000 * i for i in range(1, 9)],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
    )
    samples = [s]

    self.trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    self.trigger.AppendSamples(None, vm_spec, samples)
    self.assertEqual(
        samples,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [1, 1, 1, 1, 0, 0.1, 0.2, 0.3],
                    'timestamps': [
                        1000,
                        2000,
                        3000,
                        4000,
                        5000,
                        6000,
                        7000,
                        8000,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [1, 1, 1, 1, 0, 0.1, 0.2, 0.3],
                    'timestamps': [
                        1000,
                        2000,
                        3000,
                        4000,
                        5000,
                        6000,
                        7000,
                        8000,
                    ],
                    'interval': 1,
                    'LM_total_time': 100,
                    'Host_maintenance_start': 5,
                    'Host_maintenance_end': 8,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=1.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=2.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=3.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=2.584,
                unit='metric',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=3.4,
                unit='seconds',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=0.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamplesWithDisruptionEnds(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 0, 0.1, 0.2, 0.3, 0.95, 0.95, 0.95, 0.95],
        [1000 * i for i in range(1, 13)],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
    )
    samples = [s]
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            total_time=100, end_time=8, start_time=4
        )
    ]
    self.trigger.AppendSamples(None, vm_spec, samples)
    self.assertEqual(
        samples,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [
                        1,
                        1,
                        1,
                        1,
                        0,
                        0.1,
                        0.2,
                        0.3,
                        0.95,
                        0.95,
                        0.95,
                        0.95,
                    ],
                    'timestamps': [
                        1000,
                        2000,
                        3000,
                        4000,
                        5000,
                        6000,
                        7000,
                        8000,
                        9000,
                        10000,
                        11000,
                        12000,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [
                        1,
                        1,
                        1,
                        1,
                        0,
                        0.1,
                        0.2,
                        0.3,
                        0.95,
                        0.95,
                        0.95,
                        0.95,
                    ],
                    'timestamps': [
                        1000,
                        2000,
                        3000,
                        4000,
                        5000,
                        6000,
                        7000,
                        8000,
                        9000,
                        10000,
                        11000,
                        12000,
                    ],
                    'interval': 1,
                    'LM_total_time': 100,
                    'Host_maintenance_end': 8,
                    'Host_maintenance_start': 4,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=1.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=2.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=3.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=4.0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=2.584,
                unit='metric',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=3.4,
                unit='seconds',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='degradation_percent',
                value=5.0,
                unit='%',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=0,
                unit='s',
                metadata={},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamplesContainsMetadata(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 0, 0.1, 0.2, 0.3, 0.95, 0.95, 0.95, 0.95],
        [1000 * i for i in range(1, 13)],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
        additional_metadata={'random': 'random'},
    )
    samples = [s]
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            total_time=100, end_time=8, start_time=4
        )
    ]
    self.trigger.AppendSamples(None, vm_spec, samples)
    self.assertEqual(
        samples,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [
                        1,
                        1,
                        1,
                        1,
                        0,
                        0.1,
                        0.2,
                        0.3,
                        0.95,
                        0.95,
                        0.95,
                        0.95,
                    ],
                    'timestamps': [
                        1000,
                        2000,
                        3000,
                        4000,
                        5000,
                        6000,
                        7000,
                        8000,
                        9000,
                        10000,
                        11000,
                        12000,
                    ],
                    'interval': 1,
                    'random': 'random',
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [
                        1,
                        1,
                        1,
                        1,
                        0,
                        0.1,
                        0.2,
                        0.3,
                        0.95,
                        0.95,
                        0.95,
                        0.95,
                    ],
                    'timestamps': [
                        1000,
                        2000,
                        3000,
                        4000,
                        5000,
                        6000,
                        7000,
                        8000,
                        9000,
                        10000,
                        11000,
                        12000,
                    ],
                    'interval': 1,
                    'random': 'random',
                    'LM_total_time': 100,
                    'Host_maintenance_end': 8,
                    'Host_maintenance_start': 4,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=1.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=2.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=3.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=2.584,
                unit='metric',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=3.4,
                unit='seconds',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='degradation_percent',
                value=5.0,
                unit='%',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  def testAppendLossFunctionSamplesHandleTimeDrift(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    # Sample with 5 seconds of lost data.
    # Missing data at t=2 should be ignored, missing data at 5<=t<=8 should be
    # 0.0.
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 1, 1, 1],
        [1000 * i for i in range(1, 13) if not 5 <= i <= 8 and i != 2],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
        additional_metadata={'random': 'random'},
    )
    samples = [s]
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            total_time=100, end_time=11, start_time=4
        )
    ]
    self.trigger.AppendSamples(None, vm_spec, samples)

    self.assertEqual(
        samples,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [1, 1, 1, 1, 1, 1, 1],
                    'timestamps': [1000, 3000, 4000, 9000, 10000, 11000, 12000],
                    'interval': 1,
                    'random': 'random',
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [1, 1, 1, 1, 1, 1, 1],
                    'timestamps': [1000, 3000, 4000, 9000, 10000, 11000, 12000],
                    'interval': 1,
                    'random': 'random',
                    'LM_total_time': 100,
                    'Host_maintenance_end': 11,
                    'Host_maintenance_start': 4,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=5.0,
                unit='metric',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=5.0,
                unit='seconds',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='degradation_percent',
                value=0.0,
                unit='%',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  @flagsaver.flagsaver(
      (base_disruption_trigger.MAINTENANCE_DEGRADATION_WINDOW, 1.0)
  )
  def testMaintenanceEventTriggerAppendSamplesWithMaintenanceDegradationWindow(
      self,
  ):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    # Sample with 5 seconds of lost data.
    # Missing data at t=2 should be ignored, missing data at 5<=t<=8 should be
    # 0.0.
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 1, 1, 1],
        [1000 * i for i in range(1, 13) if not 5 <= i <= 8 and i != 2],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
        additional_metadata={'random': 'random'},
    )
    samples = [s]
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            total_time=100, end_time=11, start_time=4
        )
    ]
    self.trigger.AppendSamples(None, vm_spec, samples)
    # Assertions
    self.assertEqual(
        samples,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [1, 1, 1, 1, 1, 1, 1],
                    'timestamps': [1000, 3000, 4000, 9000, 10000, 11000, 12000],
                    'interval': 1,
                    'random': 'random',
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [1, 1, 1, 1, 1, 1, 1],
                    'timestamps': [1000, 3000, 4000, 9000, 10000, 11000, 12000],
                    'interval': 1,
                    'random': 'random',
                    'LM_total_time': 100,
                    'Host_maintenance_end': 11,
                    'Host_maintenance_start': 4,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=5.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=5.0,
                unit='metric',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=5.0,
                unit='seconds',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='degradation_percent',
                value=0.0,
                unit='%',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
        ],
    )

  @mock.patch.object(time, 'time', mock.MagicMock(return_value=0))
  @flagsaver.flagsaver(
      (base_disruption_trigger.MAINTENANCE_DEGRADATION_WINDOW, 1.0)
  )
  def testMaintenanceEventTriggerAppendSamplesWithRegressionOutsideMaintenanceWindow(
      self,
  ):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    # Sample with 5 seconds of lost data.
    # Missing data at t=2 should be ignored, missing data at 5<=t<=8 should be
    # 0.0.
    s = sample.CreateTimeSeriesSample(
        [1, 1, 1, 1, 1, 1, 1],
        [1000 * i for i in range(1, 13) if not 5 <= i <= 8 and i != 2],
        sample.TPM_TIME_SERIES,
        'TPM',
        1,
        additional_metadata={'random': 'random'},
    )
    samples = [s]
    self.trigger.trigger_time = datetime.datetime.fromtimestamp(4)
    self.trigger.disruption_events = [
        base_disruption_trigger.DisruptionEvent(
            total_time=100, end_time=8, start_time=4
        )
    ]
    self.trigger.AppendSamples(None, vm_spec, samples)
    # Assertions
    self.assertEqual(
        samples,
        [
            sample.Sample(
                metric='TPM_time_series',
                value=0.0,
                unit='TPM',
                metadata={
                    'values': [1, 1, 1, 1, 1, 1, 1],
                    'timestamps': [1000, 3000, 4000, 9000, 10000, 11000, 12000],
                    'interval': 1,
                    'random': 'random',
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='LM Total Time',
                value=100.0,
                unit='seconds',
                metadata={
                    'values': [1, 1, 1, 1, 1, 1, 1],
                    'timestamps': [1000, 3000, 4000, 9000, 10000, 11000, 12000],
                    'interval': 1,
                    'random': 'random',
                    'LM_total_time': 100,
                    'Host_maintenance_end': 8,
                    'Host_maintenance_start': 4,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_0_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_10_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_20_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_30_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_40_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_50_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_60_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_70_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_80_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='seconds_dropped_below_90_percent',
                value=0.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='unresponsive_metric',
                value=0.0,
                unit='metric',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_loss_seconds',
                value=0.0,
                unit='seconds',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='degradation_percent',
                value=62.5,
                unit='%',
                metadata={'random': 'random'},
                timestamp=0,
            ),
            sample.Sample(
                metric='total_missing_seconds',
                value=4.0,
                unit='s',
                metadata={'random': 'random'},
                timestamp=0,
            ),
        ],
    )


if __name__ == '__main__':
  unittest.main()
