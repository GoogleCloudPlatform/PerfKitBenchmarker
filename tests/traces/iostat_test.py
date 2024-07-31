# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for iostat utility."""

import os
import unittest

from absl.testing import parameterized
import mock
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from tests import pkb_common_test_case
from perfkitbenchmarker.traces import iostat


_DEFAULT_ROLE = 'default_0'
_OUTPUT_FILE = 'iostat_output.json'

_EXPECTED_SAMPLES = [
    # no dm-0 since average util < 1
    sample.Sample(
        metric='dm-1_r_await_time_series',
        value=0.0,
        unit='ms',
        metadata={
            'values': [0.57, 0.0],
            'timestamps': [1710463994.0, 1710463995.0],
            'interval': 1,
            'event': 'iostat',
            'role': _DEFAULT_ROLE,
            'nodename': 'test-instance',
            'disk': 'dm-1',
        },
        timestamp=mock.ANY,
    ),
    sample.Sample(
        metric='sda_r_await_time_series',
        value=0.0,
        unit='ms',
        metadata={
            'values': [0.88, 0.0],
            'timestamps': [1710463994.0, 1710463995.0],
            'interval': 1,
            'event': 'iostat',
            'role': _DEFAULT_ROLE,
            'nodename': 'test-instance',
            'disk': 'sda',
        },
        timestamp=mock.ANY,
    ),
]


def _CreateDummyVm(base_os_type: str):
  vm_spec = pkb_common_test_case.CreateTestVmSpec()
  vm = pkb_common_test_case.TestVirtualMachine(vm_spec)
  vm.BASE_OS_TYPE = base_os_type
  return vm


class IostatCollectorTestCase(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.output_directory = os.path.join(
        os.path.dirname(__file__), '..', 'data'
    )
    self.benchmark_spec = mock.create_autospec(
        'perfkitbenchmarker.benchmark_spec.BenchmarkSpec', instance=True
    )

  @parameterized.named_parameters(
      ('default_test', _EXPECTED_SAMPLES),
      (
          'device_regex_test',
          [s for s in _EXPECTED_SAMPLES if s.metric.startswith('sda')],
          'sda',
      ),
  )
  def testIostatCollectorAnalyze(
      self,
      expected_samples: list[sample.Sample],
      device_regex: str | None = None,
  ):
    collector = iostat.IostatCollector(
        interval=1,
        output_directory=self.output_directory,
        disk_metrics=['r_await'],
        cpu_metrics=None,
        device_regex=device_regex,
    )
    collector._role_mapping = {_DEFAULT_ROLE: _OUTPUT_FILE}
    samples = []
    collector.Analyze('test-sender', self.benchmark_spec, samples)
    self.assertCountEqual(expected_samples, samples)

  @parameterized.named_parameters(
      (
          'windows_not_supported_test',
          _CreateDummyVm(os_types.WINDOWS),
          '',
          'not supported on Windows',
      ),
      (
          'linux_default_test',
          _CreateDummyVm(os_types.DEBIAN),
          (
              'export S_TIME_FORMAT=ISO; iostat -xt 1 -o JSON >'
              f' {_OUTPUT_FILE} 2>&1 & echo $!'
          ),
      ),
  )
  def testIostatCollectorRunCommand(
      self,
      vm,
      expected_cmd: str = '',
      raised_error_regex: str = '',
  ):
    collector = iostat.IostatCollector(output_directory=self.output_directory)
    if raised_error_regex:
      with self.assertRaisesRegex(NotImplementedError, raised_error_regex):
        collector._CollectorRunCommand(vm, _OUTPUT_FILE)
    else:
      cmd = collector._CollectorRunCommand(vm, _OUTPUT_FILE)
      self.assertEqual(expected_cmd, cmd)


if __name__ == '__main__':
  unittest.main()
