# Lint as: python3
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
"""Tests for large scale boot benchmark."""

import copy
import unittest
import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import large_scale_boot_benchmark
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class LargeScaleBootBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(
      large_scale_boot_benchmark, '_GetExpectedBoots', return_value=4)
  def testWaitForResponsesSuccess(self, mock_func):
    vm1 = mock.Mock()
    vm1.RemoteCommand.side_effect = [('', ''), ('2 results', '')]
    vm2 = mock.Mock()
    vm2.RemoteCommand.side_effect = [('', ''), ('2 results', '')]
    large_scale_boot_benchmark._WaitForResponses([vm1, vm2])
    vm1.RemoteCommand.assert_called_with('wc -l /tmp/pkb/results')
    vm2.RemoteCommand.assert_called_with('wc -l /tmp/pkb/results')

  @mock.patch.object(
      large_scale_boot_benchmark, '_GetExpectedBoots', return_value=4)
  def testWaitForResponsesDeadServer(self, mock_func):
    vm1 = mock.Mock()
    vm1.RemoteCommand.side_effect = [('Error: Failed', ''), ('2 results', '')]
    with self.assertRaises(errors.Benchmarks.RunError):
      large_scale_boot_benchmark._WaitForResponses([vm1])

  @mock.patch.object(
      large_scale_boot_benchmark, '_GetExpectedBoots', return_value=5)
  def testWaitForResponsesTwice(self, mock_func):
    vm1 = mock.Mock()
    vm1.RemoteCommand.side_effect = [
        ('', ''), ('2 results', ''), ('', ''), ('5 results', '')]
    large_scale_boot_benchmark._WaitForResponses([vm1])
    self.assertEqual(vm1.RemoteCommand.call_count, 4)

  def testParseResult(self):
    FLAGS['num_vms'].value = 2
    FLAGS['boots_per_launcher'].value = 3
    FLAGS['zone'].value = 'zone'

    vm1 = mock.Mock()
    vm1.RemoteCommand.side_effect = [('6', ''),
                                     ('Pass:a:8\nPass:b:9\nPass:c:13', '')]
    vm1.zone = 'zone'
    vm2 = mock.Mock()
    vm2.RemoteCommand.side_effect = [('2', ''),
                                     ('Pass:d:4\nFail:e:5\nPass:f:6', '')]
    vm2.zone = 'zone'
    results = large_scale_boot_benchmark._ParseResult([vm1, vm2])

    common_metadata = {
        'cloud': 'GCP',
        'num_launchers': 2,
        'expected_boots_per_launcher': 3,
        'boot_os_type': 'debian9',
        'boot_machine_type': 'n1-standard-2',
        'launcher_machine_type': 'n1-standard-16',
    }

    metadata1 = copy.deepcopy(common_metadata)
    metadata1.update({
        'zone': 'zone',
        'launcher_successes': 3,
        'launcher_boot_durations_ns': [2, 3, 7],
        'launcher_closed_incoming': 0
    })
    metadata2 = copy.deepcopy(common_metadata)
    metadata2.update({
        'zone': 'zone',
        'launcher_successes': 2,
        'launcher_boot_durations_ns': [2, 4],
        'launcher_closed_incoming': 1
    })
    expected = [
        sample.Sample(
            metric='Launcher Boot Details',
            value=-1,
            unit='',
            metadata=metadata1),
        sample.Sample(
            metric='Launcher Boot Details',
            value=-1,
            unit='',
            metadata=metadata2),
        sample.Sample(
            metric='Cluster Max Boot Time',
            value=7,
            unit='nanoseconds',
            metadata=common_metadata),
        sample.Sample(
            metric='Cluster Expected Boots',
            value=6,
            unit='',
            metadata=common_metadata),
        sample.Sample(
            metric='Cluster Success Boots', value=5, unit='',
            metadata=common_metadata)
    ]
    for result, expected in zip(results, expected):
      self.assertEqual(result.metric, expected.metric,
                       'Metric name for {} is not equal.'.format(
                           expected.metric))
      self.assertEqual(result.value, expected.value,
                       'Metric value for {} is not equal.'.format(
                           expected.metric))
      self.assertDictEqual(result.metadata, expected.metadata,
                           'Metadata for {} is not equal'.format(
                               expected.metric))


if __name__ == '__main__':
  unittest.main()
