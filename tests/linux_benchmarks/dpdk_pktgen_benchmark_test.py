# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests stdout parsing for dpdk_pktgen_benchmark."""

import os
import unittest
from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import dpdk_pktgen_benchmark

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

DATA_FILENAMES = [
    'sender_stdout.txt',
    'receiver_stdout.txt',
]


def _load_data(filename):
  path = os.path.join(
      os.path.dirname(__file__), '..', 'data', 'dpdk', 'dpdk_pktgen', filename
  )
  with open(path) as fp:
    data = fp.read().strip('\n')
  return data


class DpdkBenchmarkTestCase(parameterized.TestCase, unittest.TestCase):

  def setUp(self):
    super().setUp()
    # Load data
    self.sender_stdout = _load_data(DATA_FILENAMES[0])
    self.receiver_stdout = _load_data(DATA_FILENAMES[1])

    self.expected_output_samples = [
        sample.Sample(
            'Total sender tx packets',
            670441705,
            'packets',
            {},
        ),
        sample.Sample(
            'Total sender tx pps',
            11174028,
            'packets/s',
            {},
        ),
        sample.Sample(
            'Total sender rx packets',
            11,
            'packets',
            {},
        ),
        sample.Sample(
            'Total sender rx pps',
            0,
            'packets/s',
            {},
        ),
        sample.Sample(
            'Total receiver rx packets',
            670421930,
            'packets',
            {},
        ),
        sample.Sample(
            'Total receiver rx pps',
            11173698,
            'packets/s',
            {},
        ),
        sample.Sample(
            'packet loss rate',
            0.000029511887241561144,
            'rate (1=100%)',
            {},
        ),
    ]

    self.bm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.bm_spec.vms = [mock.MagicMock(), mock.MagicMock()]
    self.bm_spec.vms[0].NumCpusForBenchmark.return_value = 22
    self.bm_spec.vms[0].tertiary_nic_bus_info = None
    self.bm_spec.vms[1].tertiary_nic_bus_info = None

  @flagsaver.flagsaver(
      dpdk_pktgen_packet_loss_threshold_rates=[1],
      dpdk_pktgen_tx_rx_lcores_list=['[8:1];[1:8]'],
  )
  def testClientServerStdout(self):
    self.bm_spec.vms[0].RemoteCommand.side_effect = [
        ('6', ''),
        ('', ''),
        ('', ''),
        (self.sender_stdout, ''),
        (670441705, ''),
        (11, ''),
        ('', ''),
        ('', ''),
        (self.sender_stdout, ''),
        (670441705, ''),
        (11, ''),
        ('', ''),
        ('', ''),
    ]
    self.bm_spec.vms[1].RemoteCommand.side_effect = [
        (self.receiver_stdout, ''),
        (670421930, ''),
        (self.receiver_stdout, ''),
        (670421930, ''),
    ]
    output_samples = dpdk_pktgen_benchmark.Run(self.bm_spec)

    # Compare each element except timestamp for each sample
    self.assertEqual(len(output_samples), len(self.expected_output_samples))
    for sample_idx in range(len(output_samples)):
      output_sample = output_samples[sample_idx]
      expected_output_sample = self.expected_output_samples[sample_idx]
      self.assertEqual(
          output_sample.metric,
          expected_output_sample.metric,
      )
      self.assertEqual(
          output_sample.value,
          expected_output_sample.value,
      )


if __name__ == '__main__':
  unittest.main()
