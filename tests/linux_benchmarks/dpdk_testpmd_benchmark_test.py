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
"""Tests stdout parsing for dpdk_testpmd_benchmark."""

import os
import unittest
from absl import flags
from absl.testing import parameterized
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import dpdk_testpmd_benchmark

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

DATA_FILENAMES = [
    'client_cmd.txt',
    'server_cmd.txt',
    'client_stdout.txt',
    'server_stdout.txt',
]

METADATA = {
    'dpdk_num_forwarding_cores': 2,
    'dpdk_forward_mode': ['txonly', 'rxonly'],
    'dpdk_txpkts': 64,
    'dpdk_txq': 2,
    'dpdk_rxq': 2,
    'dpdk_burst': 1,
    'dpdk_test_length': 60,
}


def _load_data(filename):
  path = os.path.join(
      os.path.dirname(__file__), '..', 'data', 'dpdk', 'dpdk_testpmd', filename
  )
  with open(path) as fp:
    data = fp.read().strip('\n')
  return data


class DpdkBenchmarkTestCase(parameterized.TestCase, unittest.TestCase):

  def setUp(self):
    super().setUp()
    # Load data
    self.expected_client_cmd = _load_data(DATA_FILENAMES[0])
    self.expected_server_cmd = _load_data(DATA_FILENAMES[1])
    self.client_stdout = _load_data(DATA_FILENAMES[2])
    self.server_stdout = _load_data(DATA_FILENAMES[3])

    self.expected_output_samples = [
        sample.Sample('TX-packets', 602201814.0, 'packets', METADATA),
        sample.Sample(
            'TX-packets-per-second', 10036696.0, 'packets/s', METADATA
        ),
        sample.Sample('TX-dropped', 0, 'dropped', METADATA),
        sample.Sample('RX-packets', 602121227.0, 'packets', METADATA),
        sample.Sample(
            'RX-packets-per-second', 10035353.0, 'packets/s', METADATA
        ),
        sample.Sample('RX-dropped', 0, 'dropped', METADATA),
        sample.Sample(
            'packet-loss-per-second', 1343.0, 'packets/s', METADATA
        ),
        sample.Sample(
            'packet-loss-rate', 0.00013380897458685609, 'rate', METADATA
        ),
    ]

    self.bm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.bm_spec.vms = [mock.Mock(), mock.Mock()]

    self.bm_spec.vms[0].RemoteCommand.return_value = (
        '0000:00:04.0.*drv=vfio-pci',
        '',
    )
    self.bm_spec.vms[1].RemoteCommand.return_value = (
        '0000:00:04.0.*drv=vfio-pci',
        '',
    )
    self.bm_spec.vms[0].internal_ips = ['', '10.220.0.33']
    self.bm_spec.vms[1].internal_ips = ['', '10.220.0.32']
    self.bm_spec.vms[0].secondary_nic_bus_info = '0000:00:04.0'
    self.bm_spec.vms[1].secondary_nic_bus_info = '0000:00:04.0'

  def testClientServerRemoteCmd(self):
    self.bm_spec.vms[0].RobustRemoteCommand.return_value = (
        'TX-packets: 60  TX-dropped: 0  ',
        '',
    )
    self.bm_spec.vms[1].RobustRemoteCommand.return_value = (
        'RX-packets: 60  RX-dropped: 0  ',
        '',
    )
    _ = dpdk_testpmd_benchmark.Run(self.bm_spec)
    self.bm_spec.vms[0].RobustRemoteCommand.assert_called_with(
        self.expected_client_cmd, ignore_failure=True
    )
    self.bm_spec.vms[1].RobustRemoteCommand.assert_called_with(
        self.expected_server_cmd, ignore_failure=True
    )

  def testClientServerStdout(self):
    self.bm_spec.vms[0].RobustRemoteCommand.return_value = (
        self.client_stdout,
        '',
    )
    self.bm_spec.vms[1].RobustRemoteCommand.return_value = (
        self.server_stdout,
        '',
    )
    output_samples = dpdk_testpmd_benchmark.Run(self.bm_spec)

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
      self.assertEqual(
          output_sample.metadata.items(),
          expected_output_sample.metadata.items(),
      )


if __name__ == '__main__':
  unittest.main()
