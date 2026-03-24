# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

import unittest
from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.linux_benchmarks import neper_benchmark

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class NeperBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.client_vm = mock.MagicMock()
    self.server_vm = mock.MagicMock()
    self.spec.vms = [self.client_vm, self.server_vm]
    self.client_vm.name = 'client'
    self.server_vm.name = 'server'
    self.server_vm.internal_ip = '10.0.0.2'

  @mock.patch('perfkitbenchmarker.linux_benchmarks.neper_benchmark.GetProcStat')
  @mock.patch('perfkitbenchmarker.linux_packages.neper.GetPath')
  def testRun(self, mock_get_path, mock_get_proc_stat):
    mock_get_path.return_value = '/usr/local/bin/tcp_rr'
    # Mock /proc/stat snapshots
    mock_get_proc_stat.side_effect = [
        {'softirq': 100, 'total': 1000, 'active': 500},  # client T0
        {'softirq': 200, 'total': 2000, 'active': 800},  # server T0
        {'softirq': 150, 'total': 2000, 'active': 1000},  # client Tend
        {'softirq': 300, 'total': 3000, 'active': 1300},  # server Tend
    ]

    # Mock Neper output
    self.client_vm.RemoteCommand.return_value = (
        (
            'num_transactions=1000\n'
            'throughput=100.0\n'
            'latency_min=0.1\n'
            'latency_max=1.0\n'
            'latency_p50=0.5\n'
            'latency_p90=0.8\n'
            'latency_p99=0.9\n'
        ),
        '',
    )

    samples = neper_benchmark.Run(self.spec)

    # Verify samples
    sample_names = [s.metric for s in samples]
    self.assertIn('client_cpu_utilization', sample_names)
    self.assertIn('client_softirq_utilization', sample_names)
    self.assertIn('server_cpu_utilization', sample_names)
    self.assertIn('server_softirq_utilization', sample_names)
    self.assertIn('throughput', sample_names)
    self.assertIn('latency_p50', sample_names)
    self.assertIn('latency_p99', sample_names)
    self.assertIn('latency_max', sample_names)

    # Verify deltas
    # Client: DeltaTotal=1000, DeltaActive=500, DeltaSoftIRQ=50
    # CPU util = 50%, SoftIRQ util = 5%
    for s in samples:
      if s.metric == 'client_cpu_utilization':
        self.assertEqual(s.value, 50.0)
      if s.metric == 'client_softirq_utilization':
        self.assertEqual(s.value, 5.0)


if __name__ == '__main__':
  unittest.main()
