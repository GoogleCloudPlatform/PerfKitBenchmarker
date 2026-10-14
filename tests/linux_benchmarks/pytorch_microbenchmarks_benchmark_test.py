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
"""Unit tests for the PyTorch memory bandwidth microbenchmarks."""

import os
import tempfile
import unittest
from unittest import mock
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker.linux_benchmarks import pytorch_microbenchmarks_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_SAMPLE_BENCHMARK_OUTPUT = """
{"test_name": "add_nvidia-b200_chips8", "M": 512, "N": 512, "K": 1024, "message_size_bytes": 1073741824, "device": "nvidia-b200", "num_chips": 8, "bandwidth_gbps": 51525.77}
{"test_name": "add_v6e_chips8", "M": 512, "N": 512, "K": 1024, "message_size_bytes": 1073741824, "device": "v6e", "num_chips": 8, "bandwidth_gbps": 9930.24}
"""


class PytorchMicrobenchmarksBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def testParseBenchmarkOutput(self):
    samples = pytorch_microbenchmarks_benchmark._ParseBenchmarkOutput(
        _SAMPLE_BENCHMARK_OUTPUT
    )

    self.assertLen(samples, 6)

    b200_samples = [s for s in samples if s.metadata['device'] == 'nvidia-b200']
    tpu_samples = [s for s in samples if s.metadata['device'] == 'v6e']

    self.assertLen(b200_samples, 3)
    self.assertLen(tpu_samples, 3)

    b200_by_metric = {s.metric: s for s in b200_samples}
    tpu_by_metric = {s.metric: s for s in tpu_samples}

    b200_bw = b200_by_metric['Memory_Bandwidth']
    self.assertAlmostEqual(b200_bw.value, 51525.77)
    self.assertEqual(b200_bw.unit, 'GB/s')
    self.assertEqual(b200_bw.metadata['test_name'], 'add_nvidia-b200_chips8')
    self.assertEqual(b200_bw.metadata['device'], 'nvidia-b200')
    self.assertEqual(b200_bw.metadata['num_chips'], 8)
    self.assertEqual(b200_bw.metadata['M'], 512)
    self.assertEqual(b200_bw.metadata['N'], 512)
    self.assertEqual(b200_bw.metadata['K'], 1024)

    b200_att = b200_by_metric['Attained_Percent']
    # Peak is 8000.0 * 8 = 64000.0 GB/s. 51525.77 / 64000.0 * 100 = 80.509015625
    self.assertAlmostEqual(b200_att.value, 80.509015625)
    self.assertEqual(b200_att.unit, '%')
    self.assertEqual(b200_att.metadata['test_name'], 'add_nvidia-b200_chips8')

    b200_der = b200_by_metric['Derate_Percent']
    self.assertAlmostEqual(b200_der.value, 19.490984375)
    self.assertEqual(b200_der.unit, '%')
    self.assertEqual(b200_der.metadata['test_name'], 'add_nvidia-b200_chips8')

    tpu_bw = tpu_by_metric['Memory_Bandwidth']
    self.assertAlmostEqual(tpu_bw.value, 9930.24)
    self.assertEqual(tpu_bw.unit, 'GB/s')
    self.assertEqual(tpu_bw.metadata['test_name'], 'add_v6e_chips8')

    tpu_att = tpu_by_metric['Attained_Percent']
    # Peak is 1638.0 * 8 = 13104.0 GB/s. 9930.24 / 13104.0 * 100 = 75.78022
    self.assertAlmostEqual(tpu_att.value, 75.78022, places=4)
    self.assertEqual(tpu_att.metadata['test_name'], 'add_v6e_chips8')

    tpu_der = tpu_by_metric['Derate_Percent']
    self.assertAlmostEqual(tpu_der.value, 24.21978, places=4)
    self.assertEqual(tpu_der.metadata['test_name'], 'add_v6e_chips8')

  def testInstallTpuDependencies(self):
    vm = mock.Mock()
    vm.OS_TYPE = 'ubuntu2204'
    pytorch_microbenchmarks_benchmark._InstallTpuDependencies(vm)
    vm.RemoteCommand.assert_called_once()
    self.assertIn('torch_xla[tpu]', vm.RemoteCommand.call_args[0][0])

  def testInstallNvidiaDependenciesB200(self):
    vm = mock.Mock()
    vm.OS_TYPE = 'ubuntu2404'
    vm.RemoteCommand.side_effect = [('2901', ''), ('', '')]
    pytorch_microbenchmarks_benchmark._InstallNvidiaDependencies(vm)
    self.assertEqual(vm.RemoteCommand.call_count, 2)
    self.assertIn('nvidia-headless', vm.RemoteCommand.call_args_list[1][0][0])

  @mock.patch.object(data, 'ResourcePath', return_value='/dummy/template.py.j2')
  def testPrepareRendersTemplate(self, _):
    vm = mock.Mock()
    vm.OS_TYPE = 'ubuntu2204'
    vm.RemoteCommand.return_value = ('', '')
    spec = mock.Mock(vms=[vm])
    FLAGS.pytorch_microbenchmark_device_counts = ['1', '2', '4', '8']

    pytorch_microbenchmarks_benchmark.Prepare(spec)

    vm.RenderTemplate.assert_called_once_with(
        '/dummy/template.py.j2',
        '~/pytorch_microbenchmarks_runner.py',
        {'device_counts': '1,2,4,8'},
    )

  def testPlotBandwidthCurves(self):
    raw_metrics = [
        {
            'test_name': 'add_b200_chips1',
            'num_chips': 1,
            'message_size_bytes': 1048576,
            'bandwidth_gbps': 6400.0,
            'device': 'b200',
        },
        {
            'test_name': 'add_b200_chips2',
            'num_chips': 2,
            'message_size_bytes': 1048576,
            'bandwidth_gbps': 12800.0,
            'device': 'b200',
        },
        {
            'test_name': 'add_b200_chips4',
            'num_chips': 4,
            'message_size_bytes': 1048576,
            'bandwidth_gbps': 25600.0,
            'device': 'b200',
        },
        {
            'test_name': 'add_b200_chips8',
            'num_chips': 8,
            'message_size_bytes': 1048576,
            'bandwidth_gbps': 51200.0,
            'device': 'b200',
        },
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
      output_path = os.path.join(tmpdir, 'pytorch_derate.png.log')
      pytorch_microbenchmarks_benchmark._PlotBandwidthCurves(
          raw_metrics, output_path
      )
      self.assertTrue(os.path.exists(output_path))
      self.assertGreater(os.path.getsize(output_path), 0)

  def testRunExecutesPerChipCount(self):
    vm = mock.Mock()
    vm.RemoteCommand.side_effect = [
        (
            (
                '{"test_name": "add_b200_chips1", "M": 16, "N": 16, "K": 16,'
                ' "message_size_bytes": 16384, "device": "b200", "num_chips":'
                ' 1, "bandwidth_gbps": 100.0}\n'
            ),
            '',
        ),
        (
            (
                '{"test_name": "add_b200_chips2", "M": 16, "N": 16, "K": 16,'
                ' "message_size_bytes": 16384, "device": "b200", "num_chips":'
                ' 2, "bandwidth_gbps": 200.0}\n'
            ),
            '',
        ),
        (
            (
                '{"test_name": "add_b200_chips4", "M": 16, "N": 16, "K": 16,'
                ' "message_size_bytes": 16384, "device": "b200", "num_chips":'
                ' 4, "bandwidth_gbps": 400.0}\n'
            ),
            '',
        ),
        (
            (
                '{"test_name": "add_b200_chips8", "M": 16, "N": 16, "K": 16,'
                ' "message_size_bytes": 16384, "device": "b200", "num_chips":'
                ' 8, "bandwidth_gbps": 800.0}\n'
            ),
            '',
        ),
    ]
    spec = mock.Mock(vms=[vm])
    FLAGS.pytorch_microbenchmark_device_counts = ['1', '2', '4', '8']
    FLAGS.tpu_type = None

    with mock.patch.object(
        pytorch_microbenchmarks_benchmark, '_PlotBandwidthCurves'
    ) as mock_plot:
      samples = pytorch_microbenchmarks_benchmark.Run(spec)

    self.assertEqual(vm.RemoteCommand.call_count, 4)
    vm.RemoteCommand.assert_any_call(
        'python3 ~/pytorch_microbenchmarks_runner.py --num_chips=1'
    )
    vm.RemoteCommand.assert_any_call(
        'python3 ~/pytorch_microbenchmarks_runner.py --num_chips=2'
    )
    vm.RemoteCommand.assert_any_call(
        'python3 ~/pytorch_microbenchmarks_runner.py --num_chips=4'
    )
    vm.RemoteCommand.assert_any_call(
        'python3 ~/pytorch_microbenchmarks_runner.py --num_chips=8'
    )
    mock_plot.assert_called_once()
    self.assertNotEmpty(samples)


if __name__ == '__main__':
  unittest.main()
