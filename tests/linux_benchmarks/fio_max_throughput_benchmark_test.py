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
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import fio_max_throughput_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class FioMaxThroughputBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.mock_vm = mock.MagicMock()
    self.mock_vm.num_cpus = 8
    self.mock_vm.NumCpusForBenchmark.return_value = 8
    self.mock_spec = mock.MagicMock()
    self.mock_spec.vms = [self.mock_vm]

  def testGetMaxThroughputSample(self):
    samples = [
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-1:read:bandwidth',
            1000,
            'KB/s',
            {'numjobs': '1'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-2:read:bandwidth',
            1005,
            'KB/s',
            {'numjobs': '2'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-4:read:bandwidth',
            1010,
            'KB/s',
            {'numjobs': '4'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-8:read:bandwidth',
            2000,
            'KB/s',
            {'numjobs': '8'},
        ),
    ]
    max_sample = fio_max_throughput_benchmark._GetMaxThroughputSample(samples)
    self.assertEqual(max_sample.value, 2000)
    self.assertEqual(max_sample.metadata['numjobs'], '8')

  def testGetMaxThroughputSampleNoSamples(self):
    samples = []
    with self.assertRaises(errors.Benchmarks.RunError):
      fio_max_throughput_benchmark._GetMaxThroughputSample(samples)

  @mock.patch('perfkitbenchmarker.linux_benchmarks.fio.utils.GenerateJobFile')
  @mock.patch('perfkitbenchmarker.linux_benchmarks.fio.utils.RunTest')
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.fio.flags.FIO_OPERATION_TYPE'
  )
  def testRun(self, mock_op_type, mock_run_test, mock_generate_job_file):
    mock_op_type.value = 'read'
    mock_generate_job_file.return_value = 'fake job file'
    mock_run_test.return_value = [
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-1:read:bandwidth',
            1000,
            'KB/s',
            {'numjobs': '1'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-2:read:bandwidth',
            1005,
            'KB/s',
            {'numjobs': '2'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-4:read:bandwidth',
            1010,
            'KB/s',
            {'numjobs': '4'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-8:read:bandwidth',
            2000,
            'KB/s',
            {'numjobs': '8'},
        ),
    ]

    samples = fio_max_throughput_benchmark.Run(self.mock_spec)

    self.assertEqual(len(samples), 5)  # 4 from RunTest + 1 max_throughput
    max_throughput_samples = [
        s for s in samples if s.metric == 'max_throughput'
    ]
    self.assertEqual(len(max_throughput_samples), 1)
    self.assertEqual(max_throughput_samples[0].value, 2000)

  @mock.patch('perfkitbenchmarker.linux_benchmarks.fio.utils.GenerateJobFile')
  @mock.patch('perfkitbenchmarker.linux_benchmarks.fio.utils.RunTest')
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.fio.flags.FIO_OPERATION_TYPE'
  )
  def testRunNoPlateauSuccess(
      self, mock_op_type, mock_run_test, mock_generate_job_file
  ):
    mock_op_type.value = 'read'
    mock_generate_job_file.return_value = 'fake job file'

    samples = [
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-1:read:bandwidth',
            1000,
            'KB/s',
            {'numjobs': '1'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-2:read:bandwidth',
            2000,
            'KB/s',
            {'numjobs': '2'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-4:read:bandwidth',
            3000,
            'KB/s',
            {'numjobs': '4'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-8:read:bandwidth',
            4000,
            'KB/s',
            {'numjobs': '8'},
        ),
    ]
    mock_run_test.return_value = samples

    samples = fio_max_throughput_benchmark.Run(self.mock_spec)

    max_throughput_samples = [
        s for s in samples if s.metric == 'max_throughput'
    ]
    self.assertEqual(len(max_throughput_samples), 1)
    self.assertEqual(max_throughput_samples[0].value, 4000)

  @mock.patch('perfkitbenchmarker.linux_benchmarks.fio.utils.GenerateJobFile')
  @mock.patch('perfkitbenchmarker.linux_benchmarks.fio.utils.RunTest')
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.fio.flags.FIO_OPERATION_TYPE'
  )
  def testRunPlateauSuccess(
      self, mock_op_type, mock_run_test, mock_generate_job_file
  ):

    mock_op_type.value = 'read'
    mock_generate_job_file.return_value = 'fake job file'

    samples = [
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-1:read:bandwidth',
            1000,
            'KB/s',
            {'numjobs': '1'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-2:read:bandwidth',
            1005,
            'KB/s',
            {'numjobs': '2'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-4:read:bandwidth',
            1010,
            'KB/s',
            {'numjobs': '4'},
        ),
        sample.Sample(
            'seq_1m_read_100%_iodepth-64_numjobs-8:read:bandwidth',
            2000,
            'KB/s',
            {'numjobs': '8'},
        ),
    ]
    mock_run_test.return_value = samples

    samples = fio_max_throughput_benchmark.Run(self.mock_spec)

    max_throughput_samples = [
        s for s in samples if s.metric == 'max_throughput'
    ]
    self.assertEqual(len(max_throughput_samples), 1)
    self.assertEqual(max_throughput_samples[0].value, 2000)


if __name__ == '__main__':
  unittest.main()
