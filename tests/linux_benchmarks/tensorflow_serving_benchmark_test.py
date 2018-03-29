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
"""Tests for the Tensorflow Serving benchmark."""
import os
import unittest
import mock
from perfkitbenchmarker.linux_benchmarks import tensorflow_serving_benchmark

SAMPLE_CLIENT_OUTPUT = '../data/tensorflow_serving_client_workload_stdout.txt'


class TensorflowServingBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    flag_values = {
        'tf_serving_client_thread_count': 12,
        'tf_serving_runtime': 60,
    }

    p = mock.patch(tensorflow_serving_benchmark.__name__ + '.FLAGS')
    p.start()
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__), SAMPLE_CLIENT_OUTPUT)
    with open(path) as fp:
      self.test_output = fp.read()

  def testParseStdoutFromClientScript(self):
    benchmark_spec = mock.MagicMock()
    num_client_threads = 12
    samples = tensorflow_serving_benchmark._MakeSamplesFromClientOutput(
        benchmark_spec, self.test_output, num_client_threads)

    expected_metadata = {
        'client_thread_count': num_client_threads,
        'scheduled_runtime': 60,
    }

    self.assertEqual(len(samples), 5)

    self.assertEqual(samples[0].metric, 'Completed requests')
    self.assertEqual(samples[0].unit, 'requests')
    self.assertEqual(samples[0].value, 1)
    self.assertDictEqual(samples[0].metadata, expected_metadata)

    self.assertEqual(samples[1].metric, 'Failed requests')
    self.assertEqual(samples[1].unit, 'requests')
    self.assertEqual(samples[1].value, 2)
    self.assertDictEqual(samples[1].metadata, expected_metadata)

    self.assertEqual(samples[2].metric, 'Throughput')
    self.assertEqual(samples[2].unit, 'images_per_second')
    self.assertEqual(samples[2].value, 5.2)
    self.assertDictEqual(samples[2].metadata, expected_metadata)

    self.assertEqual(samples[3].metric, 'Runtime')
    self.assertEqual(samples[3].unit, 'seconds')
    self.assertEqual(samples[3].value, 3.3)
    self.assertDictEqual(samples[3].metadata, expected_metadata)

    expected_metadata.update({'latency_array': [1.1, 2.2, 3.3]})
    self.assertEqual(samples[4].metric, 'Latency')
    self.assertEqual(samples[4].unit, 'seconds')
    self.assertEqual(samples[4].value, -1)
    self.assertDictEqual(samples[4].metadata, expected_metadata)


if __name__ == '__main__':
  unittest.main()
