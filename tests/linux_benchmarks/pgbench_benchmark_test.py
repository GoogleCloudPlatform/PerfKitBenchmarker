# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for pgbench benchmark."""
import os
import unittest
import mock

from perfkitbenchmarker.linux_benchmarks import pgbench_benchmark


class PgbenchBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(pgbench_benchmark.__name__ + '.FLAGS')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__), '../data',
                        'pgbench.stderr.txt')
    with open(path) as fp:
      self.stderr_output = fp.read()

  def testMakeSamplesFromOutput(self):
    testMetadata = {'foo': 'bar'}
    num_clients = 32
    num_jobs = 16
    expected_tps_metadata = testMetadata.copy()
    expected_tps_metadata.update({
        'clients': num_clients,
        'jobs': num_jobs,
        'tps': [7.0, 14.0, 13.0, 14.0, 13.0],
    })
    expected_latency_metadata = testMetadata.copy()
    expected_latency_metadata.update({
        'clients': num_clients,
        'jobs': num_jobs,
        'latency': [435.396, 1038.548, 1055.813, 1123.461, 1358.214],
    })

    actual = pgbench_benchmark.MakeSamplesFromOutput(
        self.stderr_output, num_clients, num_jobs, testMetadata)
    self.assertEqual(2, len(actual))

    tps_sample = [x for x in actual if x.metric == 'tps_array'][0]
    self.assertEqual(tps_sample.value, -1)
    self.assertEqual(tps_sample.unit, 'tps')
    self.assertDictEqual(tps_sample.metadata, expected_tps_metadata)

    latency_sample = [x for x in actual if x.metric == 'latency_array'][0]
    self.assertEqual(latency_sample.value, -1)
    self.assertEqual(latency_sample.unit, 'ms')
    self.assertDictEqual(latency_sample.metadata, expected_latency_metadata)


if __name__ == '__main__':
  unittest.main()
