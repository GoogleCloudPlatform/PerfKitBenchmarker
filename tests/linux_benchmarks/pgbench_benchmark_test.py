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
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import pgbench_benchmark
from perfkitbenchmarker.linux_packages import pgbench
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class PgbenchBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    path = os.path.join(
        os.path.dirname(__file__), '../data', 'pgbench.stderr.txt'
    )
    with open(path) as fp:
      self.stderr_output = fp.read()

  def testMakeSamplesFromOutput(self):
    testMetadata = {'foo': 'bar'}
    num_clients = 32
    num_jobs = 16
    expected_tps_metadata = testMetadata.copy()
    expected_tps_metadata.update({  # pyrefly: ignore[no-matching-overload]
        'clients': num_clients,
        'jobs': num_jobs,
        'tps': [7.0, 14.0, 13.0, 14.0, 13.0],
    })
    expected_latency_metadata = testMetadata.copy()
    expected_latency_metadata.update({  # pyrefly: ignore[no-matching-overload]
        'clients': num_clients,
        'jobs': num_jobs,
        'latency': [435.396, 1038.548, 1055.813, 1123.461, 1358.214],
    })

    actual = pgbench.MakeSamplesFromOutput(
        self.stderr_output, num_clients, num_jobs, testMetadata
    )
    self.assertEqual(2, len(actual))

    tps_sample = [x for x in actual if x.metric == 'tps_array'][0]
    self.assertEqual(tps_sample.value, -1)
    self.assertEqual(tps_sample.unit, 'tps')
    self.assertDictEqual(tps_sample.metadata, expected_tps_metadata)

    latency_sample = [x for x in actual if x.metric == 'latency_array'][0]
    self.assertEqual(latency_sample.value, -1)
    self.assertEqual(latency_sample.unit, 'ms')
    self.assertDictEqual(latency_sample.metadata, expected_latency_metadata)

  @flagsaver.flagsaver(pgbench_scale_factor=250)
  def testPrepareSetsSimulatedDatasetSizeGb(self):
    benchmark_spec = mock.Mock()
    benchmark_spec.vm_groups = {'clients': [mock.Mock()]}
    benchmark_spec.relational_db = mock.Mock()
    benchmark_spec.vms = [mock.Mock()]

    with mock.patch.object(pgbench_benchmark, 'CreateDatabase'):
      pgbench_benchmark.Prepare(benchmark_spec)

    expected_gb = (250 * 16.0) / 1024.0
    self.assertAlmostEqual(
        benchmark_spec.relational_db.simulated_dataset_size_gb,
        expected_gb,
        places=4,
    )


def _MakeRunSamples(tps_values, latency_values, num_clients, num_jobs):
  """Builds a (tps_array, latency_array) sample pair for one pgbench run."""
  tps_sample = sample.Sample(
      'tps_array',
      -1,
      'tps',
      {'clients': num_clients, 'jobs': num_jobs, 'tps': tps_values},
  )
  latency_sample = sample.Sample(
      'latency_array',
      -1,
      'ms',
      {'clients': num_clients, 'jobs': num_jobs, 'latency': latency_values},
  )
  return [tps_sample, latency_sample]


class PgBenchResultsTestCase(unittest.TestCase):

  def testMaxTpsSamplesSelectsHighestMeanTps(self):
    results = pgbench.PgBenchResults()
    results.Add(
        8,
        _MakeRunSamples(
            tps_values=[10.0, 20.0],
            latency_values=[1.0, 2.0],
            num_clients=8,
            num_jobs=8,
        ),
    )
    results.Add(
        16,
        _MakeRunSamples(
            tps_values=[30.0, 40.0],
            latency_values=[3.0, 4.0],
            num_clients=16,
            num_jobs=16,
        ),
    )
    results.Add(
        32,
        _MakeRunSamples(
            tps_values=[20.0, 30.0],
            latency_values=[5.0, 6.0],
            num_clients=32,
            num_jobs=32,
        ),
    )

    max_samples = results.GetBestSamples(
        'tps_array', 'max_tps', sample.Aggregation.MAX
    )

    # One max_tps_ duplicate per sample in the winning (16-client) run.
    metrics = sorted(s.metric for s in max_samples)
    self.assertEqual(metrics, ['max_tps_latency_array', 'max_tps_tps_array'])

    max_tps_sample = [
        s for s in max_samples if s.metric == 'max_tps_tps_array'
    ][0]
    self.assertEqual(max_tps_sample.metadata['clients'], 16)
    self.assertNotIn('max_tps', max_tps_sample.metadata)

  def testMaxTpsSamplesAnnotatesWinningRun(self):
    results = pgbench.PgBenchResults()
    losing_run = _MakeRunSamples(
        tps_values=[10.0, 20.0],
        latency_values=[1.0, 2.0],
        num_clients=8,
        num_jobs=8,
    )
    winning_run = _MakeRunSamples(
        tps_values=[30.0, 40.0],
        latency_values=[3.0, 4.0],
        num_clients=16,
        num_jobs=16,
    )
    results.Add(8, losing_run)
    results.Add(16, winning_run)

    results.GetBestSamples('tps_array', 'max_tps', sample.Aggregation.MAX)

    # Only the winning run's original samples get the max_tps annotation.
    for s in winning_run:
      self.assertTrue(s.metadata.get('max_tps'))
    for s in losing_run:
      self.assertNotIn('max_tps', s.metadata)


if __name__ == '__main__':
  unittest.main()
