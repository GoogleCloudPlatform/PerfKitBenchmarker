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
"""Unit tests for dpb_dataflow_gpu_inference_benchmark."""

import json
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import errors
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker.linux_benchmarks import (
    dpb_dataflow_gpu_inference_benchmark as benchmark,
)
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

# Minimal flag values required by most tests.
_REQUIRED_FLAGS = {
    'dpb_dataflow_gpu_model_path': 'gs://my-bucket/bert-model/',
    'dpb_dataflow_gpu_worker_image': 'gcr.io/my-project/gpu-worker:latest',
    'dpb_service_zone': 'us-central1-a',
}


class GetModesTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(dpb_dataflow_gpu_inference_mode='local_gpu')
  def testLocalGpuMode(self):
    self.assertEqual(benchmark._GetModes(), ['local_gpu'])

  @flagsaver.flagsaver(dpb_dataflow_gpu_inference_mode='vertex_ai')
  def testVertexAiMode(self):
    self.assertEqual(benchmark._GetModes(), ['vertex_ai'])

  @flagsaver.flagsaver(dpb_dataflow_gpu_inference_mode='both')
  def testBothMode(self):
    self.assertEqual(benchmark._GetModes(), ['local_gpu', 'vertex_ai'])


class ResolveVertexAiWorkerMachineTest(
    pkb_common_test_case.PkbCommonTestCase, parameterized.TestCase
):

  @parameterized.named_parameters(
      ('n1_unchanged', 'n1-standard-4', 'n1-standard-4'),
      ('n1_8_unchanged', 'n1-standard-8', 'n1-standard-8'),
      ('g2_to_n1', 'g2-standard-4', 'n1-standard-4'),
      ('g2_8_to_n1', 'g2-standard-8', 'n1-standard-8'),
      ('a2_to_n1', 'a2-highgpu-4g', 'n1-standard-4'),
      ('a3_to_n1', 'a3-highgpu-8g', 'n1-standard-8'),
  )
  def testMachineConversion(self, input_machine, expected):
    self.assertEqual(
        benchmark._ResolveVertexAiWorkerMachine(input_machine), expected
    )


class CheckPrerequisitesTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_model_path=None,
      dpb_dataflow_gpu_worker_image='gcr.io/p/img:latest',
  )
  def testMissingModelPathRaises(self):
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_model_path='gs://b/model/',
      dpb_dataflow_gpu_worker_image=None,
      dpb_dataflow_gpu_flex_template_gcs_location=None,
  )
  def testMissingImageAndTemplateRaises(self):
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_model_path='gs://b/model/',
      dpb_dataflow_gpu_worker_image='gcr.io/p/img:latest',
      dpb_dataflow_gpu_inference_mode='vertex_ai',
      dpb_dataflow_gpu_vertex_endpoint_id='',
  )
  def testVertexAiMissingEndpointRaises(self):
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_model_path='gs://b/model/',
      dpb_dataflow_gpu_worker_image='gcr.io/p/img:latest',
      dpb_dataflow_gpu_inference_mode='local_gpu',
  )
  def testValidLocalGpuPassesCheck(self):
    # Should not raise.
    benchmark.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_model_path='gs://b/model/',
      dpb_dataflow_gpu_worker_image='gcr.io/p/img:latest',
      dpb_dataflow_gpu_inference_mode='vertex_ai',
      dpb_dataflow_gpu_vertex_endpoint_id='12345',
  )
  def testValidVertexAiPassesCheck(self):
    # Should not raise.
    benchmark.CheckPrerequisites(None)


class ComputeSamplesTest(
    pkb_common_test_case.PkbCommonTestCase, parameterized.TestCase
):

  def _MakeMessages(
      self, count, latency_ms=100, inference_ms=10.0, queue_ms=50
  ):
    return [
        {
            'latency_ms': latency_ms,
            'pure_inference_time_ms': inference_ms,
            'queue_wait_ms': queue_ms,
            'inference_overhead_ms': latency_ms - queue_ms,
        }
        for _ in range(count)
    ]

  def _GetMetricValue(self, samples, name):
    for s in samples:
      if s.metric == name:
        return s.value
    return None

  @flagsaver.flagsaver(dpb_dataflow_gpu_duration_per_rate=100)
  def testFullCollectionHealthy(self):
    messages = self._MakeMessages(100, latency_ms=200)
    samples = benchmark._ComputeSamples(
        collected=messages,
        published_count=100,
        rate=1,
        metadata={'inference_mode': 'local_gpu'},
    )
    self.assertEqual(self._GetMetricValue(samples, 'healthy'), 1)
    self.assertAlmostEqual(self._GetMetricValue(samples, 'loss_rate'), 0.0)
    self.assertAlmostEqual(self._GetMetricValue(samples, 'latency_p50'), 200.0)
    self.assertAlmostEqual(self._GetMetricValue(samples, 'latency_p99'), 200.0)

  @flagsaver.flagsaver(dpb_dataflow_gpu_duration_per_rate=100)
  def testHighLossRateUnhealthy(self):
    # Only 50% collected — below the 90% threshold.
    messages = self._MakeMessages(50)
    samples = benchmark._ComputeSamples(
        collected=messages,
        published_count=100,
        rate=1,
        metadata={'inference_mode': 'local_gpu'},
    )
    self.assertEqual(self._GetMetricValue(samples, 'healthy'), 0)
    self.assertAlmostEqual(self._GetMetricValue(samples, 'loss_rate'), 0.5)

  @flagsaver.flagsaver(dpb_dataflow_gpu_duration_per_rate=100)
  def testEmptyCollectedReturnsBasicSamples(self):
    samples = benchmark._ComputeSamples(
        collected=[],
        published_count=100,
        rate=1,
        metadata={'inference_mode': 'local_gpu'},
    )
    metric_names = {s.metric for s in samples}
    self.assertIn('collected_count', metric_names)
    self.assertIn('loss_rate', metric_names)
    self.assertIn('healthy', metric_names)
    # Latency metrics should not appear when nothing was collected.
    self.assertNotIn('latency_p50', metric_names)

  @flagsaver.flagsaver(dpb_dataflow_gpu_duration_per_rate=100)
  def testThroughputCalculation(self):
    messages = self._MakeMessages(100)
    samples = benchmark._ComputeSamples(
        collected=messages,
        published_count=100,
        rate=1,
        metadata={'inference_mode': 'local_gpu'},
    )
    # 100 messages / 100 seconds = 1.0 msg/s
    self.assertAlmostEqual(
        self._GetMetricValue(samples, 'processing_throughput'), 1.0
    )

  @flagsaver.flagsaver(dpb_dataflow_gpu_duration_per_rate=100)
  def testAllMetricNamesPresent(self):
    messages = self._MakeMessages(100)
    samples = benchmark._ComputeSamples(
        collected=messages,
        published_count=100,
        rate=1,
        metadata={'inference_mode': 'local_gpu'},
    )
    metric_names = {s.metric for s in samples}
    for expected in (
        'collected_count',
        'published_count',
        'loss_rate',
        'healthy',
        'latency_p50',
        'latency_p95',
        'latency_p99',
        'latency_mean',
        'pure_inference_p50',
        'pure_inference_p99',
        'queue_wait_p50',
        'processing_throughput',
    ):
      self.assertIn(expected, metric_names, f'Missing metric: {expected}')


class EstimateCostPerHourTest(
    pkb_common_test_case.PkbCommonTestCase, parameterized.TestCase
):

  @parameterized.named_parameters(
      (
          'local_gpu_t4_n1s4',
          {
              'inference_mode': 'local_gpu',
              'num_workers': 1,
              'worker_machine_type': 'n1-standard-4',
              'gpu_type': 'nvidia-tesla-t4',
          },
          # 1 * (0.190 + 0.35) = 0.540
          0.540,
      ),
      (
          'local_gpu_t4_n1s4_two_workers',
          {
              'inference_mode': 'local_gpu',
              'num_workers': 2,
              'worker_machine_type': 'n1-standard-4',
              'gpu_type': 'nvidia-tesla-t4',
          },
          # 2 * (0.190 + 0.35) = 1.08
          1.08,
      ),
      (
          'local_gpu_unknown_machine_returns_none',
          {
              'inference_mode': 'local_gpu',
              'num_workers': 1,
              'worker_machine_type': 'n2-standard-4',
              'gpu_type': 'nvidia-tesla-t4',
          },
          None,
      ),
  )
  def testLocalGpuCost(self, metadata, expected_cost):
    result = benchmark._EstimateCostPerHour(metadata)
    if expected_cost is None:
      self.assertIsNone(result)
    else:
      self.assertAlmostEqual(result, expected_cost, places=3)

  def testVertexAiCostSingleReplica(self):
    metadata = {
        'inference_mode': 'vertex_ai',
        'num_workers': 1,
        'worker_machine_type': 'n1-standard-4',
        'gpu_type': 'nvidia-tesla-t4',
        'vertex_replicas': 1,
    }
    result = benchmark._EstimateCostPerHour(metadata)
    # worker: 1 * 0.190
    # endpoint: 1 * (0.219 + 0.402) = 0.621
    expected = 1 * 0.190 + 1 * (0.219 + 0.402)
    self.assertAlmostEqual(result, expected, places=3)

  def testVertexAiCostMultipleReplicas(self):
    metadata = {
        'inference_mode': 'vertex_ai',
        'num_workers': 2,
        'worker_machine_type': 'n1-standard-4',
        'gpu_type': 'nvidia-tesla-t4',
        'vertex_replicas': 5,
    }
    result = benchmark._EstimateCostPerHour(metadata)
    # workers: 2 * 0.190 = 0.380
    # endpoint: 5 * (0.219 + 0.402) = 3.105
    expected = 2 * 0.190 + 5 * (0.219 + 0.402)
    self.assertAlmostEqual(result, expected, places=3)


class EnsureFlexTemplateTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_flex_template_gcs_location='gs://b/template.json'
  )
  def testReturnsExistingLocation(self):
    state = {}
    result = benchmark._EnsureFlexTemplate(state, 'gs://b/staging/')
    self.assertEqual(result, 'gs://b/template.json')

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_flex_template_gcs_location=None,
      dpb_dataflow_gpu_worker_image='gcr.io/p/img:latest',
  )
  @mock.patch('perfkitbenchmarker.vm_util.IssueCommand')
  def testGeneratesAndUploadsSpec(self, mock_issue):
    temp_dir.CreateTemporaryDirectories()
    mock_issue.return_value = ('', '', 0)
    state = {}

    result = benchmark._EnsureFlexTemplate(state, 'gs://b/staging/')

    self.assertEqual(result, 'gs://b/staging/gpu_inference_template_spec.json')
    mock_issue.assert_called_once()
    cmd_args = mock_issue.call_args[0][0]
    self.assertEqual(cmd_args[0], 'gsutil')
    self.assertEqual(cmd_args[1], 'cp')

  @flagsaver.flagsaver(
      dpb_dataflow_gpu_flex_template_gcs_location=None,
      dpb_dataflow_gpu_worker_image='gcr.io/p/img:latest',
  )
  @mock.patch('perfkitbenchmarker.vm_util.IssueCommand')
  def testSpecContainsImageAndSdkInfo(self, mock_issue):
    temp_dir.CreateTemporaryDirectories()
    mock_issue.return_value = ('', '', 0)
    state = {}

    benchmark._EnsureFlexTemplate(state, 'gs://b/staging/')

    # The local spec file path is the second arg to gsutil cp.
    local_spec_path = mock_issue.call_args[0][0][2]
    with open(local_spec_path) as f:
      spec = json.load(f)

    self.assertEqual(spec['image'], 'gcr.io/p/img:latest')
    self.assertEqual(spec['sdkInfo']['language'], 'PYTHON')
    self.assertEqual(
        spec['defaultEnvironment']['sdkContainerImage'], 'gcr.io/p/img:latest'
    )
    self.assertIn(
        'use_runner_v2',
        spec['defaultEnvironment']['additionalExperiments'],
    )


if __name__ == '__main__':
  unittest.main()
