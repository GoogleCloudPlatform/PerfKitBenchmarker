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

from typing import Any
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker.linux_benchmarks.kubernetes import kubernetes_glm_sglang_benchmark as glm_bench
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources.container_service import container
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case
import yaml

FLAGS = flags.FLAGS

_MODEL_URI = 'gs://my-bucket/kubernetes_glm_sglang/GLM-5.3'
# A trimmed sglang.bench_serving (v0.5.18) result: the arguments it echoes, its
# server info and its metrics.
_BENCH_RESULT = {
    'tag': None,
    'backend': 'sglang',
    'dataset_name': 'random-ids',
    'request_rate': float('inf'),
    'max_concurrency': 16,
    'sharegpt_output_len': None,
    'random_input_len': 8192,
    'random_output_len': 1024,
    'random_range_ratio': 1.0,
    'server_info': {'version': '0.5.18'},
    'duration': 120.5,
    'completed': 256,
    'request_throughput': 2.1,
    'output_throughput': 1600.0,
    'total_throughput': 14400.0,
    'mean_ttft_ms': 5900.0,
    'median_tpot_ms': 16.5,
    'p99_itl_ms': 40.0,
    'accept_length': 1.8,
    'max_output_tokens_per_s': 2400.0,
}


def _MockBenchmarkSpec(num_nodes: int = 2) -> mock.NonCallableMagicMock:
  """Returns a spec whose cluster has num_nodes 8-GPU A4 nodes."""
  nodepool = mock.create_autospec(container.BaseNodePoolConfig, instance=True)
  nodepool.num_nodes = num_nodes
  nodepool.machine_type = 'a4-highgpu-8g'
  cluster = mock.create_autospec(
      kubernetes_cluster.KubernetesCluster, instance=True
  )
  cluster.gpu_count = 8
  cluster.nodepools = {glm_bench.GPU_NODEPOOL: nodepool}
  spec = mock.create_autospec(benchmark_spec.BenchmarkSpec, instance=True)
  spec.container_cluster = cluster
  return spec


class KubernetesGlmSglangBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.write_and_apply_manifest = self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            '_WriteAndApplyManifest',
            autospec=True,
            return_value=[],
        )
    )
    self.wait_for_rollout = self.enter_context(
        mock.patch.object(kubernetes_commands, 'WaitForRollout', autospec=True)
    )

  def _PrepareAndGetManifest(self, num_nodes: int) -> list[dict[str, Any]]:
    """Runs Prepare and returns the documents of the manifest it applied."""
    glm_bench.Prepare(_MockBenchmarkSpec(num_nodes))
    self.write_and_apply_manifest.assert_called_once()
    manifest = self.write_and_apply_manifest.call_args.args[0]
    return list(yaml.safe_load_all(manifest))

  def testConfigDecodesForGke(self):
    with flagsaver.flagsaver(zone=['europe-north1-b']):
      FLAGS['zone'].present = True
      config = glm_bench.GetConfig({})
      spec = pkb_common_test_case.CreateBenchmarkSpecFromConfigDict(
          config, glm_bench.BENCHMARK_NAME
      )
    cluster_spec = spec.config.container_cluster  # pyrefly: ignore[missing-attribute]
    gpu_vm_spec = cluster_spec.nodepools[glm_bench.GPU_NODEPOOL].vm_spec
    self.assertEqual('a4-highgpu-8g', gpu_vm_spec.machine_type)
    self.assertEqual('b200', gpu_vm_spec.gpu_type)
    self.assertEqual(8, gpu_vm_spec.gpu_count)
    self.assertEqual(32, gpu_vm_spec.num_local_ssds)
    self.assertEqual('europe-north1-b', gpu_vm_spec.zone)
    self.assertEqual('e2-standard-8', cluster_spec.vm_spec.machine_type)

  def testConfigSetsProviderFlags(self):
    config = glm_bench.GetConfig({})
    self.assertEqual('latest', config['flags']['gke_gpu_driver_version'])

  @flagsaver.flagsaver(
      run_uri='123p',
      gcloud_path='gcloud',
      gce_reservation_id='nvidia-b200-res',
      gke_gpu_driver_version='latest',
  )
  def testGkeGpuNodePoolCommand(self):
    with flagsaver.flagsaver(zone=['europe-north1-b']):
      FLAGS['zone'].present = True
      spec = pkb_common_test_case.CreateBenchmarkSpecFromConfigDict(
          glm_bench.GetConfig({}), glm_bench.BENCHMARK_NAME
      )
    self.enter_context(
        mock.patch.object(
            gcp_util,
            'MakeFormattedDefaultTags',
            autospec=True,
            return_value='foo=bar',
        )
    )
    self.enter_context(
        mock.patch.object(
            gcp_util, 'GetDefaultUser', autospec=True, return_value='fakeuser'
        )
    )
    self.enter_context(
        mock.patch.object(
            gce_network.GceFirewall,
            'GetFirewall',
            autospec=True,
            return_value='firewall',
        )
    )
    self.enter_context(
        mock.patch.object(
            gce_network.GceNetwork,
            'GetNetwork',
            autospec=True,
            return_value=gce_network.GceNetwork(
                gce_network.GceNetworkSpec(
                    'fakeproject', zone='europe-north1-b'
                )
            ),
        )
    )
    issue_command = self.MockIssueCommand({'': [('', '', 0)]})
    cluster = google_kubernetes_engine.GkeCluster(
        spec.config.container_cluster  # pyrefly: ignore[missing-attribute]
    )
    cluster._CreateNodePools()
    command = issue_command.GetCommandWithSubstring(
        'node-pools create gpupool'
    )
    for expected in [
        '--machine-type a4-highgpu-8g',
        '--accelerator type=nvidia-b200,count=8,gpu-driver-version=latest',
        '--reservation nvidia-b200-res',
        '--reservation-affinity specific',
        '--ephemeral-storage-local-ssd count=32',
        '--disk-type hyperdisk-balanced',
        '--disk-size 200',
        'pkb_nodepool=gpupool',
        '--workload-metadata GKE_METADATA',
        '--enable-gvnic',
    ]:
      self.assertIn(expected, command)

  def testCheckPrerequisitesRequiresModelUri(self):
    with self.assertRaisesRegex(
        errors.Config.MissingOption, 'needs --glm_sglang_model_uri'
    ):
      glm_bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(glm_sglang_model_uri=_MODEL_URI)
  def testCheckPrerequisitesAcceptsModelUri(self):
    glm_bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(glm_sglang_model_uri=_MODEL_URI)
  def testPrepareAppliesManifest(self):
    docs = self._PrepareAndGetManifest(num_nodes=2)

    self.assertEqual(
        ['ConfigMap', 'Deployment', 'Service'], [d['kind'] for d in docs]
    )
    config_map, deployment, service = docs
    fetch_script = config_map['data']['fetch_weights.sh']
    self.assertTrue(fetch_script.startswith('#!/bin/bash'))
    self.assertIn('gcloud storage rsync', fetch_script)
    self.assertNotIn('huggingface', fetch_script)
    self.assertIn('sglang.launch_server', config_map['data']['serve.sh'])
    self.assertEqual(2, deployment['spec']['replicas'])
    pod_spec = deployment['spec']['template']['spec']
    self.assertEqual({'pkb_nodepool': 'gpupool'}, pod_spec['nodeSelector'])
    fetch = pod_spec['initContainers'][0]
    self.assertEqual('google/cloud-sdk:slim', fetch['image'])
    self.assertEqual(
        {'MODEL_URI': _MODEL_URI, 'MODEL_DIR': '/models/GLM-5.3'},
        {e['name']: e['value'] for e in fetch['env']},
    )
    server = pod_spec['containers'][0]
    self.assertEqual('8', server['resources']['limits']['nvidia.com/gpu'])
    self.assertEqual(
        '/var/lib/kubelet/pkb-models',
        pod_spec['volumes'][0]['hostPath']['path'],
    )
    self.assertEqual(30000, service['spec']['ports'][0]['port'])

  @flagsaver.flagsaver(glm_sglang_model_uri=_MODEL_URI)
  def testPrepareWaitsForRollout(self):
    glm_bench.Prepare(_MockBenchmarkSpec())

    self.wait_for_rollout.assert_called_once_with(
        'deployment/glm-sglang', timeout=2 * 3600
    )

  @flagsaver.flagsaver(
      glm_sglang_model_uri=_MODEL_URI,
      glm_sglang_bench_max_concurrency=flag_util.IntegerList([16, 64]),
  )
  def testRunBenchmarksEachMaxConcurrency(self):
    exec_and_parse = self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'ExecAndParseResult',
            autospec=True,
            side_effect=[None, _BENCH_RESULT, None, _BENCH_RESULT],
        )
    )

    samples = glm_bench.Run(_MockBenchmarkSpec(num_nodes=2))

    calls = exec_and_parse.call_args_list
    self.assertLen(calls, 4)
    self.assertEqual({'glm-sglang'}, {c.kwargs['pod_selector'] for c in calls})
    bench_16, cat_16, bench_64, cat_64 = [c.kwargs for c in calls]
    for expected in [
        '--base-url=http://glm-sglang:30000',
        '--tokenizer=/models/GLM-5.3',
        '--dataset-name=random-ids',
        '--max-concurrency=16',
        '--output-file=/tmp/bench_serving_16.jsonl',
    ]:
      self.assertIn(expected, bench_16['command'])
    self.assertIn('--max-concurrency=64', bench_64['command'])
    self.assertEqual(['cat', '/tmp/bench_serving_16.jsonl'], cat_16['command'])
    self.assertEqual(['cat', '/tmp/bench_serving_64.jsonl'], cat_64['command'])
    self.assertIs(glm_bench.ParseBenchServingOutput, cat_16['parse_fn'])
    per_gpu = [s for s in samples if s.metric == 'output_throughput_per_gpu']
    # 1600 tok/s over 2 x 8 GPUs.
    self.assertEqual([100.0, 100.0], [s.value for s in per_gpu])
    self.assertEqual(
        [16, 64], [s.metadata['bench_max_concurrency'] for s in per_gpu]
    )
    self.assertEqual(2, per_gpu[0].metadata['replicas'])
    self.assertEqual(_MODEL_URI, per_gpu[0].metadata['model_uri'])
    self.assertEqual(
        'a4-highgpu-8g', per_gpu[0].metadata['gpu_machine_type']
    )

  def testMakeSamplesSkipsEchoedArgumentsAndNonNumbers(self):
    samples = glm_bench.MakeSamples(_BENCH_RESULT, {}, num_gpus=16)

    self.assertCountEqual(
        [
            'duration',
            'completed',
            'request_throughput',
            'output_throughput',
            'total_throughput',
            'mean_ttft_ms',
            'median_tpot_ms',
            'p99_itl_ms',
            'accept_length',
            'max_output_tokens_per_s',
            'output_throughput_per_gpu',
        ],
        [s.metric for s in samples],
    )

  def testMakeSamplesSkipsBoolsAndNonFiniteValues(self):
    samples = glm_bench.MakeSamples(
        {
            'duration': 2.0,
            'flag': True,
            'mean_ttft_ms': float('nan'),
            'p99_itl_ms': float('inf'),
            'output_throughput': float('nan'),
        },
        {},
        num_gpus=8,
    )

    self.assertEqual(['duration'], [s.metric for s in samples])

  def testMakeSamplesSetsUnits(self):
    samples = glm_bench.MakeSamples(
        {
            'mean_ttft_ms': 1.0,
            'duration': 2.0,
            'request_throughput': 3.0,
            'output_throughput': 4.0,
            'max_output_tokens_per_s': 5.0,
            'completed': 6,
        },
        {},
        num_gpus=0,
    )

    self.assertEqual(
        {
            'mean_ttft_ms': 'ms',
            'duration': 's',
            'request_throughput': 'requests/s',
            'output_throughput': 'tokens/s',
            'max_output_tokens_per_s': 'tokens/s',
            'completed': '',
        },
        {s.metric: s.unit for s in samples},
    )

  def testParseBenchServingOutputReturnsLastJsonLine(self):
    stdout = '{"completed": 1}\n{"completed": 2}\n  \n'

    self.assertEqual(
        {'completed': 2}, glm_bench.ParseBenchServingOutput(stdout)
    )

  def testParseBenchServingOutputRejectsEmptyOutput(self):
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError, 'Empty sglang.bench_serving output'
    ):
      glm_bench.ParseBenchServingOutput('\n')


if __name__ == '__main__':
  unittest.main()
