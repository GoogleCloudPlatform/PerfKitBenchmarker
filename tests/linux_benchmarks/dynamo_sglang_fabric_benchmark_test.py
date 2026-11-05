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
"""Dynamo SGLang fabric benchmark test suite."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import dynamo_sglang_fabric_benchmark
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case
import yaml

_FAKE_VALUES_YAML = """
dynamo:
  namespace: dynamo-cloud
  deploymentName: dynamo-disagg1p1d
  computeDomain:
    name: a4x-domain
    numNodes: 2
    resourceClaimTemplateName: a4x-channel
  frontend:
    replicas: 1
  decodeWorker:
    nodeCount: 1
    envs:
      - name: SGLANG_MOONCAKE_CUSTOM_MEM_POOL
        value: "True"
      - name: HF_TOKEN
        valueFrom:
          secretKeyRef:
            name: hf-token-secret
            key: HF_TOKEN
volumes:
  gcsfuse:
    bucketName: fake-bucket
"""


class DynamoSglangFabricBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetConfigDefaultsToA4xBlueprintAndMachineType(self):
    config = dynamo_sglang_fabric_benchmark.GetConfig({})
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['machine_type'],
        'a4x-highgpu-4g',
    )
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['gpu_type'],
        'nvidia-gb200',
    )
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['gpu_count'], 4
    )
    self.assertEqual(
        config['flags']['cluster_template_file'],
        'cluster/gke/dynamo_sglang_fabric.yaml.j2',
    )

  def testBuildValuesContextIncludesComputeDomain(self):
    fake_cluster = mock.Mock(gpu_count=4)
    with mock.patch(
        'builtins.open', mock.mock_open(read_data=_FAKE_VALUES_YAML)
    ):
      context = dynamo_sglang_fabric_benchmark._BuildValuesContext(fake_cluster)

    self.assertEqual(context['compute_domain_name'], 'a4x-domain')
    self.assertEqual(context['compute_domain_num_nodes'], 2)
    self.assertEqual(context['compute_domain_channel'], 'a4x-channel')
    self.assertEqual(
        context['env_vars'], {'SGLANG_MOONCAKE_CUSTOM_MEM_POOL': 'True'}
    )

  @flagsaver.flagsaver(dynamo_sglang_fabric_workload_profile='throughput')
  def testRunParsesBenchServingOutputIntoSamples(self):
    fake_cluster = mock.Mock(gpu_count=4)
    benchmark_spec = mock.Mock(container_cluster=fake_cluster)
    with mock.patch(
        'builtins.open', mock.mock_open(read_data=_FAKE_VALUES_YAML)
    ):
      with mock.patch.object(
          kubernetes_commands,
          'ExecAndParseResult',
          side_effect=[None, {'mean_ttft_ms': 456.0}],
      ) as exec_and_parse:
        samples = dynamo_sglang_fabric_benchmark.Run(benchmark_spec)

    self.assertEqual(exec_and_parse.call_count, 2)
    sample_metrics = {s.metric: s.value for s in samples}
    self.assertEqual(sample_metrics['mean_ttft_ms'], 456.0)
    self.assertEqual(
        samples[0].metadata,
        {
            'profile': 'throughput',
            'model_id': 'deepseek-ai/DeepSeek-R1',
            'dynamo_release_version': '0.7.0',
            'decode_replicas': 1,
            'prefill_replicas': 1,
            'decode_tp': 4,
            'prefill_tp': 4,
            'pod_placement': 'unconstrained',
            'num_prompts': 10240,
            'input_len': 8192,
            'output_len': 1024,
            'max_concurrency': 2048,
        },
    )

  @flagsaver.flagsaver(dynamo_sglang_fabric_workload_profile='throughput')
  def testDeepEpValuesPreservesEightDecodeReplicas(self):
    context = dynamo_sglang_fabric_benchmark._BuildValuesContext(
        mock.Mock(gpu_count=4)
    )
    self.assertEqual(context['decode_replicas'], 8)
    self.assertEqual(context['prefill_replicas'], 5)

  def testRenderFabricTemplateSupportsOverridesAndOptionalComputeDomain(self):
    base = yaml.safe_load(_FAKE_VALUES_YAML)
    base['dynamo']['podPlacement'] = 'cross_node'
    base['dynamo']['disaggregationTransferBackend'] = 'nixl'
    base['dynamo']['frontend']['routerMode'] = 'kv'
    base['dynamo']['decodeWorker']['tp'] = 2
    base['dynamo']['prefillWorker'] = {'replicas': 2, 'tp': 2}
    with mock.patch(
        'builtins.open', mock.mock_open(read_data=yaml.safe_dump(base))
    ):
      context = dynamo_sglang_fabric_benchmark._BuildValuesContext(
          mock.Mock(gpu_count=4)
      )

    docs = list(
        yaml.safe_load_all(
            vm_util.ReadAndRenderJinja2Template(
                'dynamo/sglang_fabric/dynamo_graph.yaml.j2',
                trim_spaces=False,
                **context,
            )
        )
    )
    self.assertLen(docs, 2)
    self.assertEqual(docs[0]['kind'], 'ComputeDomain')
    decode_spec = docs[1]['spec']['services']['decode']
    self.assertEqual(decode_spec['resources']['limits']['gpu'], '2')
    self.assertIn('podAntiAffinity', decode_spec['extraPodSpec']['affinity'])

    context['enable_compute_domain'] = False
    docs_no_cd = list(
        yaml.safe_load_all(
            vm_util.ReadAndRenderJinja2Template(
                'dynamo/sglang_fabric/dynamo_graph.yaml.j2',
                trim_spaces=False,
                **context,
            )
        )
    )
    self.assertLen(docs_no_cd, 1)
    self.assertEqual(docs_no_cd[0]['kind'], 'DynamoGraphDeployment')
    self.assertNotIn(
        'resourceClaims',
        docs_no_cd[0]['spec']['services']['decode']['extraPodSpec'],
    )


if __name__ == '__main__':
  unittest.main()
