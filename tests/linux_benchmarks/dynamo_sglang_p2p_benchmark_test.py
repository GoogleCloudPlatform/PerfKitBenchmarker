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
"""Dynamo SGLang p2p benchmark test suite."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import dynamo_sglang_p2p_benchmark
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case
import yaml

_FAKE_VALUES_YAML = """
dynamo:
  namespace: dynamo-cloud
  deploymentName: dynamo-disagg1p1d
  frontend:
    replicas: 1
  decodeWorker:
    nodeCount: 1
    envs:
      - name: HF_HUB_ENABLE_HF_TRANSFER
        value: "1"
      - name: HF_TOKEN
        valueFrom:
          secretKeyRef:
            name: hf-token-secret
            key: HF_TOKEN
volumes:
  gcsfuse:
    bucketName: fake-bucket
"""

_ORIGINAL_ANTI_AFFINITY = """\
        affinity:
          podAntiAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              - labelSelector:
                  matchExpressions:
                    - key: nvidia.com/dynamo-component-type
                      operator: In
                      values:
                        - worker
                topologyKey: kubernetes.io/hostname
"""
_TEMPLATE = 'dynamo/sglang_p2p/dynamo_graph.yaml.j2'
_AFFINITY_KEYS = ('podAntiAffinity', 'podAffinity')


class DynamoSglangP2PBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetConfig(self):
    config = dynamo_sglang_p2p_benchmark.GetConfig({})
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['machine_type'],
        'g4-standard-96',
    )
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['gpu_type'],
        'nvidia-rtx-pro-6000',
    )
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['gpu_count'], 4
    )
    self.assertEqual(
        config['flags']['cluster_template_file'],
        'cluster/gke/dynamo_sglang_p2p.yaml.j2',
    )

  def testBuildValuesContextReadsValuesFile(self):
    fake_cluster = mock.Mock(gpu_count=4)
    with mock.patch(
        'builtins.open', mock.mock_open(read_data=_FAKE_VALUES_YAML)
    ):
      context = dynamo_sglang_p2p_benchmark._BuildValuesContext(fake_cluster)

    self.assertEqual(context['namespace'], 'dynamo-cloud')
    self.assertEqual(context['deployment_name'], 'dynamo-disagg1p1d')
    self.assertEqual(context['gpu_count'], 4)
    self.assertEqual(context['env_vars'], {'HF_HUB_ENABLE_HF_TRANSFER': '1'})

  def testRunParsesBenchServingOutputIntoSamples(self):
    fake_cluster = mock.Mock(gpu_count=4)
    benchmark_spec = mock.Mock(container_cluster=fake_cluster)
    with mock.patch(
        'builtins.open', mock.mock_open(read_data=_FAKE_VALUES_YAML)
    ):
      with mock.patch.object(
          kubernetes_commands,
          'ExecAndParseResult',
          side_effect=[
              None,
              {'mean_ttft_ms': 123.4, 'output_throughput': 500.0},
          ],
      ) as exec_and_parse:
        samples = dynamo_sglang_p2p_benchmark.Run(benchmark_spec)

    self.assertEqual(exec_and_parse.call_count, 2)
    sample_metrics = {s.metric: s.value for s in samples}
    self.assertEqual(sample_metrics['mean_ttft_ms'], 123.4)
    self.assertEqual(sample_metrics['output_throughput'], 500.0)
    self.assertEqual(
        samples[0].metadata,
        {
            'profile': 'latency',
            'model_id': 'Qwen/Qwen2.5-32B-Instruct',
            'dynamo_release_version': '0.7.0',
            'decode_replicas': 1,
            'prefill_replicas': 1,
            'decode_tp': 4,
            'prefill_tp': 4,
            'pod_placement': 'cross_node',
            'num_prompts': 10240,
            'input_len': 8192,
            'output_len': 1024,
            'max_concurrency': 2048,
        },
    )

  def _Context(self, values_overrides=None):
    fake_cluster = mock.Mock(gpu_count=4)
    base = yaml.safe_load(_FAKE_VALUES_YAML)
    for section, updates in (values_overrides or {}).items():
      if isinstance(updates, dict) and isinstance(base.get(section), dict):
        for k, v in updates.items():
          if isinstance(v, dict) and isinstance(base[section].get(k), dict):
            base[section][k].update(v)
          else:
            base[section][k] = v
      else:
        base[section] = updates
    rendered_yaml = yaml.safe_dump(base)
    with mock.patch('builtins.open', mock.mock_open(read_data=rendered_yaml)):
      return dynamo_sglang_p2p_benchmark._BuildValuesContext(fake_cluster)

  def _Render(self, context):
    return vm_util.ReadAndRenderJinja2Template(
        _TEMPLATE, trim_spaces=False, **context
    )

  def _WorkerAffinities(self, rendered):
    workers = yaml.safe_load(rendered)['spec']['services']
    return {
        name: workers[name]['extraPodSpec'].get('affinity')
        for name in ('decode', 'prefill')
    }

  def testPlacementDefaultsToCrossNode(self):
    self.assertEqual(self._Context()['pod_placement'], 'cross_node')

  def testRenderCrossNodeUsesRequiredAntiAffinity(self):
    affinities = self._WorkerAffinities(self._Render(self._Context()))
    for affinity in affinities.values():
      self.assertEqual(list(affinity), ['podAntiAffinity'])
      term = affinity['podAntiAffinity'][
          'requiredDuringSchedulingIgnoredDuringExecution'
      ][0]
      self.assertEqual(term['topologyKey'], 'kubernetes.io/hostname')
      self.assertEqual(
          term['labelSelector']['matchExpressions'][0]['values'], ['worker']
      )

  def testRenderCrossNodeMatchesOriginalHardCodedTemplate(self):
    original = _ORIGINAL_ANTI_AFFINITY
    rendered = self._Render(self._Context())
    self.assertEqual(rendered.count(original), 2)
    self.assertNotIn('podAffinity', rendered)

  def testRenderColocatedUsesRequiredAffinity(self):
    context = self._Context({
        'dynamo': {
            'podPlacement': 'colocated',
            'decodeWorker': {'tp': 2},
            'prefillWorker': {'tp': 2},
        }
    })
    affinities = self._WorkerAffinities(self._Render(context))
    for affinity in affinities.values():
      self.assertEqual(list(affinity), ['podAffinity'])
      term = affinity['podAffinity'][
          'requiredDuringSchedulingIgnoredDuringExecution'
      ][0]
      self.assertEqual(term['topologyKey'], 'kubernetes.io/hostname')

  def testRenderUnconstrainedHasNoAffinity(self):
    context = self._Context({'dynamo': {'podPlacement': 'unconstrained'}})
    rendered = self._Render(context)
    self.assertEqual(
        self._WorkerAffinities(rendered), {'decode': None, 'prefill': None}
    )
    for key in _AFFINITY_KEYS:
      self.assertNotIn(key, rendered)

  def testRenderedManifestStaysValidYamlForEveryPlacement(self):
    for placement in ('cross_node', 'colocated', 'unconstrained'):
      rendered = self._Render(
          self._Context({
              'dynamo': {
                  'podPlacement': placement,
                  'decodeWorker': {'tp': 2},
                  'prefillWorker': {'tp': 2},
              }
          })
      )
      services = yaml.safe_load(rendered)['spec']['services']
      self.assertEqual(
          set(services), {'Frontend', 'decode', 'prefill'}, placement
      )

  def testRenderRouterModeExtraArgsAndTransferBackend(self):
    context = self._Context({
        'dynamo': {
            'disaggregationTransferBackend': 'nixl',
            'frontend': {'routerMode': 'kv'},
            'decodeWorker': {'extraArgs': ['--quantization', 'fp8']},
        },
        'volumes': {'useGcs': False},
    })
    doc = yaml.safe_load(self._Render(context))
    self.assertEqual(
        doc['spec']['envs'], [{'name': 'HF_HOME', 'value': '/tmp/model'}]
    )
    self.assertEqual(
        doc['spec']['services']['Frontend']['extraPodSpec']['mainContainer'][
            'args'
        ],
        ['--router-mode', 'kv'],
    )
    decode_args = doc['spec']['services']['decode']['extraPodSpec'][
        'mainContainer'
    ]['args']
    self.assertContainsSubsequence(
        decode_args, ['--disaggregation-transfer-backend', 'nixl']
    )
    self.assertContainsSubsequence(decode_args, ['--quantization', 'fp8'])

  def testRunMetadataAndBenchArgsReadFromValuesYaml(self):
    benchmark_spec = mock.Mock(container_cluster=mock.Mock(gpu_count=8))
    custom_values = yaml.safe_dump({
        'dynamo': {
            'namespace': 'dynamo-cloud',
            'workloadProfile': 'throughput',
            'podPlacement': 'colocated',
            'decodeWorker': {
                'nodeCount': 1,
                'tp': 4,
                'extraArgs': ['--quantization', 'fp8'],
            },
            'prefillWorker': {'replicas': 1, 'tp': 2},
        },
        'volumes': {'useGcs': False},
        'workload': {'model': 'Qwen/Qwen3-30B-A3B'},
        'benchmark': {
            'numPrompts': 256,
            'inputLen': 4096,
            'rangeRatio': 0.8,
            'outputLen': 1024,
            'maxConcurrency': 64,
        },
    })
    with mock.patch('builtins.open', mock.mock_open(read_data=custom_values)):
      with mock.patch.object(
          kubernetes_commands,
          'ExecAndParseResult',
          side_effect=[None, {'mean_ttft_ms': 1.0}],
      ) as exec_mock:
        samples = dynamo_sglang_p2p_benchmark.Run(benchmark_spec)
    bench_cmd = exec_mock.call_args_list[0].kwargs['command']
    self.assertEqual(
        bench_cmd[bench_cmd.index('--model') + 1], 'Qwen/Qwen3-30B-A3B'
    )
    self.assertEqual(bench_cmd[bench_cmd.index('--num-prompts') + 1], '256')
    self.assertEqual(
        bench_cmd[bench_cmd.index('--random-input-len') + 1], '4096'
    )
    self.assertEqual(bench_cmd[bench_cmd.index('--max-concurrency') + 1], '64')
    self.assertEqual(samples[0].metadata['profile'], 'throughput')
    self.assertEqual(samples[0].metadata['model_id'], 'Qwen/Qwen3-30B-A3B')
    self.assertEqual(samples[0].metadata['decode_replicas'], 1)
    self.assertEqual(samples[0].metadata['prefill_replicas'], 2)
    self.assertEqual(samples[0].metadata['decode_tp'], 4)
    self.assertEqual(samples[0].metadata['prefill_tp'], 2)
    self.assertEqual(samples[0].metadata['pod_placement'], 'colocated')
    self.assertEqual(samples[0].metadata['num_prompts'], 256)
    self.assertEqual(samples[0].metadata['input_len'], 4096)
    self.assertEqual(samples[0].metadata['output_len'], 1024)
    self.assertEqual(samples[0].metadata['max_concurrency'], 64)

  def testColocatedRejectsTpExceedingGpusPerNode(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, r'4 \+ 2 = 6'):
      self._Context({
          'dynamo': {
              'podPlacement': 'colocated',
              'decodeWorker': {'tp': 4},
              'prefillWorker': {'tp': 2},
          }
      })

  def testColocatedAcceptsTpFittingGpusPerNode(self):
    context = self._Context({
        'dynamo': {
            'podPlacement': 'colocated',
            'decodeWorker': {'tp': 2},
            'prefillWorker': {'tp': 2},
        }
    })
    self.assertEqual(context['pod_placement'], 'colocated')

  def testCrossNodeDoesNotCheckGpuFit(self):
    context = self._Context(
        {
            'dynamo': {
                'decodeWorker': {'tp': 4},
                'prefillWorker': {'tp': 4},
            }
        }
    )
    self.assertEqual(context['pod_placement'], 'cross_node')

  def testColocatedSkipsCheckWhenGpuCountUnknown(self):
    fake_cluster = mock.Mock(gpu_count=None)
    custom = yaml.safe_dump({
        'dynamo': {
            'podPlacement': 'colocated',
            'decodeWorker': {'tp': 4},
            'prefillWorker': {'tp': 4},
        }
    })
    with mock.patch('builtins.open', mock.mock_open(read_data=custom)):
      context = dynamo_sglang_p2p_benchmark._BuildValuesContext(fake_cluster)
    self.assertEqual(context['pod_placement'], 'colocated')

  def testCrossNodeRejectsSinglePodTpExceedingGpusPerNode(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, r'decodeWorker\.tp=8 exceeds'
    ):
      self._Context({'dynamo': {'decodeWorker': {'tp': 8}}})

  def testColocatedRejectsMultiReplicaExceedingGpusPerNode(self):
    with self.assertRaisesRegex(errors.Config.InvalidValue, r'2 \+ 4 = 6'):
      self._Context({
          'dynamo': {
              'podPlacement': 'colocated',
              'decodeWorker': {'tp': 2},
              'prefillWorker': {'replicas': 2, 'tp': 2},
          }
      })

  def testThroughputProfileWithG4ValuesPreservesTwoPrefillReplicas(self):
    with open(data.ResourcePath('dynamo/dynamo_values_g4.yaml')) as f:
      g4_values = yaml.safe_load(f)
    g4_values['dynamo']['workloadProfile'] = 'throughput'
    rendered = yaml.safe_dump(g4_values)
    with mock.patch('builtins.open', mock.mock_open(read_data=rendered)):
      context = dynamo_sglang_p2p_benchmark._BuildValuesContext(
          mock.Mock(gpu_count=4)
      )
    self.assertEqual(context['deployment_name'], 'dynamo-disagg2p1d')
    self.assertEqual(context['prefill_replicas'], 2)

  @flagsaver.flagsaver(dynamo_sglang_p2p_model_id='custom/cli-model')
  def testModelIdFlagOverridesValuesYamlWorkloadModel(self):
    context = self._Context({'workload': {'model': 'custom/yaml-model'}})
    self.assertEqual(context['dynamo_model_id'], 'custom/cli-model')


if __name__ == '__main__':
  unittest.main()
