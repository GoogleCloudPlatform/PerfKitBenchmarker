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

from perfkitbenchmarker.linux_benchmarks import dynamo_sglang_p2p_benchmark
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case

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
            'model_id': 'Qwen/Qwen2.5-32B-Instruct',
            'dynamo_release_version': '0.7.0',
        },
    )


if __name__ == '__main__':
  unittest.main()
