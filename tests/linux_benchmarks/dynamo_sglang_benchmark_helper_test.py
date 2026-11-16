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
"""Dynamo SGLang benchmark helper test suite."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import dynamo_sglang_benchmark_helper
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case


class OverridesAndValidationTest(pkb_common_test_case.PkbCommonTestCase):

  def testApplyValuesOverridesPopulatesDefaults(self):
    ctx = dynamo_sglang_benchmark_helper.ApplyValuesOverrides(
        {'gpu_count': 4}, {}
    )
    self.assertEqual(ctx['decode_tp'], 4)
    self.assertEqual(ctx['prefill_tp'], 4)
    self.assertEqual(ctx['pod_placement'], 'cross_node')
    self.assertEqual(ctx['model_path_prefix'], '/data/model')
    self.assertIsNone(ctx['disaggregation_transfer_backend'])
    self.assertEqual(ctx['benchmark']['num_prompts'], 10240)
    self.assertIsNone(ctx['benchmark']['request_rate'])

  def testApplyValuesOverridesReadsCustomValues(self):
    ctx = dynamo_sglang_benchmark_helper.ApplyValuesOverrides(
        {'gpu_count': 8},
        {
            'dynamo': {
                'podPlacement': 'colocated',
                'disaggregationTransferBackend': 'nixl',
                'frontend': {'routerMode': 'kv'},
                'decodeWorker': {'replicas': 2, 'tp': 4, 'extraArgs': ['--x']},
                'prefillWorker': {'replicas': 3, 'tp': 2},
            },
            'volumes': {'useGcs': False},
            'benchmark': {'numPrompts': 128, 'requestRate': 4.5},
        },
    )
    self.assertEqual(ctx['decode_replicas'], 2)
    self.assertEqual(ctx['prefill_replicas'], 3)
    self.assertEqual(ctx['decode_tp'], 4)
    self.assertEqual(ctx['prefill_tp'], 2)
    self.assertEqual(ctx['pod_placement'], 'colocated')
    self.assertEqual(ctx['disaggregation_transfer_backend'], 'nixl')
    self.assertEqual(ctx['router_mode'], 'kv')
    self.assertEqual(ctx['decode_extra_args'], ['--x'])
    self.assertEqual(ctx['model_path_prefix'], '')
    self.assertEqual(ctx['benchmark']['num_prompts'], 128)
    self.assertEqual(ctx['benchmark']['request_rate'], 4.5)

  def testApplyValuesOverridesInvalidPlacementRaises(self):
    with self.assertRaises(errors.Config.InvalidValue):
      dynamo_sglang_benchmark_helper.ApplyValuesOverrides(
          {'gpu_count': 4}, {'dynamo': {'podPlacement': 'rack_local'}}
      )

  def testApplyValuesOverridesPrefersNodeCountOverDecodeReplicas(self):
    ctx = dynamo_sglang_benchmark_helper.ApplyValuesOverrides(
        {'gpu_count': 4},
        {'dynamo': {'decodeWorker': {'nodeCount': 8, 'replicas': 1}}},
    )
    self.assertEqual(ctx['decode_replicas'], 8)
    self.assertEqual(ctx['worker_replicas'], 8)
    self.assertEqual(ctx['prefill_replicas'], 8)

  def testApplyValuesOverridesDefaultsPrefillReplicasFromDecodeReplicas(self):
    ctx = dynamo_sglang_benchmark_helper.ApplyValuesOverrides(
        {'gpu_count': None},
        {'dynamo': {'decodeWorker': {'replicas': 3}}},
    )
    self.assertEqual(ctx['decode_replicas'], 3)
    self.assertEqual(ctx['prefill_replicas'], 3)
    self.assertEqual(ctx['decode_tp'], 4)
    self.assertEqual(ctx['prefill_tp'], 4)

  def testBuildRunMetadataIncludesReplicasAndBenchmarkOverrides(self):
    ctx = dynamo_sglang_benchmark_helper.ApplyValuesOverrides(
        {'gpu_count': 4, 'dynamo_model_id': 'fake/model'},
        {
            'dynamo': {
                'decodeWorker': {'replicas': 2},
                'prefillWorker': {'replicas': 3},
            },
            'benchmark': {'numPrompts': 64, 'requestRate': 8.0},
        },
    )
    metadata = dynamo_sglang_benchmark_helper.BuildRunMetadata(ctx, 'latency')
    self.assertEqual(metadata['decode_replicas'], 2)
    self.assertEqual(metadata['prefill_replicas'], 3)
    self.assertEqual(metadata['num_prompts'], 64)
    self.assertEqual(metadata['input_len'], 8192)
    self.assertEqual(metadata['output_len'], 1024)
    self.assertEqual(metadata['max_concurrency'], 2048)
    self.assertEqual(metadata['request_rate'], 8.0)


class PrepareTest(pkb_common_test_case.PkbCommonTestCase):

  def testPrepareInstallsChartsCreatesNamespaceAndAppliesManifest(self):
    install_charts = self.enter_context(
        mock.patch.object(helm, 'InstallCharts')
    )
    run_kubectl_command = self.enter_context(
        mock.patch.object(
            kubernetes_commands.kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('', 'NotFound', 1),  # operator deployment check -> install
                ('', '', 0),  # set image
                ('', '', 0),  # wait
                ('', '', 0),  # create namespace
            ],
        )
    )
    apply_manifest = self.enter_context(
        mock.patch.object(kubernetes_commands, 'ApplyManifest')
    )
    wait_for_pods_ready = self.enter_context(
        mock.patch.object(kubernetes_commands, 'WaitForPodsReady')
    )

    context = {'namespace': 'dynamo-cloud'}
    dynamo_sglang_benchmark_helper.Prepare(
        context, 'dynamo/sglang_p2p/dynamo_graph.yaml.j2'
    )

    install_charts.assert_called_once()
    self.assertTrue(
        any(
            call.args[0] == ['create', 'namespace', 'dynamo-cloud']
            for call in run_kubectl_command.call_args_list
        )
    )
    apply_manifest.assert_called_once_with(
        'dynamo/sglang_p2p/dynamo_graph.yaml.j2', **context
    )
    wait_for_pods_ready.assert_called_once_with('dynamo-cloud', timeout=900)

  def testPrepareSkipsHelmInstallWhenOperatorDeploymentExists(self):
    install_charts = self.enter_context(
        mock.patch.object(helm, 'InstallCharts')
    )
    run_kubectl_command = self.enter_context(
        mock.patch.object(
            kubernetes_commands.kubectl,
            'RunKubectlCommand',
            return_value=('', '', 0),
        )
    )
    self.enter_context(mock.patch.object(kubernetes_commands, 'ApplyManifest'))
    self.enter_context(
        mock.patch.object(kubernetes_commands, 'WaitForPodsReady')
    )

    dynamo_sglang_benchmark_helper.Prepare(
        {'namespace': 'dynamo-cloud'},
        'dynamo/sglang_p2p/dynamo_graph.yaml.j2',
    )

    install_charts.assert_not_called()
    self.assertTrue(
        any(
            call.args[0][:2] == ['set', 'image']
            for call in run_kubectl_command.call_args_list
        )
    )

  @flagsaver.flagsaver(dynamo_hf_token='fake-token')
  def testPrepareCreatesHfTokenSecretWhenTokenFlagSet(self):
    self.enter_context(mock.patch.object(helm, 'InstallCharts'))
    run_kubectl_command = self.enter_context(
        mock.patch.object(
            kubernetes_commands.kubectl,
            'RunKubectlCommand',
            return_value=('', '', 0),
        )
    )
    self.enter_context(mock.patch.object(kubernetes_commands, 'ApplyManifest'))
    self.enter_context(
        mock.patch.object(kubernetes_commands, 'WaitForPodsReady')
    )

    context = {'namespace': 'dynamo-cloud'}
    dynamo_sglang_benchmark_helper.Prepare(
        context, 'dynamo/sglang_p2p/dynamo_graph.yaml.j2'
    )

    create_secret_calls = [
        call.args[0]
        for call in run_kubectl_command.call_args_list
        if call.args[0][:3] == ['create', 'secret', 'generic']
    ]
    self.assertEqual(len(create_secret_calls), 1)
    self.assertIn('--from-literal=HF_TOKEN=fake-token', create_secret_calls[0])

  def testPrepareIgnoresAlreadyExistsNamespaceError(self):
    self.enter_context(mock.patch.object(helm, 'InstallCharts'))
    self.enter_context(
        mock.patch.object(
            kubernetes_commands.kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('', 'NotFound', 1),
                ('', '', 0),
                ('', '', 0),
                ('', 'Error from server (AlreadyExists): already exists', 1),
            ],
        )
    )
    self.enter_context(mock.patch.object(kubernetes_commands, 'ApplyManifest'))
    self.enter_context(
        mock.patch.object(kubernetes_commands, 'WaitForPodsReady')
    )

    dynamo_sglang_benchmark_helper.Prepare(
        {'namespace': 'dynamo-cloud'},
        'dynamo/sglang_p2p/dynamo_graph.yaml.j2',
    )

  def testPrepareRaisesOnUnexpectedNamespaceCreationError(self):
    self.enter_context(mock.patch.object(helm, 'InstallCharts'))
    self.enter_context(
        mock.patch.object(
            kubernetes_commands.kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('', 'NotFound', 1),
                ('', '', 0),
                ('', '', 0),
                ('', 'Error from server (Forbidden): forbidden', 1),
            ],
        )
    )

    with self.assertRaises(errors.VmUtil.IssueCommandError):
      dynamo_sglang_benchmark_helper.Prepare(
          {'namespace': 'dynamo-cloud'},
          'dynamo/sglang_p2p/dynamo_graph.yaml.j2',
      )

  def testPrepareRaisesOnUnexpectedOperatorCheckError(self):
    install_charts = self.enter_context(
        mock.patch.object(helm, 'InstallCharts')
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands.kubectl,
            'RunKubectlCommand',
            return_value=('', 'Unable to connect to the server', 1),
        )
    )

    with self.assertRaises(errors.VmUtil.IssueCommandError):
      dynamo_sglang_benchmark_helper.Prepare(
          {'namespace': 'dynamo-cloud'},
          'dynamo/sglang_p2p/dynamo_graph.yaml.j2',
      )
    install_charts.assert_not_called()


class RunTest(pkb_common_test_case.PkbCommonTestCase):

  def testRunTriggersBenchServingAndParsesSamples(self):
    exec_and_parse_result = self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'ExecAndParseResult',
            side_effect=[
                None,
                {'mean_ttft_ms': 123.4, 'output_throughput': 500.0},
            ],
        )
    )

    context = {'namespace': 'dynamo-cloud', 'dynamo_model_id': 'fake/model'}
    samples = dynamo_sglang_benchmark_helper.Run(context)

    self.assertEqual(exec_and_parse_result.call_count, 2)
    sample_metrics = {s.metric: s.value for s in samples}
    self.assertEqual(sample_metrics['mean_ttft_ms'], 123.4)
    self.assertEqual(sample_metrics['output_throughput'], 500.0)
    self.assertEqual(samples[0].metadata, {})

  def testRunAttachesMetadataFromMetadataFn(self):
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'ExecAndParseResult',
            side_effect=[None, {'mean_ttft_ms': 456.0}],
        )
    )

    context = {'namespace': 'dynamo-cloud', 'dynamo_model_id': 'fake/model'}
    samples = dynamo_sglang_benchmark_helper.Run(
        context, metadata_fn=lambda: {'profile': 'throughput'}
    )

    self.assertEqual(samples[0].metadata, {'profile': 'throughput'})


if __name__ == '__main__':
  unittest.main()
