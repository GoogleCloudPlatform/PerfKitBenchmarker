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

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import dynamo_sglang_benchmark_helper
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case


class PrepareTest(pkb_common_test_case.PkbCommonTestCase):

  def testPrepareInstallsChartsCreatesNamespaceAndAppliesManifest(self):
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
    dynamo_sglang_benchmark_helper.FLAGS.dynamo_hf_token = 'fake-token'

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
