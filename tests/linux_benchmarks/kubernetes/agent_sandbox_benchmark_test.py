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

import json
from unittest import mock

from absl import flags
from absl.testing import absltest
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks.kubernetes import agent_sandbox_benchmark
from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox

FLAGS = flags.FLAGS


class AgentSandboxBenchmarkTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_kubectl = self.enterContext(
        mock.patch.object(
            agent_sandbox_benchmark.kubectl, 'RunKubectlCommand', autospec=True
        )
    )
    self.mock_apply_yaml = self.enterContext(
        mock.patch.object(kubernetes_commands, 'ApplyYaml', autospec=True)
    )
    self.mock_convert = self.enterContext(
        mock.patch.object(
            kubernetes_commands, 'ConvertManifestToYamlDicts', autospec=True
        )
    )
    self.mock_wait = self.enterContext(
        mock.patch.object(
            kubernetes_commands,
            'WaitForResourceForMultiConditions',
            autospec=True,
        )
    )

    self.mock_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    self.mock_sandbox = mock.MagicMock()
    self.mock_sandbox.spec.namespace = 'default'
    self.mock_sandbox.spec.sandbox_warmpool.replicas = 0
    self.mock_sandbox.spec.sandbox_template.runtime_class = 'gvisor-sandbox'
    self.mock_sandbox.GetResourceMetadata.return_value = {
        'agent_sandbox_manifest_ref': 'v1.0.4',
    }
    self.mock_spec.agent_sandbox = self.mock_sandbox
    self.mock_spec.container_cluster = mock.MagicMock()
    self.mock_spec.container_cluster.GetResourceMetadata.return_value = {}

    # Setup default outputs for logs mock
    self.mock_wait.return_value = 'condition=Complete'
    self.mock_kubectl.side_effect = [
        ('', '', 0),  # Delete job
        ('pod-123', '', 0),  # Get pod
        (
            '---RESULTS---\n'
            + json.dumps({'summary': {'peak_concurrency': 5}}),
            '',
            0,
        ),  # Logs
    ]

  def testRun_ModifiesYamlAndApplies(self):
    # Build a mock yaml parsing logic to return a template (resembling
    # what read-modify-apply expects)
    mock_yaml_doc = {'spec': {'template': {'spec': {'containers': [{}]}}}}
    self.mock_convert.return_value = [mock_yaml_doc]
    self.mock_apply_yaml.return_value = ['job.batch/agent-sandbox-load-runner']

    # Set some flags for the run
    FLAGS.agent_sandbox_benchmark_target_qps = 5.0
    FLAGS.agent_sandbox_benchmark_target_duration = 30.0

    samples = agent_sandbox_benchmark.Run(self.mock_spec)

    self.mock_convert.assert_called_once_with(
        'agent_sandbox/load_runner_job.yaml.j2',
        namespace='default',
    )

    # Assert ApplyYaml was called with the modified dictionary
    self.mock_apply_yaml.assert_called_once()
    applied_docs = self.mock_apply_yaml.call_args[0][0]
    self.assertLen(applied_docs, 1)

    script_arg = applied_docs[0]['spec']['template']['spec']['containers'][0][
        'args'
    ][0]
    self.assertIn('python /opt/load-runner/load_runner.py', script_arg)
    self.assertIn('--namespace default', script_arg)
    self.assertIn('--qps 5.0', script_arg)
    self.assertIn('--duration 30.0', script_arg)
    self.assertNotEmpty(samples)
    self.assertEqual(
        samples[0].metadata['agent_sandbox_manifest_ref'], 'v1.0.4'
    )
    self.assertIn('workload_duration', samples[0].metadata)

  def testRun_MultiPodParallelismAndControlPlaneMetrics(self):
    mock_yaml_doc = {'spec': {'template': {'spec': {'containers': [{}]}}}}
    self.mock_convert.return_value = [mock_yaml_doc]
    self.mock_apply_yaml.return_value = ['job.batch/agent-sandbox-load-runner']
    self.mock_sandbox.ScrapeControlPlaneMetrics.side_effect = [
        {'gomaxprocs': 4.0, 'process_start_time': 1000.0},
        {'gomaxprocs': 4.0, 'process_start_time': 1000.0},
    ]
    rec0 = json.dumps({
        'name': 'claim-pod1-0',
        'requested_at': 10.0,
        'ready_at': 10.2,
        'warm_served': True,
    })
    rec1 = json.dumps({
        'name': 'claim-pod2-0',
        'requested_at': 10.1,
        'ready_at': 10.4,
        'warm_served': True,
    })
    self.mock_kubectl.side_effect = [
        ('', '', 0),  # Delete job
        ('pod-1 pod-2', '', 0),  # Get pods
        (
            'pip log\n---RESULTS---\n'
            + rec0
            + '\n'
            + json.dumps({'summary': {'peak_concurrency': 5}}),
            '',
            0,
        ),
        (
            'pip log\n---RESULTS---\n'
            + rec1
            + '\n'
            + json.dumps({'summary': {'peak_concurrency': 7}}),
            '',
            0,
        ),
    ]

    FLAGS.agent_sandbox_benchmark_target_qps = 100.0
    FLAGS.agent_sandbox_benchmark_target_duration = 60.0
    FLAGS.agent_sandbox_load_runner_parallelism = 2
    try:
      samples = agent_sandbox_benchmark.Run(self.mock_spec)
    finally:
      FLAGS.agent_sandbox_load_runner_parallelism = 1

    applied_doc = self.mock_apply_yaml.call_args[0][0][0]
    self.assertEqual(applied_doc['spec']['parallelism'], 2)
    self.assertEqual(applied_doc['spec']['completions'], 2)
    container = applied_doc['spec']['template']['spec']['containers'][0]
    self.assertEqual(
        container['resources'],
        {
            'requests': {'cpu': '1', 'memory': '2Gi'},
            'limits': {'cpu': '8', 'memory': '8Gi'},
        },
    )
    script_arg = container['args'][0]
    self.assertIn('--qps 50.0', script_arg)
    self.assertIn('--claim-prefix "claim-${HOSTNAME:-0}"', script_arg)

    by_name = {s.metric: s for s in samples}
    self.assertEqual(by_name['peak_concurrency'].value, 12)
    self.assertEqual(by_name['success_count'].value, 2)
    self.assertEqual(samples[0].metadata['cp_gomaxprocs_pre'], 4.0)
    self.assertEqual(samples[0].metadata['cp_gomaxprocs_post'], 4.0)
    self.assertFalse(samples[0].metadata['cp_resized_mid_run'])

  def testRun_FailedJobWithoutPodChecksQuota(self):
    mock_yaml_doc = {'spec': {'template': {'spec': {'containers': [{}]}}}}
    self.mock_convert.return_value = [mock_yaml_doc]
    self.mock_wait.return_value = 'condition=Failed'
    self.mock_kubectl.side_effect = [
        ('', '', 0),  # Delete job
        ('', '', 1),  # Get pod returns empty when unscheduled
    ]

    with self.assertRaises(errors.Benchmarks.RunError):
      agent_sandbox_benchmark.Run(self.mock_spec)

    self.mock_spec.container_cluster.event_poller.CheckForQuotaFailure.assert_called_once()

  def testPrepare_MissingAgentSandboxRaises(self):
    self.mock_spec.agent_sandbox = None
    with self.assertRaises(errors.Benchmarks.PrepareException):
      agent_sandbox_benchmark.Prepare(self.mock_spec)

  def testPrepare_InstallsWorkloadAndAppliesRbac(self):
    mock_apply_manifest = self.enterContext(
        mock.patch.object(kubernetes_commands, 'ApplyManifest', autospec=True)
    )
    with mock.patch('builtins.open', mock.mock_open(read_data='print(1)')):
      agent_sandbox_benchmark.Prepare(self.mock_spec)

    self.mock_sandbox.InstallWorkload.assert_called_once()
    self.mock_apply_yaml.assert_called_once()
    applied_cm = self.mock_apply_yaml.call_args[0][0][0]
    self.assertEqual(applied_cm['kind'], 'ConfigMap')
    self.assertEqual(applied_cm['data']['load_runner.py'], 'print(1)')
    mock_apply_manifest.assert_called_once_with(
        'agent_sandbox/load_runner_rbac.yaml.j2',
        namespace='default',
    )


class AgentSandboxBenchmarkConfigTest(absltest.TestCase):

  def testConfigBuildsK8sAgentSandbox(self):
    config = configs.LoadConfig(
        agent_sandbox_benchmark.BENCHMARK_CONFIG,
        {},
        agent_sandbox_benchmark.BENCHMARK_NAME,
    )
    agent_sandbox_dict = config['agent_sandbox']
    sandbox_spec = agent_sandbox_spec.AgentSandboxConfigDecoder(
        option='agent_sandbox'
    ).Decode(agent_sandbox_dict, 'test', FLAGS)
    sandbox = agent_sandbox.GetAgentSandbox(sandbox_spec, mock.Mock())
    self.assertIsInstance(sandbox, k8s_agent_sandbox.K8sAgentSandbox)

  def testConfigIncludesAzureVmSpecs(self):
    config = configs.LoadConfig(
        agent_sandbox_benchmark.BENCHMARK_CONFIG,
        {},
        agent_sandbox_benchmark.BENCHMARK_NAME,
    )
    cluster_cfg = config['container_cluster']
    self.assertEqual(
        cluster_cfg['vm_spec']['Azure']['machine_type'], 'Standard_D4s_v5'
    )
    self.assertEqual(
        cluster_cfg['nodepools']['sandbox']['vm_spec']['Azure']['machine_type'],
        'Standard_D16s_v5',
    )


if __name__ == '__main__':
  absltest.main()
