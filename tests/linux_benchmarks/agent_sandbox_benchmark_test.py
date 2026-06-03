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
"""Tests for perfkitbenchmarker.linux_benchmarks.agent_sandbox_benchmark."""

import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized

from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import agent_sandbox_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GetConfigTest(pkb_common_test_case.PkbCommonTestCase):

  def testBenchmarkNameConstant(self):
    self.assertEqual(agent_sandbox_benchmark.BENCHMARK_NAME, 'agent_sandbox')

  @parameterized.named_parameters(
      ('Gcp', 'GCP', 'c4-standard-4', 'c4-standard-16'),
      ('Aws', 'AWS', 'm8i.xlarge', 'm8i.4xlarge'),
  )
  def testGetConfigSelectsCloudAndDefaultMachineTypes(
      self, cloud, general_machine_type, sandbox_machine_type
  ):
    """Cloud selection drives the vm_spec block and per-cloud default types."""
    with flagsaver.flagsaver(cloud=cloud):
      config = agent_sandbox_benchmark.GetConfig({})
    cluster = config['container_cluster']
    self.assertEqual(cluster['cloud'], cloud)
    self.assertEqual(cluster['type'], 'Kubernetes')
    self.assertIn('sandbox', cluster['nodepools'])
    self.assertEqual(
        cluster['vm_spec'][cloud]['machine_type'], general_machine_type
    )
    self.assertEqual(
        cluster['nodepools']['sandbox']['vm_spec'][cloud]['machine_type'],
        sandbox_machine_type,
    )

  @parameterized.named_parameters(
      ('Gcp', 'GCP', 'n2-standard-16', 'n2-standard-32'),
      ('Aws', 'AWS', 'm8i.2xlarge', 'm8i.8xlarge'),
  )
  def testGetConfigAppliesPoolFlags(
      self, cloud, general_machine_type, sandbox_machine_type
  ):
    """Sizing and machine-type flags override the selected cloud's vm_spec."""
    with flagsaver.flagsaver(
        cloud=cloud,
        agent_sandbox_general_pool_machine_type=general_machine_type,
        agent_sandbox_general_pool_nodes=2,
        agent_sandbox_sandbox_pool_machine_type=sandbox_machine_type,
        agent_sandbox_sandbox_pool_nodes=5,
        agent_sandbox_sandbox_pool_max_pods_per_node=64,
    ):
      config = agent_sandbox_benchmark.GetConfig({})
    cluster = config['container_cluster']
    self.assertEqual(cluster['vm_count'], 2)
    self.assertEqual(
        cluster['vm_spec'][cloud]['machine_type'], general_machine_type
    )
    sandbox = cluster['nodepools']['sandbox']
    self.assertEqual(sandbox['vm_count'], 5)
    self.assertEqual(
        sandbox['vm_spec'][cloud]['machine_type'], sandbox_machine_type
    )
    self.assertEqual(sandbox['max_pods_per_node'], 64)


class ConfigDecodeTest(pkb_common_test_case.PkbCommonTestCase):
  """Exercises full spec decode so unrecognized nodepool keys raise at test time.

  The GetConfigTest class only inspects the raw dict returned by
  configs.LoadConfig. That dict is never validated against the NodepoolSpec
  decoder, so keys like node_labels/node_taints silently survive. This class
  runs the same BenchmarkConfigSpec decode that PKB executes at startup, so any
  unrecognized key raises errors.Config.UnrecognizedOption and the test fails.
  """

  def testBenchmarkConfigDecodesWithoutError(self):
    config = agent_sandbox_benchmark.GetConfig({})
    # This is exactly the call PKB makes during benchmark setup. It must not
    # raise errors.Config.UnrecognizedOption.
    benchmark_config_spec.BenchmarkConfigSpec(
        agent_sandbox_benchmark.BENCHMARK_NAME,
        flag_values=FLAGS,
        **config,
    )

  def testSandboxNodepoolSpecHasSandboxConfigAndMaxPods(self):
    """Confirm decoded spec carries sandbox_config and max_pods_per_node."""
    config = agent_sandbox_benchmark.GetConfig({})
    spec = benchmark_config_spec.BenchmarkConfigSpec(
        agent_sandbox_benchmark.BENCHMARK_NAME,
        flag_values=FLAGS,
        **config,
    )
    sandbox_nodepool = spec.container_cluster.nodepools['sandbox']  # pylint: disable=no-member
    # sandbox_config defaults to None when not set in config; that is fine.
    self.assertIsNone(sandbox_nodepool.sandbox_config)
    # max_pods_per_node defaults to None when not overridden by flag.
    self.assertIsNone(sandbox_nodepool.max_pods_per_node)

  def testSandboxNodepoolSpecCarriesNodeLabelsAndTaints(self):
    """Confirm decoded spec carries the restored gVisor node_labels/node_taints."""
    config = agent_sandbox_benchmark.GetConfig({})
    spec = benchmark_config_spec.BenchmarkConfigSpec(
        agent_sandbox_benchmark.BENCHMARK_NAME,
        flag_values=FLAGS,
        **config,
    )
    sandbox_nodepool = spec.container_cluster.nodepools['sandbox']  # pylint: disable=no-member
    self.assertEqual(
        sandbox_nodepool.node_labels,
        {'sandbox.gke.io/runtime': 'runsc'},
    )
    self.assertEqual(
        sandbox_nodepool.node_taints,
        ['sandbox.gke.io/runtime=runsc:NoSchedule'],
    )


class PrepareRunTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      agent_sandbox_controller_ref='v0.4.6', agent_sandbox_warmpool_replicas=5
  )
  def testPrepareInstallsStackWithFlags(self):
    with mock.patch.object(
        agent_sandbox_benchmark.agent_sandbox, 'install_stack'
    ) as install:
      agent_sandbox_benchmark.Prepare(mock.Mock())
    install.assert_called_once()
    self.assertEqual(install.call_args.kwargs['controller_ref'], 'v0.4.6')
    self.assertEqual(install.call_args.kwargs['warmpool_replicas'], 5)

  @flagsaver.flagsaver(agent_sandbox_qps=5.0, agent_sandbox_duration=2.0)
  def testRunDrivesLoadAndReturnsSamples(self):
    fake_records = [
        _loadgen_record('claim-0', 0.0, 1.0),
    ]
    with mock.patch.object(
        agent_sandbox_benchmark.agent_sandbox_loadgen, 'ClaimDriver'
    ), mock.patch.object(
        agent_sandbox_benchmark.agent_sandbox_loadgen, 'LoadGenerator'
    ) as gen_cls:
      gen = gen_cls.return_value
      gen.run.return_value = fake_records
      gen.peak_concurrency = 1
      spec = mock.Mock()
      spec.container_cluster.GetResourceMetadata.return_value = {'k8s': 'x'}
      samples = agent_sandbox_benchmark.Run(spec)

    metrics = {s.metric for s in samples}
    self.assertIn('startup_time_p50', metrics)
    self.assertIn('success_count', metrics)


def _loadgen_record(name, requested_at, ready_at):
  from perfkitbenchmarker.linux_packages import agent_sandbox_loadgen

  rec = agent_sandbox_loadgen.ClaimRecord(name=name, requested_at=requested_at)
  rec.ready_at = ready_at
  return rec


if __name__ == '__main__':
  unittest.main()
