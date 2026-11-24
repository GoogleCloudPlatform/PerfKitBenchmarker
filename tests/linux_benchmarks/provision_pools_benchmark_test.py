import time
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.linux_benchmarks.provisioning_benchmarks import provision_pools_benchmark
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case


class ProvisionPoolsBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.cluster = mock.create_autospec(
        kubernetes_cluster.KubernetesCluster, instance=True
    )
    self.cluster.CLOUD = 'GCP'

  def _MakeSpec(self):
    b_spec = mock.create_autospec(benchmark_spec.BenchmarkSpec, instance=True)
    b_spec.container_cluster = self.cluster
    b_spec.container_specs = {
        'provisioning': mock.Mock(image='pkb_busybox'),
    }
    return b_spec

  def setUpWithXNodes(self, num_nodes: int):
    many_nodes = [f'foo{i}' for i in range(num_nodes)]
    mock_kubectl = self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            autospec=True,
            return_value=(' '.join(many_nodes), None, None),
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'ApplyManifest',
            autospec=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'GetNodeNames',
            autospec=True,
            return_value=set(many_nodes),
        )
    )
    self.cluster.GetNodePoolNames.return_value = many_nodes
    return mock_kubectl

  def test_IndividualChecks(self):
    self.setUpWithXNodes(20)
    provision_pools_benchmark._AssertNodes(10, 10)
    provision_pools_benchmark._AssertNodePools(self.cluster, 10, 10)

  @flagsaver.flagsaver(provision_pools_init_batch=0)
  @flagsaver.flagsaver(provision_pools_test_batch=10)
  def test_FullRun(self):
    mock_kubectl = self.setUpWithXNodes(10)
    samples = provision_pools_benchmark.Run(self._MakeSpec())
    metrics = [s.metric for s in samples]
    self.assertContainsSubset(
        [
            'test_batch_apply_time',
            'total_time',
            'total_time_per_node_pool',
            'test_batch_ready_time',
            'test_batch_provisioning_time',
        ],
        metrics,
    )
    poll_cmd = mock_kubectl.call_args_list[0].args[0]
    self.assertIn('jsonpath={.items[*].metadata.name}', poll_cmd)

  @flagsaver.flagsaver(provision_pools_init_batch=0)
  @flagsaver.flagsaver(provision_pools_test_batch=1)
  def test_EmptyPollDoesNotBreakEarlyAndPacesSleep(self):
    clock = [0.0]
    self.enter_context(
        mock.patch.object(
            time, 'monotonic', autospec=True, side_effect=lambda: clock[0]
        )
    )
    sleep_calls = []

    def _FakeSleep(seconds: float) -> None:
      sleep_calls.append(seconds)
      clock[0] += seconds

    self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True, side_effect=_FakeSleep)
    )
    responses = ['', 'foo0']

    def _FakeRunKubectl(command):
      del command
      clock[0] += 3.0
      return (responses.pop(0), '', 0)

    mock_kubectl = self.setUpWithXNodes(1)
    mock_kubectl.side_effect = _FakeRunKubectl
    provision_pools_benchmark.Run(self._MakeSpec())
    self.assertEqual(sleep_calls, [7.0])

  @flagsaver.flagsaver(provision_pools_init_batch=0)
  @flagsaver.flagsaver(provision_pools_test_batch=1)
  def test_SlowPollSkipsNegativeSleep(self):
    clock = [0.0]
    self.enter_context(
        mock.patch.object(
            time, 'monotonic', autospec=True, side_effect=lambda: clock[0]
        )
    )
    mock_sleep = self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True)
    )
    responses = ['', 'foo0']

    def _FakeRunKubectl(command):
      del command
      clock[0] += 12.0
      return (responses.pop(0), '', 0)

    mock_kubectl = self.setUpWithXNodes(1)
    mock_kubectl.side_effect = _FakeRunKubectl
    provision_pools_benchmark.Run(self._MakeSpec())
    mock_sleep.assert_not_called()


if __name__ == '__main__':
  unittest.main()
