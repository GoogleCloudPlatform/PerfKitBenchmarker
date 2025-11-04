import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import container_service
from perfkitbenchmarker.linux_benchmarks.provisioning_benchmarks import provision_node_pools_benchmark
from tests import pkb_common_test_case


class ProvisionNodePoolsBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.cluster = mock.create_autospec(
        container_service.KubernetesCluster, instance=True
    )
    self.cluster.CLOUD = 'GCP'

  def setUpWithXNodes(self, num_nodes: int):
    many_nodes = ['foo'] * num_nodes
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            return_value=(' '.join(many_nodes), None, None),
        )
    )
    self.cluster.GetNodeNames.return_value = many_nodes
    self.cluster.GetNodePoolNames.return_value = many_nodes

  def test_IndividualChecks(self):
    self.setUpWithXNodes(20)
    provision_node_pools_benchmark._AssertNodes(self.cluster, 10, 10)
    provision_node_pools_benchmark._AssertNodePools(self.cluster, 10, 10)

  @flagsaver.flagsaver(provision_node_pools_init_batch=0)
  @flagsaver.flagsaver(provision_node_pools_test_batch=10)
  def test_FullRun(self):
    self.setUpWithXNodes(10)
    b_spec = mock.create_autospec(benchmark_spec.BenchmarkSpec, instance=True)
    b_spec.container_cluster = self.cluster
    samples = provision_node_pools_benchmark.Run(b_spec)
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


if __name__ == '__main__':
  unittest.main()
