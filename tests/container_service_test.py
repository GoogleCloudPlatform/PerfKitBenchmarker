from typing import Iterable
import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case


# Use Mesos as a valid cloud we can override the implementation for.
_CLUSTER_CLOUD = provider_info.UNIT_TEST


class TestKubernetesCluster(container_service.KubernetesCluster):

  CLOUD = _CLUSTER_CLOUD

  def _Create(self):
    pass

  def _Delete(self):
    pass


class ContainerServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(kubeconfig='kubeconfig'))
    self.enter_context(flagsaver.flagsaver(run_uri='123'))
    self.kubernetes_cluster = TestKubernetesCluster(
        container_spec.ContainerClusterSpec(
            'test-cluster',
            **{
                'cloud': _CLUSTER_CLOUD,
                'vm_spec': {
                    _CLUSTER_CLOUD: {
                        'machine_type': 'fake-machine-type',
                        'zone': 'us-east2-a',
                    },
                },
            },
        )
    )

  def test_apply_manifest_gets_deployment_name(self):
    self.MockIssueCommand(
        {'apply -f': [('deployment.apps/test-deployment created', '', 0)]}
    )
    self.enter_context(
        mock.patch.object(
            container_service.data,
            'ResourcePath',
            return_value='path/to/test-deployment.yaml',
        )
    )
    deploy_ids = self.kubernetes_cluster.ApplyManifest(
        'test-deployment.yaml',
    )
    self.assertEqual(next(deploy_ids), 'deployment.apps/test-deployment')

  def test_retriable_kubectl_command_fails_on_random_error(self):
    self.MockIssueCommand(
        {'get podpatchwork': [('', 'error: invalid syntax', 1)]}
    )
    with self.assertRaises(errors.VmUtil.IssueCommandError):
      container_service.RunRetryableKubectlCommand(['get', 'podpatchwork'])

  def test_retriable_kubectl_command_retries_on_connection_reset(self):
    self.MockIssueCommand({
        'get pods': [
            ('', 'error: read: connection reset by peer', 1),
            ('pod1, pod2', '', 0),
        ]
    })
    out, err, ret = container_service.RunRetryableKubectlCommand(
        ['get', 'pods']
    )
    self.assertEqual(out, 'pod1, pod2')
    self.assertEqual(err, '')
    self.assertEqual(ret, 0)

  def test_GetNumReplicasSamples_found(self):
    resource_name = 'deployment/my_deployment'
    namespace = 'my_namespace'
    self.MockIssueCommand({
        f'get {resource_name} -n {namespace} -o=jsonpath=': [
            ('456, 123', '', 0)
        ]
    })

    def _Sample(count: int, state: str) -> Sample:
      return Sample(
          metric='k8s/num_replicas_' + state,
          value=count,
          unit='',
          metadata={
              'namespace': namespace,
              'resource_name': resource_name,
          },
          timestamp=0,
      )

    samples = _ClearTimestamps(
        container_service.KubernetesClusterCommands.GetNumReplicasSamples(
            resource_name, namespace
        )
    )
    self.assertCountEqual(
        samples,
        [
            _Sample(456, 'any'),
            _Sample(123, 'ready'),
            _Sample(456 - 123, 'unready'),
        ],
    )

  def test_GetNumReplicasSamples_not_found(self):
    resource_name = 'deployment/my_deployment'
    namespace = 'my_namespace'
    self.MockIssueCommand({
        f'get {resource_name} -n {namespace} -o=jsonpath=': [
            ('', 'Error from server (NotFound): <details>', 1)
        ]
    })

    samples = container_service.KubernetesClusterCommands.GetNumReplicasSamples(
        resource_name, namespace
    )
    self.assertEmpty(samples)

  def test_GetNumNodesSamples(self):
    self.MockIssueCommand({
        'get nodes -o=jsonpath=': [
            ('True\nFalse\nTrue\nSomethingUnexpected\n', '', 0)
        ]
    })

    def _Sample(count: int, state: str) -> Sample:
      return Sample(
          metric='k8s/num_nodes_' + state,
          value=count,
          unit='',
          metadata={},
          timestamp=0,
      )

    samples = _ClearTimestamps(
        container_service.KubernetesClusterCommands.GetNumNodesSamples()
    )
    self.assertCountEqual(
        samples,
        [
            _Sample(4, 'any'),
            _Sample(2, 'ready'),
            _Sample(1, 'unready'),
            _Sample(1, 'unknown'),
        ],
    )


def _ClearTimestamps(samples: Iterable[Sample]) -> Iterable[Sample]:
  for s in samples:
    yield Sample(s.metric, s.value, s.unit, s.metadata, timestamp=0)


if __name__ == '__main__':
  unittest.main()
