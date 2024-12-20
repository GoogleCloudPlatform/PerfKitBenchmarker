import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import container_spec
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


if __name__ == '__main__':
  unittest.main()
