"""Provides a simple mock implemenation of a ContainerService.

Use both MockContainerInit and CreateTestKubernetesCluster to initialize
your test case.
"""

from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import container_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import container_spec
from tests import pkb_common_test_case


# Use Mesos as a valid cloud we can override the implementation for.
TEST_CLOUD = provider_info.UNIT_TEST


class TestKubernetesCluster(container_service.KubernetesCluster):

  CLOUD = TEST_CLOUD

  def _Create(self):
    pass

  def _Delete(self):
    pass


def CreateTestKubernetesCluster(
    container_cluster_spec: container_spec.ContainerClusterSpec | None = None,
) -> TestKubernetesCluster:
  """Returns a ContainerCluster object for testing. Needs MockContainerInit."""
  if container_cluster_spec is None:
    container_cluster_spec = container_spec.ContainerClusterSpec(
        'test-cluster',
        **{
            'cloud': TEST_CLOUD,
            'vm_spec': {
                TEST_CLOUD: {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-east2-a',
                },
            },
        },
    )
  return TestKubernetesCluster(container_cluster_spec)


def MockContainerInit(test_case: pkb_common_test_case.PkbCommonTestCase):
  """Mocks needed dependencies for ContainerService initialization."""
  test_case.enter_context(flagsaver.flagsaver(kubeconfig='kubeconfig'))
  test_case.enter_context(flagsaver.flagsaver(run_uri='123'))
  test_case.enter_context(
      mock.patch('perfkitbenchmarker.providers.LoadProvider', autospec=True)
  )
