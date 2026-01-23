import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import container_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import trino
from tests import pkb_common_test_case


FLAGS = flags.FLAGS

EDW_SERVICE_SPEC = mock.Mock(
    snapshot=None,
    concurrency=5,
    node_type=None,
    node_count=1,
    endpoint=None,
    db=None,
    user=None,
    password=None,
    type='trino',
    cluster_identifier=None,
)


class TrinoTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri='123'))
    self.enter_context(flagsaver.flagsaver(kubeconfig='kube1'))

  def testBuildAndCompile(self):
    db = trino.Trino(EDW_SERVICE_SPEC)
    self.assertEqual(db.name, 'pkb-123')

  def testCreateSetsAddress(self):
    # Arrange.
    self.enter_context(
        mock.patch.object(
            vm_util,
            'WriteTemporaryFile',
            return_value='trino.yaml',
        )
    )
    db = trino.Trino(EDW_SERVICE_SPEC)
    mock_kubernetes = mock.create_autospec(
        container_service.KubernetesCluster, instance=True
    )
    mock_kubernetes.DeployIngress.return_value = 'http://1.0.0.0:12345'
    db.SetContainerCluster(mock_kubernetes)
    mock_cmd = self.MockIssueCommand({
        'projects list': [('[{"projectNumber": 123}]', '', 0)],
        'get service pkb-123-trino': [('12345', '', 0)],
    })
    # Act.
    db._Create()
    # Assert.
    mock_cmd.func_to_mock.assert_has_calls([
        mock.call([
            'helm',
            'repo',
            'add',
            'trino',
            'https://trinodb.github.io/charts',
        ]),
        mock.call([
            'helm',
            'install',
            '-f',
            'trino.yaml',
            'pkb-123',
            'trino/trino',
            '--kubeconfig',
            'kube1',
        ]),
    ])
    self.assertEqual(db.address, 'http://1.0.0.0:12345')
    self.assertEqual(db.client_interface.hostname, '1.0.0.0')
    self.assertEqual(db.client_interface.port, 12345)
    self.assertEqual(db.client_interface.http_scheme, trino.HttpScheme.HTTP)


if __name__ == '__main__':
  unittest.main()
