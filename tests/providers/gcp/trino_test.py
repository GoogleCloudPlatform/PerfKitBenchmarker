import unittest

from absl import flags
from absl.testing import flagsaver
import mock
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

  def testCreate(self):
    db = trino.Trino(EDW_SERVICE_SPEC)
    mock_cmd = self.MockIssueCommand({'': [('', '', 0)]})
    db._Create()
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
            'pkb-123',
            'trino/trino',
            '--kubeconfig',
            'kube1',
        ]),
    ])


if __name__ == '__main__':
  unittest.main()
