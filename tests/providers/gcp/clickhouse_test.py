import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import clickhouse
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
    type='clickhouse',
    cluster_identifier=None,
)


class ClickhouseTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri='123'))
    self.enter_context(flagsaver.flagsaver(kubeconfig='kube1'))

  def testBuildAndCompile(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    self.assertEqual(db.name, 'pkb-123')
    self.assertEqual(db.port, 9000)
    self.assertEqual(db._GetClickhouseReplicaPrefix(), 'chi-pkb-123-pkb-123')

  def testCreate(self):
    mock_tf = mock.MagicMock()
    mock_tf.__enter__.return_value = mock_tf
    mock_tf.name = 'manifest.yaml'
    self.enter_context(
        mock.patch.object(vm_util, 'NamedTemporaryFile', return_value=mock_tf)
    )
    mock_cmd = self.MockIssueCommand({
        'projects list': [('[{"projectNumber": 123}]', '', 0)],
    })
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)

    db._Create()

    self.assertTrue(mock_cmd.func_to_mock.called)
    self.assertEqual(mock_tf.write.call_count, 2)
    keeper_yaml = mock_tf.write.call_args_list[0].args[0]
    cluster_yaml = mock_tf.write.call_args_list[1].args[0]
    self.assertIn('ClickHouseKeeperInstallation', keeper_yaml)
    self.assertIn('replicasCount: 3', keeper_yaml)
    self.assertIn('ClickHouseInstallation', cluster_yaml)
    self.assertIn('replicasCount: 3', cluster_yaml)
    self.assertIn('shardsCount: 1', cluster_yaml)
    self.assertIn('memory: 32G', cluster_yaml)

  def testCpuAndReplicasInferred(self):
    self.enter_context(
        flagsaver.flagsaver(
            clickhouse_memory=64,
            clickhouse_num_shards=2,
            clickhouse_num_replicas=None,
        )
    )
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    self.assertEqual(db.cpu, 16.0)
    self.assertEqual(db.num_replicas, 6)

  def testPostCreate(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    mock_write = self.enter_context(
        mock.patch.object(vm_util, 'WriteTemporaryFile', return_value='lb.yaml')
    )
    mock_cmd = self.MockIssueCommand({
        'get service': [('34.123.45.67', '', 0)],
        'apply -f': [('service/pkb-123 created', '', 0)],
    })

    db._PostCreate()

    self.assertEqual(db.port, 9000)
    self.assertEqual(db.address, '34.123.45.67')
    self.assertIn(
        'clickhouse.altinity.com/app: chop', mock_write.call_args[0][0]
    )
    self.assertTrue(mock_cmd.func_to_mock.called)

  def testIsReadyTrue(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    mock_cmd = self.MockIssueCommand({
        'rollout status': [('rollout successfully completed', '', 0)],
        'wait': [('pod condition met', '', 0)],
    })
    self.assertTrue(db._IsReady())
    self.assertTrue(mock_cmd.func_to_mock.called)

  def testIsReadyFalse(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    mock_cmd = self.MockIssueCommand({
        'rollout status': [('', 'not found', 1)],
    })
    self.assertFalse(db._IsReady())
    self.assertTrue(mock_cmd.func_to_mock.called)

  def testDelete(self):
    mock_cmd = self.MockIssueCommand({})
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)

    db._Delete()

    self.assertTrue(mock_cmd.func_to_mock.called)

  def testGetMetadata(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    metadata = db.GetMetadata()
    self.assertIn('clickhouse_memory', metadata)


if __name__ == '__main__':
  unittest.main()
