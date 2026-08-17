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
        'get service': [('34.123.45.67', '', 0)],
    })
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)

    db._Create()

    self.assertEqual(db.address, '34.123.45.67')
    self.assertEqual(db.client_interface.address, '34.123.45.67')
    self.assertTrue(mock_cmd.func_to_mock.called)
    self.assertEqual(mock_tf.write.call_count, 3)
    keeper_yaml = mock_tf.write.call_args_list[0].args[0]
    cluster_yaml = mock_tf.write.call_args_list[1].args[0]
    self.assertIn('ClickHouseKeeperInstallation', keeper_yaml)
    self.assertIn('ClickHouseInstallation', cluster_yaml)
    self.assertIn('<external replace="1">', cluster_yaml)
    self.assertIn('<password_sha256_hex>', cluster_yaml)

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

  def testWaitForDeployment(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    mock_cmd = self.MockIssueCommand({
        'rollout status': [('rollout successfully completed', '', 0)],
        'wait': [('pod condition met', '', 0)],
    })
    db._WaitForDeployment()
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

  def testRunClientCommand(self):
    client = clickhouse.ClickhouseClientInterface()
    client.address = '10.0.0.1'
    client.port = 9000
    client.client_vm = self.MockRemoteCommand({'SELECT 1': [('output', '')]})

    stdout, _ = client._RunClientCommand(
        'clickhouse-client', ['SELECT 1', '--format=CSV']
    )

    self.assertEqual(stdout, 'output')
    client.client_vm.RemoteCommand.assert_called_once_with(
        'clickhouse-client --host=10.0.0.1 --port=9000 --user=external'
        ' --password= SELECT 1 --format=CSV'
    )

  def testExecuteQuery(self):
    client = clickhouse.ClickhouseClientInterface()
    client.address = '10.0.0.1'
    client.http_port = 8123
    json_output = (
        '{"query_wall_time_in_secs": 0.123, "details": {"job_id": "test-uuid",'
        ' "query_id": "test-uuid"}}'
    )
    client.client_vm = self.MockRemoteCommand({'single': [(json_output, '')]})

    time_taken, details = client.ExecuteQuery(
        'query_1.sql', print_results=False
    )

    self.assertAlmostEqual(time_taken, 0.123, places=5)
    self.assertEqual(details['client'], 'python')
    self.assertEqual(details['job_id'], 'test-uuid')
    self.assertEqual(details['query_id'], 'test-uuid')
    client.client_vm.RemoteCommand.assert_called_once_with(
        '.venv/bin/python ch_python_driver.py single --host=10.0.0.1'
        ' --port=8123 --user=external --password= --query_file=query_1.sql'
    )

  def testExecuteStatement(self):
    client = clickhouse.ClickhouseClientInterface()
    client.address = '10.0.0.1'
    client.port = 9000
    client.client_vm = self.MockRemoteCommand(
        {'clickhouse-client': [('stdout output', '')]}
    )

    res = client.ExecuteViaClickhouseClient('SELECT 1')

    self.assertEqual(res, 'stdout output')
    client.client_vm.RemoteCommand.assert_called_once_with(
        'clickhouse-client --host=10.0.0.1 --port=9000 --user=external'
        ' --password= --query="SELECT 1"'
    )

  def testExecuteThroughput(self):
    client = clickhouse.ClickhouseClientInterface()
    client.address = '10.0.0.1'
    client.http_port = 8123
    client.client_vm = self.MockRemoteCommand(
        {'throughput': [('{"throughput_wall_time_in_secs": 5.0}', '')]}
    )

    res = client.ExecuteThroughput([['1.sql', '2.sql'], ['3.sql', '4.sql']])

    self.assertEqual(res, '{"throughput_wall_time_in_secs": 5.0}')
    client.client_vm.RemoteCommand.assert_called_once_with(
        '.venv/bin/python ch_python_driver.py throughput --host=10.0.0.1'
        ' --port=8123 --user=external --password= --query_streams=\'[["1.sql",'
        ' "2.sql"], ["3.sql", "4.sql"]]\''
    )

  def testIsReadyTrue(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    mock_cmd = self.MockIssueCommand({
        'curl': [('24.10.1.1', '', 0)],
    })
    self.assertTrue(db._IsReady())
    self.assertTrue(mock_cmd.func_to_mock.called)

  def testIsReadyFalse(self):
    db = clickhouse.Clickhouse(EDW_SERVICE_SPEC)
    mock_cmd = self.MockIssueCommand({
        'curl': [('', '', 1)],
    })
    self.assertFalse(db._IsReady())
    self.assertTrue(mock_cmd.func_to_mock.called)

  def testGetTableStats(self):
    client = clickhouse.ClickhouseClientInterface()
    client.address = '10.0.0.1'
    client.port = 9000
    client.client_vm = self.MockRemoteCommand(
        {'clickhouse-client': [('10737418240\t100000000\n', '')]}
    )

    size, rows = client.GetTableStats('hits')

    self.assertEqual(size, 10.0)
    self.assertEqual(rows, 100000000)
    client.client_vm.RemoteCommand.assert_called_once_with(
        'clickhouse-client --host=10.0.0.1 --port=9000 --user=external'
        ' --password= --query="SELECT total_bytes, total_rows FROM'
        ' system.tables WHERE table = \'hits\'"'
    )

  def testGetTableStatsRaisesErrorWhenMissing(self):
    client = clickhouse.ClickhouseClientInterface()
    client.address = '10.0.0.1'
    client.port = 9000
    client.client_vm = self.MockRemoteCommand(
        {'clickhouse-client': [('', '')]}
    )

    with self.assertRaises(ValueError):
      client.GetTableStats('hits')


if __name__ == '__main__':
  unittest.main()
