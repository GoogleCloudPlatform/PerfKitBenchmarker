import datetime
import inspect
import json
import time
import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class AzureFlexibleServerMetricsTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=('', '', ''))
    )
    FLAGS.run_uri = '123'
    FLAGS.cloud = provider_info.AZURE
    FLAGS['db_engine'].parse(sql_engine_utils.FLEXIBLE_SERVER_POSTGRES)
    test_spec = inspect.cleandoc("""
    sysbench:
      relational_db:
        engine: postgres
        engine_version: '13'
        database_username: user
        database_password: password
        high_availability: False
        db_spec:
          Azure:
            machine_type: Standard_D2s_v3
            zone: westus2
        db_disk_spec:
          Azure:
            disk_size: 128
        vm_groups:
          clients:
            vm_spec:
              Azure:
                machine_type: Standard_B4ms
                zone: westus2
    """)
    self.spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='sysbench'
    )
    self.spec.ConstructRelationalDb()
    self.server = self.spec.relational_db
    self.server.resource_group = mock.Mock()
    self.server.resource_group.name = 'test-group'
    self.enter_context(
        mock.patch.object(util, 'GetSubscriptionId', return_value='test-sub')
    )

  def testCollectMetrics(self):
    # Mock the response from Azure Monitor
    mock_response = {
        'value': [{
            'timeseries': [{
                'data': [
                    {
                        'timeStamp': '2025-11-26T10:00:00Z',
                        'average': 10.0,
                    },
                    {
                        'timeStamp': '2025-11-26T10:01:00Z',
                        'average': 20.0,
                    },
                ]
            }]
        }]
    }
    self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueRetryableCommand',
            return_value=(json.dumps(mock_response), ''),
        )
    )

    start_time = datetime.datetime(2025, 11, 26, 10, 0, 0)
    end_time = datetime.datetime(2025, 11, 26, 10, 1, 0)
    samples = self.server.CollectMetrics(start_time, end_time)

    # Check the number of samples returned (4 per metric * 6 metrics)
    self.assertLen(samples, 24)

    # Spot check a few sample values
    sample_names = [s.metric for s in samples]
    self.assertIn('cpu_utilization_average', sample_names)
    self.assertIn('cpu_utilization_min', sample_names)
    self.assertIn('cpu_utilization_max', sample_names)
    self.assertIn('disk_read_iops_average', sample_names)

    cpu_avg = next(s for s in samples if s.metric == 'cpu_utilization_average')
    self.assertEqual(cpu_avg.value, 15.0)
    self.assertEqual(cpu_avg.unit, '%')

    cpu_min = next(s for s in samples if s.metric == 'cpu_utilization_min')
    self.assertEqual(cpu_min.value, 10.0)
    self.assertEqual(cpu_min.unit, '%')

    cpu_max = next(s for s in samples if s.metric == 'cpu_utilization_max')
    self.assertEqual(cpu_max.value, 20.0)
    self.assertEqual(cpu_max.unit, '%')

  def testCollectMetricsAggregation(self):
    mock_issue_cmd = self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueRetryableCommand',
            return_value=(json.dumps({'value': []}), ''),
        )
    )
    start_time = datetime.datetime(2025, 11, 26, 10, 0, 0)
    end_time = datetime.datetime(2025, 11, 26, 10, 1, 0)

    # Test 'Total' aggregation
    count_metric = relational_db.MetricSpec(
        'some_count', 'some_count', 'count', None
    )
    self.server._CollectProviderMetric(count_metric, start_time, end_time)

    call_args = mock_issue_cmd.call_args[0][0]
    self.assertIn('--aggregation', call_args)
    self.assertIn('Total', call_args)

    # Test 'Average' aggregation
    avg_metric = relational_db.MetricSpec(
        'some_metric', 'some_metric', 'percent', None
    )
    self.server._CollectProviderMetric(avg_metric, start_time, end_time)

    call_args = mock_issue_cmd.call_args[0][0]
    self.assertIn('--aggregation', call_args)
    self.assertIn('Average', call_args)


class AzureFlexibleServerCreateTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.cloud = provider_info.AZURE
    FLAGS.run_uri = 'test_run_uri'
    mock_resource_group = mock.Mock()
    self.resource_group_patch = self.enter_context(
        mock.patch.object(azure_network, 'GetResourceGroup')
    )
    self.resource_group_patch.return_value = mock_resource_group
    mock_resource_group.name = 'az_resource'
    self.mock_command = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand')
    )
    self.mock_command.return_value = (None, '', None)

  def testCreatePostgres(self):
    yaml_spec = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            cloud: {provider_info.AZURE}
            engine: {sql_engine_utils.FLEXIBLE_SERVER_POSTGRES}
            engine_version: '13'
            db_tier: GeneralPurpose
            db_spec:
              {provider_info.AZURE}:
                machine_type: Standard_D2ds_v4
                zone: eastus
            db_disk_spec:
              {provider_info.AZURE}:
                disk_size: 128
                provisioned_iops: 1000
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_spec, 'sysbench'
    )
    bm_spec.ConstructRelationalDb()

    bm_spec.relational_db._Create()
    metadata = bm_spec.relational_db.GetResourceMetadata()

    cmd = ' '.join(self.mock_command.call_args[0][0])
    with self.subTest(name='StorageType'):
      self.assertIn('--storage-type Premium_LRS', cmd)
    with self.subTest(name='Iops'):
      self.assertIn('--iops 1000', cmd)
    with self.subTest(name='Metadata'):
      self.assertEqual(metadata['disk_type'], 'Premium_LRS')

  def testCreateMysql(self):
    yaml_spec = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            cloud: {provider_info.AZURE}
            engine: {sql_engine_utils.FLEXIBLE_SERVER_MYSQL}
            engine_version: '8.0'
            db_tier: GeneralPurpose
            db_spec:
              {provider_info.AZURE}:
                machine_type: Standard_D2ds_v4
                zone: eastus
            db_disk_spec:
              {provider_info.AZURE}:
                disk_size: 128
                provisioned_iops: 1000
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_spec, 'sysbench'
    )
    bm_spec.ConstructRelationalDb()

    bm_spec.relational_db._Create()
    metadata = bm_spec.relational_db.GetResourceMetadata()

    cmd = ' '.join(self.mock_command.call_args[0][0])
    with self.subTest(name='AutoScaleIops'):
      self.assertIn('--auto-scale-iops Disabled', cmd)
    with self.subTest(name='Metadata'):
      self.assertEqual(metadata['autoscale_iops'], 'Disabled')

  def testConstructMysqlWithThroughputRaises(self):
    yaml_spec = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            cloud: {provider_info.AZURE}
            engine: {sql_engine_utils.FLEXIBLE_SERVER_MYSQL}
            engine_version: '8.0'
            db_tier: GeneralPurpose
            db_spec:
              {provider_info.AZURE}:
                machine_type: Standard_D2ds_v4
                zone: eastus
            db_disk_spec:
              {provider_info.AZURE}:
                disk_size: 128
                provisioned_iops: 1000
                provisioned_throughput: 200
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_spec, 'sysbench'
    )
    with self.assertRaises(errors.Config.InvalidValue):
      bm_spec.ConstructRelationalDb()


class AzureFlexibleServerPremiumV2CreateTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    FLAGS.cloud = provider_info.AZURE
    FLAGS.run_uri = 'test_run_uri'
    self.resource_group_patch = self.enter_context(
        mock.patch.object(
            azure_network, 'GetResourceGroup', return_value=mock.Mock()
        )
    )
    self.enter_context(
        mock.patch.object(util, 'GetSubscriptionId', return_value='test-sub')
    )
    self.enter_context(mock.patch.object(time, 'sleep'))

  def testCreatePostgresPremiumV2(self):
    mock_cmd = self.MockIssueCommand({
        'rest --method PUT': [(
            '',
            "'Azure-AsyncOperation': 'https://management.azure.com/async_op'",
            0,
        )],
        'rest --method GET': [
            ('{"status": "InProgress"}', '', 0),
            ('{"status": "Succeeded"}', '', 0),
        ],
    })
    yaml_spec = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            cloud: {provider_info.AZURE}
            engine: {sql_engine_utils.FLEXIBLE_SERVER_POSTGRES}
            engine_version: '13'
            db_tier: GeneralPurpose
            db_spec:
              {provider_info.AZURE}:
                machine_type: Standard_D2ds_v4
                zone: eastus
            db_disk_spec:
              {provider_info.AZURE}:
                disk_size: 128
                disk_type: PremiumV2_LRS
                provisioned_iops: 1000
                provisioned_throughput: 200
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_spec, 'sysbench'
    )
    bm_spec.ConstructRelationalDb()

    bm_spec.relational_db._Create()

    self.assertEqual(mock_cmd.func_to_mock.call_count, 3)
    put_call, get_call1, get_call2 = mock_cmd.func_to_mock.call_args_list

    # Check az rest PUT call
    put_cmd = ' '.join(put_call[0][0])
    with self.subTest(name='RestApi'):
      self.assertIn('rest --method PUT', put_cmd)
      self.assertIn('PremiumV2_LRS', put_cmd)
      self.assertIn('"iops": 1000', put_cmd)
      self.assertIn('"throughput": 200', put_cmd)

    # Check az rest GET calls
    get_cmd1 = ' '.join(get_call1[0][0])
    self.assertIn('rest --method GET', get_cmd1)
    get_cmd2 = ' '.join(get_call2[0][0])
    self.assertIn('rest --method GET', get_cmd2)

  def testCreatePostgresPremiumV2Fails(self):
    self.MockIssueCommand({
        'rest --method PUT': [(
            '',
            "'Azure-AsyncOperation': 'https://management.azure.com/async_op'",
            0,
        )],
        'rest --method GET': [('{"status": "Failed"}', '', 0)],
    })
    yaml_spec = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            cloud: {provider_info.AZURE}
            engine: {sql_engine_utils.FLEXIBLE_SERVER_POSTGRES}
            engine_version: '13'
            db_tier: GeneralPurpose
            db_spec:
              {provider_info.AZURE}:
                machine_type: Standard_D2ds_v4
                zone: eastus
            db_disk_spec:
              {provider_info.AZURE}:
                disk_size: 128
                disk_type: PremiumV2_LRS
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_spec, 'sysbench'
    )
    bm_spec.ConstructRelationalDb()

    with self.assertRaises(errors.Resource.CreationError):
      bm_spec.relational_db._Create()

  def testCreatePostgresPremiumV2NoAsyncHeader(self):
    self.MockIssueCommand(
        {
            'rest --method PUT': [(
                '',
                'some other stderr',
                0,
            )]
        }
    )
    yaml_spec = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            cloud: {provider_info.AZURE}
            engine: {sql_engine_utils.FLEXIBLE_SERVER_POSTGRES}
            engine_version: '13'
            db_tier: GeneralPurpose
            db_spec:
              {provider_info.AZURE}:
                machine_type: Standard_D2ds_v4
                zone: eastus
            db_disk_spec:
              {provider_info.AZURE}:
                disk_size: 128
                disk_type: PremiumV2_LRS
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_spec, 'sysbench'
    )
    bm_spec.ConstructRelationalDb()

    with self.assertRaises(errors.Resource.CreationError):
      bm_spec.relational_db._Create()


if __name__ == '__main__':
  unittest.main()
