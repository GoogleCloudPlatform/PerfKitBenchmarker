import inspect
import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


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


if __name__ == '__main__':
  unittest.main()
