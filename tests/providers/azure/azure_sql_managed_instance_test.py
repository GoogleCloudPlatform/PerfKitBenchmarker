import inspect
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_benchmarks
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import azure_sql_managed_instance
from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def _CreateBenchmarkSpecFromYaml(yaml_string, benchmark_name):
  config = configs.LoadConfig(yaml_string, {}, benchmark_name)
  config_spec = benchmark_config_spec.BenchmarkConfigSpec(
      benchmark_name, flag_values=FLAGS, **config
  )
  benchmark_module = next(
      b
      for b in windows_benchmarks.BENCHMARKS
      if b.BENCHMARK_NAME == benchmark_name
  )
  return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, 'name0')


class AzureSqlManagedInstanceInitTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    FLAGS.cloud = provider_info.AZURE
    FLAGS.use_managed_db = True
    mock_resource_group = mock.Mock()
    self.resource_group_patch = self.enter_context(
        mock.patch.object(azure_network, 'GetResourceGroup')
    )
    self.resource_group_patch.return_value = mock_resource_group
    mock_resource_group.name = 'az_resource'
    self.enter_context(
        mock.patch.object(util, 'GetSubscriptionId', return_value='test-sub')
    )

  @flagsaver.flagsaver(run_uri='test_uri')
  def testInitializationFromSpec(self):
    test_spec = inspect.cleandoc("""
    hammerdbcli:
      relational_db:
        engine: sqlserver
        db_tier: BusinessCritical
        family: Gen5
        high_availability: True
        db_spec:
          Azure:
            machine_type:
              cpus: 8
            zone: eastus
        db_disk_spec:
          Azure:
            disk_size: 1024
            provisioned_iops: 2000
        vm_groups:
          clients:
            vm_spec: *default_dual_core
            disk_spec: *default_500_gb
    """)
    self.test_bm_spec = _CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='hammerdbcli'
    )
    self.test_bm_spec.ConstructRelationalDb()
    instance = self.test_bm_spec.relational_db
    with self.subTest(name='instance_type'):
      self.assertIsInstance(
          instance, azure_sql_managed_instance.AzureSqlManagedInstance
      )
    with self.subTest(name='instance_id'):
      self.assertEqual(instance.instance_id, 'pkb-db-instance-test_uri')
    with self.subTest(name='tier'):
      self.assertEqual(instance.tier, 'BusinessCritical')
    with self.subTest(name='family'):
      self.assertEqual(instance.family, 'Gen5')
    with self.subTest(name='cpus'):
      self.assertEqual(instance.cpus, 8)
    with self.subTest(name='disk_size'):
      self.assertEqual(instance.disk_size, 1024)
    with self.subTest(name='disk_iops'):
      self.assertEqual(instance.disk_iops, 2000)
    with self.subTest(name='high_availability'):
      self.assertTrue(instance.high_availability)
    with self.subTest(name='version'):
      self.assertEqual(instance.version, 'AlwaysUpToDate')

  @flagsaver.flagsaver(run_uri='test_uri')
  def testInitializationFromFlags(self):
    test_spec = inspect.cleandoc("""
    hammerdbcli:
      relational_db:
        engine: sqlserver
        db_spec:
          Azure:
            machine_type:
              compute_units: 500
            zone: eastus
        db_disk_spec:
          Azure:
            disk_size: 500
            disk_type: Premium_LRS
        vm_groups:
          clients:
            vm_spec: *default_dual_core
            disk_spec: *default_500_gb
    """)
    FLAGS['zone'].parse('westus')
    FLAGS['db_zone'].parse('westus')
    FLAGS['db_high_availability'].parse(True)
    FLAGS['managed_db_tier'].parse('BusinessCritical')
    FLAGS['db_family'].parse('Gen8IM')
    FLAGS['db_cpus'].parse(8)
    FLAGS['db_disk_size'].parse(1024)
    FLAGS['db_disk_iops'].parse(10000)
    FLAGS['db_engine_version'].parse('SQLServer2022')
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='hammerdbcli'
    )
    self.test_bm_spec.ConstructRelationalDb()
    instance = self.test_bm_spec.relational_db
    with self.subTest(name='instance_type'):
      self.assertIsInstance(
          instance, azure_sql_managed_instance.AzureSqlManagedInstance
      )
    with self.subTest(name='instance_id'):
      self.assertEqual(instance.instance_id, 'pkb-db-instance-test_uri')
    with self.subTest(name='tier'):
      self.assertEqual(instance.tier, 'BusinessCritical')
    with self.subTest(name='family'):
      self.assertEqual(instance.family, 'Gen8IM')
    with self.subTest(name='cpus'):
      self.assertEqual(instance.cpus, 8)
    with self.subTest(name='disk_size'):
      self.assertEqual(instance.disk_size, 1024)
    with self.subTest(name='disk_iops'):
      self.assertEqual(instance.disk_iops, 10000)
    with self.subTest(name='high_availability'):
      self.assertTrue(instance.high_availability)
    with self.subTest(name='region'):
      self.assertEqual(instance.region, 'westus')
    with self.subTest(name='version'):
      self.assertEqual(instance.version, 'SQLServer2022')

  @flagsaver.flagsaver(run_uri='test_uri')
  def testInitializationFromFlagsInvalid(self):
    test_spec = inspect.cleandoc("""
    hammerdbcli:
      relational_db:
        engine: sqlserver
        db_spec:
          Azure:
            machine_type:
              compute_units: 500
            zone: eastus
        db_disk_spec:
          Azure:
            disk_size: 500
            disk_type: Premium_LRS
        vm_groups:
          clients:
            vm_spec: *default_dual_core
            disk_spec: *default_500_gb
    """)
    FLAGS['managed_db_tier'].parse('BusinessCritical')
    FLAGS['db_cpus'].parse(8)
    FLAGS['db_memory'].parse('64GiB')
    with self.assertRaises(errors.Config.InvalidValue):
      self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
          yaml_string=test_spec, benchmark_name='hammerdbcli'
      )
      self.test_bm_spec.ConstructRelationalDb()


class AzureSqlManagedInstanceTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.cloud = provider_info.AZURE
    FLAGS.use_managed_db = True
    FLAGS.run_uri = 'test_uri'
    mock_resource_group = mock.Mock()
    self.resource_group_patch = self.enter_context(
        mock.patch.object(azure_network, 'GetResourceGroup')
    )
    self.resource_group_patch.return_value = mock_resource_group
    mock_resource_group.name = 'az_resource'
    self.enter_context(
        mock.patch.object(util, 'GetSubscriptionId', return_value='test-sub')
    )
    test_spec = inspect.cleandoc("""
    hammerdbcli:
      relational_db:
        engine: sqlserver
        db_tier: GeneralPurpose
        family: Gen5
        high_availability: True
        db_spec:
          Azure:
            machine_type:
              cpus: 8
              memory: 32GiB
            zone: eastus
        db_disk_spec:
          Azure:
            disk_size: 1024
            provisioned_iops: 2000
        vm_groups:
          clients:
            vm_spec: *default_dual_core
            disk_spec: *default_500_gb
    """)
    self.test_bm_spec = _CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='hammerdbcli'
    )
    self.test_bm_spec.ConstructRelationalDb()
    self.instance = self.test_bm_spec.relational_db

  @flagsaver.flagsaver(run_uri='test_uri')
  def testGetResourceMetadata(self):
    metadata = self.instance.GetResourceMetadata()

    with self.subTest(name='tier'):
      self.assertEqual(metadata['tier'], 'GeneralPurpose')
    with self.subTest(name='family'):
      self.assertEqual(metadata['family'], 'Gen5')
    with self.subTest(name='zone'):
      self.assertEqual(metadata['zone'], 'eastus')
    with self.subTest(name='engine'):
      self.assertEqual(metadata['engine'], 'sqlserver')
    with self.subTest(name='high_availability'):
      self.assertEqual(metadata['high_availability'], True)
    with self.subTest(name='cpus'):
      self.assertEqual(metadata['cpus'], 8)
    with self.subTest(name='memory'):
      self.assertEqual(metadata['memory'], 32768)
    with self.subTest(name='disk_size'):
      self.assertEqual(metadata['disk_size'], 1024)
    with self.subTest(name='iops'):
      self.assertEqual(metadata['disk_iops'], 2000)
    with self.subTest(name='version'):
      self.assertEqual(metadata['engine_version'], 'AlwaysUpToDate')

  @flagsaver.flagsaver(run_uri='test_uri')
  def testCreate(self):
    self.instance.subnet = mock.Mock()
    self.instance.subnet.name = 'subnet-name'
    self.instance.client_vm = mock.Mock()
    self.instance.client_vm.network.vnet.name = 'vnet-name'
    self.enter_context(mock.patch.object(self.instance, '_WaitUntilRunning'))
    mock_issue_command = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand')
    )

    self.instance._Create()

    cmd = mock_issue_command.call_args_list[0][0][0]
    cmd_str = ' '.join(cmd)
    with self.subTest(name='resource_group'):
      self.assertIn('--resource-group az_resource', cmd_str)
    with self.subTest(name='location'):
      self.assertIn('--location eastus', cmd_str)
    with self.subTest(name='subnet'):
      self.assertIn('--subnet subnet-name', cmd_str)
    with self.subTest(name='vnet_name'):
      self.assertIn('--vnet-name vnet-name', cmd_str)
    with self.subTest(name='database_format'):
      self.assertIn('--database-format AlwaysUpToDate', cmd_str)
    with self.subTest(name='edition'):
      self.assertIn('--edition GeneralPurpose', cmd_str)
    with self.subTest(name='family'):
      self.assertIn('--family Gen5', cmd_str)
    with self.subTest(name='capacity'):
      self.assertIn('--capacity 8', cmd_str)
    with self.subTest(name='storage'):
      self.assertIn('--storage 1024', cmd_str)
    with self.subTest(name='iops'):
      self.assertIn('--iops 2000', cmd_str)
    with self.subTest(name='zone_redundant'):
      self.assertIn('--zone-redundant true', cmd_str)
    with self.subTest(name='gpv2'):
      self.assertIn('--gpv2 true', cmd_str)
    with self.subTest(name='memory'):
      self.assertIn('--memory 32', cmd_str)


if __name__ == '__main__':
  unittest.main()
