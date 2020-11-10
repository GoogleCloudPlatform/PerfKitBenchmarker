"""Tests for perfkitbenchmarker.tests.providers.ibmcloud.ibmcloud_virtual_machine."""

import unittest
import json
from absl import flags
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.ibmcloud import ibm_api
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_virtual_machine
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'unixbench'
BENCHMARK_CONFIG = """
unixbench:
  description: Runs UnixBench.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""
URI = 'ibmuri123'


class TestIbmCloudVirtualMachine(pkb_common_test_case.TestOsMixin,
                                 ibmcloud_virtual_machine.IbmCloudVirtualMachine):
  pass


class IbmCloudVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(IbmCloudVirtualMachineTest, self).setUp()
    self.mock_create_instance = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreateInstance'))
    self.mock_instance_status = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'InstanceStatus'))
    self.mock_get_instance_primary_vnic = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'InstanceGetPrimaryVnic'))
    self.mock_instance_fip_create = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'InstanceFipCreate'))
    self.mock_list_resources = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'ListResources'))
    self.mock_create_vpc = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreateVpc'))
    self.mock_create_prefix = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreatePrefix'))
    self.mock_create_subnet = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreateSubnet'))
    self.mock_image_show = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'ImageShow'))
    self.vm = self._CreateTestIbmCloudVirtualMachine()

  def _CreateBenchmarkSpecFromYaml(self, yaml_string,
                                   benchmark_name=BENCHMARK_NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    spec = self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)
    spec.disable_interrupt_moderation = False
    spec.disable_rss = False
    spec.zone = 'us-south-1'
    spec.cidr = '10.101.0.0/24'
    spec.machine_type = 'Test_machine_type'
    spec.gpu_count = '1'
    spec.gpu_type = 'test-gpu-type'
    spec.image = 'test-image'
    spec.install_packages = 'None'
    spec.background_cpu_threads = 'None'
    spec.background_network_mbits_per_sec = '1'
    spec.background_network_ip_type = 'None'
    spec.vm_metadata = {}
    return spec

  def _CreateBenchmarkSpecFromConfigDict(self, config_dict, benchmark_name):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        benchmark_name,
        flag_values=FLAGS,
        **config_dict)
    benchmark_module = next((b for b in linux_benchmarks.BENCHMARKS
                             if b.BENCHMARK_NAME == benchmark_name))
    return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, URI)

  def _CreateTestIbmCloudVirtualMachine(self):
    spec = self._CreateBenchmarkSpecFromYaml(BENCHMARK_CONFIG)
    return TestIbmCloudVirtualMachine(spec)

  def testSetupMzone(self):
    self.vm.zone = 'us-south-2'
    self.prefix = 'perfkit'
    FLAGS.run_uri = 'testuri'
    self.mock_image_show.side_effect = [{'name': 'Debian 10', 'operating_system': {'name': 'debian10'}}]
    self.vm._SetupMzone()
    self.cmd = ibm_api.IbmAPICommand(self)
    self.mock_create_vpc.side_effect = ['vpc_id']
    self.mock_create_subnet.side_effect = [{'id': 'subnet_id'}]
    self.assertEqual('vpc_id', self.cmd.CreateVpc())
    self.assertEqual('subnet_id', self.cmd.CreateSubnet()['id'])

  def testIbmCloudVirtualMachine(self):
    self.assertEqual(20000, self.vm.volume_iops)
    self.mock_create_instance.side_effect = [json.dumps({'id': 'vm_test_id'}), '', 0]  # convert the json to str
    self.mock_instance_status.side_effect = ['running']
    self.vm._Create()
    self.assertTrue(self.vm.vm_started)

  def testCreateFip(self):
    self.mock_get_instance_primary_vnic.side_effect = ['vnic_id']
    self.mock_instance_fip_create.side_effect = [json.dumps({'id': 'fip_id', 'address': '165.55.22.33'})]
    self.assertEqual(('165.55.22.33', 'fip_id'), self.vm._CreateFip('fip_name'))


if __name__ == '__main__':
  unittest.main()
