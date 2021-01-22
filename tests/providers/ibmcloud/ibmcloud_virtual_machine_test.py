# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for perfkitbenchmarker.tests.providers.ibmcloud.ibmcloud_virtual_machine."""

import unittest

from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import linux_benchmarks
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
                                 ibmcloud_virtual_machine.IbmCloudVirtualMachine
                                ):
  pass


class IbmCloudVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(IbmCloudVirtualMachineTest, self).setUp()
    self.mock_create_instance = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreateInstance'))
    self.mock_instance_status = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'InstanceStatus'))
    self.mock_get_resource = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'GetResource'))
    self.mock_create_vpc = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreateVpc'))
    self.mock_create_subnet = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, 'CreateSubnet'))
    self.mock_check_environment = self.enter_context(
        mock.patch.object(ibm_api.IbmAPICommand, '_CheckEnvironment'))
    with mock.patch.object(ibm_api.IbmAPICommand, '__init__',
                           lambda self: None):
      self.cmd = ibm_api.IbmAPICommand()
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

  def testSetupResources(self):
    self.vm.zone = 'us-south-2'
    self.prefix = 'perfkit'
    FLAGS.run_uri = 'testuri'
    self.mock_create_vpc.side_effect = ['vpc_id']
    self.mock_create_subnet.side_effect = [{'id': 'subnet_id'}]
    self.mock_get_resource.side_effect = [{'id': 'resource_id'}]
    self.assertEqual('vpc_id', self.cmd.CreateVpc())
    self.assertEqual('subnet_id', self.cmd.CreateSubnet()['id'])
    self.assertEqual('resource_id', self.cmd.GetResource()['id'])

  def testIbmCloudVirtualMachine(self):
    self.mock_create_instance.side_effect = [{'id': 'vm_test_id'}]
    self.mock_instance_status.side_effect = ['running']
    self.assertEqual(20000, self.vm.volume_iops)
    self.assertEqual('running', self.cmd.InstanceStatus())
    self.assertEqual('vm_test_id', self.cmd.CreateInstance()['id'])


if __name__ == '__main__':
  unittest.main()
