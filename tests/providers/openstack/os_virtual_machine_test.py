"""Tests for perfkitbenchmarker.providers.openstack.os_virtual_machine_test."""

import os
import unittest
import mock
import json
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.openstack import os_virtual_machine
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
_BENCHMARK_NAME = 'iperf'
_URI = 'uri45678'
_CFG_DEFAULT_DEFAULT = """
iperf:
  vm_groups:
    vm_1:
      cloud: OpenStack
      vm_spec:
        OpenStack:
            image: test-image
            machine_type: test_machine_type
            disable_interrupt_moderation: False
"""

_network_true = {'router:external': True}
_network_external = {'router:external': 'External'}
_network_fail = {'router:external': 'Fail'}


class BaseOpenStackNetworkTest(pkb_common_test_case.PkbCommonTestCase):

  def _CreateBenchmarkSpecFromYaml(self, yaml_string,
                                   benchmark_name=_BENCHMARK_NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    spec = self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)
    spec.disable_interrupt_moderation = False
    spec.disable_rss = False
    spec.zone = "test-zone"
    spec.cidr = "192.164.1.0/24"
    spec.machine_type = "Test_machine_type"
    spec.gpu_count = "1"
    spec.gpu_type = "test-gpu-type"
    spec.image = "test-image"
    spec.install_packages = "None"
    spec.background_cpu_threads = "None"
    spec.background_network_mbits_per_sec = "1"
    spec.background_network_ip_type = "None"
    spec.vm_metadata = {}
    return spec

  def _CreateBenchmarkSpecFromConfigDict(self, config_dict, benchmark_name):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        benchmark_name,
        flag_values=FLAGS,
        **config_dict)
    benchmark_module = next((b for b in linux_benchmarks.BENCHMARKS
                             if b.BENCHMARK_NAME == benchmark_name))
    return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, _URI)


class OpenStackVirtualMachineTest(BaseOpenStackNetworkTest):


  @mock.patch.object(os_virtual_machine.OpenStackVirtualMachine, '_CheckNetworkExists')
  def test_CheckFloatingIPNetworkExistsWithTrue(self, mock_CheckNetworkExists, flags=FLAGS):
    mock_CheckNetworkExists.return_value = _network_true
    flags.ignore_package_requirements = True
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_DEFAULT)
    openstackVM = os_virtual_machine.OpenStackVirtualMachine(spec)
    network = openstackVM._CheckFloatingIPNetworkExists('External')
    self.assertEqual(_network_true, network)


  @mock.patch.object(os_virtual_machine.OpenStackVirtualMachine, '_CheckNetworkExists')
  def test_CheckFloatingIPNetworkExistsWithExternal(self, mock_CheckNetworkExists, flags=FLAGS):
    mock_CheckNetworkExists.return_value = _network_external
    flags.ignore_package_requirements = True
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_DEFAULT)
    openstackVM = os_virtual_machine.OpenStackVirtualMachine(spec)
    network = openstackVM._CheckFloatingIPNetworkExists('External')
    self.assertEqual(_network_external, network)


  @mock.patch.object(os_virtual_machine.OpenStackVirtualMachine, '_CheckNetworkExists')
  def test_CheckFloatingIPNetworkExistsWithFail(self, mock_CheckNetworkExists, flags=FLAGS):
    mock_CheckNetworkExists.return_value = _network_fail
    flags.ignore_package_requirements = True
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_DEFAULT)
    openstackVM = os_virtual_machine.OpenStackVirtualMachine(spec)
    with self.assertRaises(errors.Config.InvalidValue):
      openstackVM._CheckFloatingIPNetworkExists('External')


if __name__ == '__main__':
  unittest.main()
