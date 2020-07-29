"""Tests for perfkitbenchmarker.providers.openstack.os_virtual_machine_test."""

import unittest
from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
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


class TestOpenStackVirtualMachine(pkb_common_test_case.TestOsMixin,
                                  os_virtual_machine.OpenStackVirtualMachine):
  pass


class BaseOpenStackNetworkTest(pkb_common_test_case.PkbCommonTestCase):

  def _CreateBenchmarkSpecFromYaml(self, yaml_string,
                                   benchmark_name=_BENCHMARK_NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    spec = self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)
    spec.disable_interrupt_moderation = False
    spec.disable_rss = False
    spec.zone = 'test-zone'
    spec.cidr = '192.164.1.0/24'
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
    return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, _URI)

  def _CreateTestOpenStackVm(self):
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_DEFAULT)
    return TestOpenStackVirtualMachine(spec)


class OpenStackVirtualMachineTest(BaseOpenStackNetworkTest):

  def setUp(self):
    super(OpenStackVirtualMachineTest, self).setUp()
    self.mock_check_network_exists = self.enter_context(mock.patch.object(
        os_virtual_machine.OpenStackVirtualMachine,
        '_CheckNetworkExists'))
    FLAGS.ignore_package_requirements = True
    self.openstack_vm = self._CreateTestOpenStackVm()

  def test_CheckFloatingIPNetworkExistsWithTrue(self):
    self.mock_check_network_exists.return_value = _network_true
    network = self.openstack_vm._CheckFloatingIPNetworkExists('External')
    self.assertEqual(_network_true, network)

  def test_CheckFloatingIPNetworkExistsWithExternal(self):
    self.mock_check_network_exists.return_value = _network_external
    network = self.openstack_vm._CheckFloatingIPNetworkExists('External')
    self.assertEqual(_network_external, network)

  def test_CheckFloatingIPNetworkExistsWithFail(self):
    self.mock_check_network_exists.return_value = _network_fail
    with self.assertRaises(errors.Config.InvalidValue):
      self.openstack_vm._CheckFloatingIPNetworkExists('External')


if __name__ == '__main__':
  unittest.main()
