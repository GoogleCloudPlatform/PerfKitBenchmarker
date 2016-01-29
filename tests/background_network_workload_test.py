# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for background network workload"""

import unittest

from mock import patch

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from tests import mock_flags


NAME = 'ping'
UID = 'name0'

CONFIG_WITH_BACKGROUND_NETWORK = """
ping:
    description: Benchmarks ping latency over internal IP addresses
    vm_groups:
      vm_1:
        vm_spec:
          GCP:
            machine_type: n1-standard-1
      vm_2:
        vm_spec:
          GCP:
            background_network_mbits_per_sec: 300
            machine_type: n1-standard-1
"""

CONFIG_WITH_BACKGROUND_NETWORK_BAD_IPFLAG = """
ping:
    description: Benchmarks ping latency over internal IP addresses
    vm_groups:
      vm_1:
        vm_spec:
          GCP:
            machine_type: n1-standard-1
            background_network_mbits_per_sec: 200
            background_network_ip_type: BOTH
      vm_2:
        vm_spec:
          GCP:
            background_network_mbits_per_sec: 300
            machine_type: n1-standard-1
"""

CONFIG_WITH_BACKGROUND_NETWORK_IPFLAG = """
ping:
    description: Benchmarks ping latency over internal IP addresses
    vm_groups:
      vm_1:
        vm_spec:
          GCP:
            machine_type: n1-standard-1
      vm_2:
        vm_spec:
          GCP:
            background_network_mbits_per_sec: 300
            background_network_ip_type: INTERNAL
            machine_type: n1-standard-1
"""


class TestBackgroundNetworkWorkload(unittest.TestCase):

  def setUp(self):
    super(TestBackgroundNetworkWorkload, self).setUp()
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.os_type = os_types.DEBIAN
    self.mocked_flags.cloud = providers.GCP
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CheckVMFromSpec(self, spec, num_working):
    vm0 = spec.vms[0]
    with patch.object(vm0.__class__, 'RemoteCommand'):
      with patch.object(vm0.__class__, 'Install'):
        with patch.object(vm0.__class__, 'AllowPort'):
          expected_install_post_prepare = num_working
          # one call for client, one call for server, one call for each pid
          expected_remote_post_start = 4 * num_working
          # all the start calls, plus stop server and stop client
          expected_remote_post_stop = (expected_remote_post_start +
                                       2 * num_working)
          side_effect_list = []
          for i in range(expected_remote_post_stop):
            side_effect_list.append((str(i), ' '))
          vm0.RemoteCommand.side_effect = side_effect_list
          spec.Prepare()
          self.assertEqual(vm0.Install.call_count,
                           expected_install_post_prepare)
          spec.StartBackgroundWorkload()
          self.assertEqual(vm0.RemoteCommand.call_count,
                           expected_remote_post_start)
          self.assertEqual(vm0.AllowPort.call_count,
                           num_working)
          spec.StopBackgroundWorkload()
          self.assertEqual(vm0.RemoteCommand.call_count,
                           expected_remote_post_stop)

  def makeSpec(self, yaml_benchmark_config=ping_benchmark.BENCHMARK_CONFIG):
    config = configs.LoadConfig(yaml_benchmark_config, {}, NAME)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=self.mocked_flags, **config)
    spec = benchmark_spec.BenchmarkSpec(config_spec, NAME, UID)
    spec.ConstructVirtualMachines()
    return spec

  def testWindowsVMCausesError(self):
    """ windows vm with background_network_mbits_per_sec raises exception """
    self.mocked_flags['background_network_mbits_per_sec'].Parse(200)
    self.mocked_flags['os_type'].Parse(os_types.WINDOWS)
    spec = self.makeSpec()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.Prepare()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.StartBackgroundWorkload()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.StopBackgroundWorkload()

  def testBackgroundWorkloadVM(self):
    """ Check that the vm background workload calls work """
    self.mocked_flags['background_network_mbits_per_sec'].Parse(200)
    spec = self.makeSpec()
    self._CheckVMFromSpec(spec, 2)

  def testBackgroundWorkloadVanillaConfig(self):
    """ Test that nothing happens with the vanilla config """
    # background_network_mbits_per_sec defaults to None
    spec = self.makeSpec()
    for vm in spec.vms:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, 0)

  def testBackgroundWorkloadWindows(self):
    """ Test that nothing happens with the vanilla config """
    self.mocked_flags.os_type = os_types.WINDOWS
    # background_network_mbits_per_sec defaults to None.
    spec = self.makeSpec()

    for vm in spec.vms:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, 0)

  def testBackgroundWorkloadVanillaConfigFlag(self):
    """ Check that the flag overrides the config """
    self.mocked_flags['background_network_mbits_per_sec'].Parse(200)
    spec = self.makeSpec()
    for vm in spec.vms:
      self.assertEqual(vm.background_network_mbits_per_sec, 200)
      self.assertEqual(vm.background_network_ip_type, 'EXTERNAL')
    self._CheckVMFromSpec(spec, 2)

  def testBackgroundWorkloadVanillaConfigFlagIpType(self):
    """ Check that the flag overrides the config """
    self.mocked_flags['background_network_mbits_per_sec'].Parse(200)
    self.mocked_flags['background_network_ip_type'].Parse('INTERNAL')
    spec = self.makeSpec()
    for vm in spec.vms:
      self.assertEqual(vm.background_network_mbits_per_sec, 200)
      self.assertEqual(vm.background_network_ip_type, 'INTERNAL')
    self._CheckVMFromSpec(spec, 2)

  def testBackgroundWorkloadVanillaConfigBadIpTypeFlag(self):
    """ Check that the flag overrides the config """
    self.mocked_flags['background_network_mbits_per_sec'].Parse(200)
    self.mocked_flags['background_network_ip_type'].Parse('IAMABADFLAGVALUE')
    config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark_config_spec.BenchmarkConfigSpec(
          NAME, flag_values=self.mocked_flags, **config)

  def testBackgroundWorkloadConfig(self):
    """ Check that the config can be used to set the background iperf """
    spec = self.makeSpec(
        yaml_benchmark_config=CONFIG_WITH_BACKGROUND_NETWORK)
    for vm in spec.vm_groups['vm_1']:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    for vm in spec.vm_groups['vm_2']:
      self.assertEqual(vm.background_network_mbits_per_sec, 300)
    self.assertEqual(vm.background_network_ip_type, 'EXTERNAL')
    self._CheckVMFromSpec(spec, 1)

  def testBackgroundWorkloadConfigBadIp(self):
    """ Check that the config with invalid ip type gets an error """
    config = configs.LoadConfig(CONFIG_WITH_BACKGROUND_NETWORK_BAD_IPFLAG,
                                {}, NAME)
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark_config_spec.BenchmarkConfigSpec(
          NAME, flag_values=self.mocked_flags, **config)

  def testBackgroundWorkloadConfigGoodIp(self):
    """ Check that the config can be used with an internal ip address """
    spec = self.makeSpec(
        yaml_benchmark_config=CONFIG_WITH_BACKGROUND_NETWORK_IPFLAG)
    for vm in spec.vm_groups['vm_1']:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    for vm in spec.vm_groups['vm_2']:
      self.assertEqual(vm.background_network_mbits_per_sec, 300)
      self.assertEqual(vm.background_network_ip_type, 'INTERNAL')
    self._CheckVMFromSpec(spec, 1)


if __name__ == '__main__':
  unittest.main()
