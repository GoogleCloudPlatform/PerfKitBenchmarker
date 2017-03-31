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

import itertools
import unittest

import contextlib2
from mock import patch

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark
from perfkitbenchmarker.providers.gcp import util
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

_GROUP_1 = 'vm_1'
_GROUP_2 = 'vm_2'
_MOCKED_VM_FUNCTIONS = 'AllowPort', 'Install', 'RemoteCommand'


class TestBackgroundNetworkWorkload(unittest.TestCase):

  def setUp(self):
    super(TestBackgroundNetworkWorkload, self).setUp()
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.os_type = os_types.DEBIAN
    self.mocked_flags.cloud = providers.GCP
    self.mocked_flags.temp_dir = 'tmp'
    p = patch(util.__name__ + '.GetDefaultProject')
    p.start()
    self.addCleanup(p.stop)
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CheckVmCallCounts(self, spec, working_groups, working_expected_counts,
                         non_working_groups, non_working_expected_counts):
    # TODO(skschneider): This is also used in TestBackgroundNetworkWorkload.
    # Consider moving to a shared function or base class.
    expected_call_counts = {group: working_expected_counts
                            for group in working_groups}
    expected_call_counts.update({group: non_working_expected_counts
                                 for group in non_working_groups})
    for group_name, vm_expected_call_counts in expected_call_counts.iteritems():
      group_vms = spec.vm_groups[group_name]
      self.assertEqual(len(group_vms), 1,
                       msg='VM group "{0}" had {1} VMs'.format(group_name,
                                                               len(group_vms)))
      vm = group_vms[0]
      iter_mocked_functions = itertools.izip_longest(_MOCKED_VM_FUNCTIONS,
                                                     vm_expected_call_counts)
      for function_name, expected_call_count in iter_mocked_functions:
        call_count = getattr(vm, function_name).call_count
        self.assertEqual(call_count, expected_call_count, msg=(
            'Expected {0} from VM group "{1}" to be called {2} times, but it '
            'was called {3} times.'.format(function_name, group_name,
                                           expected_call_count, call_count)))

  def _CheckVMFromSpec(self, spec, working_groups=(), non_working_groups=()):
    with contextlib2.ExitStack() as stack:
      for vm in spec.vms:
        for function_name in _MOCKED_VM_FUNCTIONS:
          stack.enter_context(patch.object(vm, function_name))
        vm.RemoteCommand.side_effect = itertools.repeat(('0', ''))

      working, non_working = working_groups, non_working_groups
      self._CheckVmCallCounts(spec, working, (0, 0, 0), non_working, (0, 0, 0))

      spec.Prepare()
      self._CheckVmCallCounts(spec, working, (0, 1, 0), non_working, (0, 0, 0))

      spec.StartBackgroundWorkload()
      self._CheckVmCallCounts(spec, working, (1, 1, 4), non_working, (0, 0, 0))

      spec.StopBackgroundWorkload()
      self._CheckVmCallCounts(spec, working, (1, 1, 6), non_working, (0, 0, 0))

  def makeSpec(self, yaml_benchmark_config=ping_benchmark.BENCHMARK_CONFIG):
    config = configs.LoadConfig(yaml_benchmark_config, {}, NAME)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=self.mocked_flags, **config)
    spec = benchmark_spec.BenchmarkSpec(ping_benchmark, config_spec, UID)
    spec.ConstructVirtualMachines()
    return spec

  def testWindowsVMCausesError(self):
    """ windows vm with background_network_mbits_per_sec raises exception """
    self.mocked_flags['background_network_mbits_per_sec'].parse(200)
    self.mocked_flags['os_type'].parse(os_types.WINDOWS)
    spec = self.makeSpec()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.Prepare()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.StartBackgroundWorkload()
    with self.assertRaisesRegexp(Exception, 'NotImplementedError'):
      spec.StopBackgroundWorkload()

  def testBackgroundWorkloadVM(self):
    """ Check that the vm background workload calls work """
    self.mocked_flags['background_network_mbits_per_sec'].parse(200)
    spec = self.makeSpec()
    self._CheckVMFromSpec(spec, working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadVanillaConfig(self):
    """ Test that nothing happens with the vanilla config """
    # background_network_mbits_per_sec defaults to None
    spec = self.makeSpec()
    for vm in spec.vms:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, non_working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadWindows(self):
    """ Test that nothing happens with the vanilla config """
    self.mocked_flags.os_type = os_types.WINDOWS
    # background_network_mbits_per_sec defaults to None.
    spec = self.makeSpec()

    for vm in spec.vms:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    self._CheckVMFromSpec(spec, non_working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadVanillaConfigFlag(self):
    """ Check that the flag overrides the config """
    self.mocked_flags['background_network_mbits_per_sec'].parse(200)
    spec = self.makeSpec()
    for vm in spec.vms:
      self.assertEqual(vm.background_network_mbits_per_sec, 200)
      self.assertEqual(vm.background_network_ip_type, 'EXTERNAL')
    self._CheckVMFromSpec(spec, working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadVanillaConfigFlagIpType(self):
    """ Check that the flag overrides the config """
    self.mocked_flags['background_network_mbits_per_sec'].parse(200)
    self.mocked_flags['background_network_ip_type'].parse('INTERNAL')
    spec = self.makeSpec()
    for vm in spec.vms:
      self.assertEqual(vm.background_network_mbits_per_sec, 200)
      self.assertEqual(vm.background_network_ip_type, 'INTERNAL')
    self._CheckVMFromSpec(spec, working_groups=(_GROUP_1, _GROUP_2))

  def testBackgroundWorkloadVanillaConfigBadIpTypeFlag(self):
    """ Check that the flag overrides the config """
    self.mocked_flags['background_network_mbits_per_sec'].parse(200)
    self.mocked_flags['background_network_ip_type'].parse('IAMABADFLAGVALUE')
    config = configs.LoadConfig(ping_benchmark.BENCHMARK_CONFIG, {}, NAME)
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark_config_spec.BenchmarkConfigSpec(
          NAME, flag_values=self.mocked_flags, **config)

  def testBackgroundWorkloadConfig(self):
    """ Check that the config can be used to set the background iperf """
    spec = self.makeSpec(
        yaml_benchmark_config=CONFIG_WITH_BACKGROUND_NETWORK)
    for vm in spec.vm_groups[_GROUP_1]:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    for vm in spec.vm_groups[_GROUP_2]:
      self.assertEqual(vm.background_network_mbits_per_sec, 300)
    self.assertEqual(vm.background_network_ip_type, 'EXTERNAL')
    self._CheckVMFromSpec(spec, working_groups=[_GROUP_2],
                          non_working_groups=[_GROUP_1])

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
    for vm in spec.vm_groups[_GROUP_1]:
      self.assertIsNone(vm.background_network_mbits_per_sec)
    for vm in spec.vm_groups[_GROUP_2]:
      self.assertEqual(vm.background_network_mbits_per_sec, 300)
      self.assertEqual(vm.background_network_ip_type, 'INTERNAL')
    self._CheckVMFromSpec(spec, working_groups=[_GROUP_2],
                          non_working_groups=[_GROUP_1])


if __name__ == '__main__':
  unittest.main()
