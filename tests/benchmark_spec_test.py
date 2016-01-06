# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.benchmark_spec."""

import unittest
import mock_flags

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import static_virtual_machine as static_vm
from perfkitbenchmarker.providers.aws import aws_virtual_machine as aws_vm
from perfkitbenchmarker.providers.gcp import gce_virtual_machine as gce_vm
from perfkitbenchmarker.linux_benchmarks import iperf_benchmark

NAME = 'name'
UID = 'name0'
SIMPLE_CONFIG = """
name:
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-central1-c
          project: my-project
"""
MULTI_CLOUD_CONFIG = """
name:
  vm_groups:
    group1:
      cloud: AWS
      vm_spec:
        AWS:
          machine_type: c3.2xlarge
          zone: us-east-1a
    group2:
      cloud: GCP
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          project: my-project
"""
STATIC_VM_CONFIG = """
static_vms:
  - &vm1
    ip_address: 1.1.1.1
    ssh_private_key: /path/to/key1
    user_name: user1
name:
  vm_groups:
    group1:
      vm_spec: *default_single_core
    group2:
      vm_count: 3
      vm_spec: *default_single_core
      static_vms:
       - *vm1
       - ip_address: 2.2.2.2
         os_type: rhel
         ssh_private_key: /path/to/key2
         user_name: user2
         disk_specs:
           - mount_point: /scratch
"""
BAD_VM_PARAMETER_CONFIG = """
name:
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          not_a_vm_parameter: 4
"""
BAD_DISK_PARAMETER_CONFIG = """
name:
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
      disk_spec:
        GCP:
          not_a_disk_parameter: 4
"""
VALID_CONFIG_WITH_DISK_SPEC = """
name:
  vm_groups:
    default:
      disk_count: 3
      disk_spec:
        GCP:
          disk_size: 75
      vm_count: 2
      vm_spec:
        GCP:
          machine_type: n1-standard-4
"""
ALWAYS_SUPPORTED = 'iperf'
NEVER_SUPPORTED = 'mysql_service'
NO_SUPPORT_INFO = 'this_is_not_a_benchmark'


class ConstructVmsTestCase(unittest.TestCase):

  def setUp(self):
    # Reset the current benchmark spec.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def testSimpleConfig(self):
    config = configs.LoadConfig(SIMPLE_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 1)
    vm = spec.vms[0]

    self.assertEqual(vm.machine_type, 'n1-standard-4')
    self.assertEqual(vm.zone, 'us-central1-c')
    self.assertEqual(vm.project, 'my-project')
    self.assertEqual(vm.disk_specs, [])

  def testMultiCloud(self):
    config = configs.LoadConfig(MULTI_CLOUD_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 2)
    self.assertIsInstance(spec.vm_groups['group1'][0], aws_vm.AwsVirtualMachine)
    self.assertIsInstance(spec.vm_groups['group2'][0], gce_vm.GceVirtualMachine)

  def testStaticVms(self):
    config = configs.LoadConfig(STATIC_VM_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 4)

    vm0 = spec.vm_groups['group1'][0]
    vm1, vm2, vm3 = spec.vm_groups['group2']

    self.assertIsInstance(vm0, gce_vm.GceVirtualMachine)
    self.assertIsInstance(vm1, static_vm.StaticVirtualMachine)
    self.assertIsInstance(vm2, static_vm.RhelBasedStaticVirtualMachine)
    self.assertIsInstance(vm3, gce_vm.GceVirtualMachine)

    self.assertEqual(vm2.disk_specs[0].mount_point, '/scratch')

  def testBadVmParameter(self):
    config = configs.LoadConfig(BAD_VM_PARAMETER_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      spec.ConstructVirtualMachines()
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in '
        'name.vm_groups.default.vm_spec.GCP: not_a_vm_parameter.'))

  def testBadDiskParameter(self):
    config = configs.LoadConfig(BAD_DISK_PARAMETER_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      spec.ConstructVirtualMachines()
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in '
        'name.vm_groups.default.disk_spec.GCP: not_a_disk_parameter.'))

  def testValidConfigWithDiskSpec(self):
    config = configs.LoadConfig(VALID_CONFIG_WITH_DISK_SPEC, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, UID)
    spec.ConstructVirtualMachines()
    vms = spec.vm_groups['default']
    self.assertEqual(len(vms), 2)
    for vm in vms:
      self.assertEqual(len(vm.disk_specs), 3)
      self.assertTrue(all(disk_spec.disk_size == 75
                          for disk_spec in vm.disk_specs))


class BenchmarkSupportTestCase(unittest.TestCase):

  def setUp(self):
    # Reset the current benchmark spec.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def createBenchmarkSpec(self, config, benchmark):
    spec = benchmark_spec.BenchmarkSpec(config, benchmark, UID)
    spec.ConstructVirtualMachines()
    return True

  def testBenchmarkSupportFlag(self):
    """ Test the benchmark_compatibility_checking flag

    We use Kubernetes as our test cloud platform because it has
    supported benchmarks (IsBenchmarkSupported returns true)
    unsupported benchmarks (IsBenchmarkSupported returns false)
    and returns None if the benchmark isn't in either list.
    """

    with mock_flags.PatchFlags() as mocked_flags:
      mocked_flags.cloud = 'Kubernetes'
      mocked_flags.os_type = 'debian'
      mocked_flags.machine_type = None
      config = configs.LoadConfig(iperf_benchmark.BENCHMARK_CONFIG,
                                  {}, ALWAYS_SUPPORTED)
      self.assertTrue(self.createBenchmarkSpec(config, ALWAYS_SUPPORTED))
      with self.assertRaises(ValueError):
        self.createBenchmarkSpec(config, NEVER_SUPPORTED)
      with self.assertRaises(ValueError):
        self.createBenchmarkSpec(config, NO_SUPPORT_INFO)

      mocked_flags.benchmark_compatibility_checking = 'permissive'
      self.assertTrue(self.createBenchmarkSpec(config, ALWAYS_SUPPORTED),
                      'benchmark is supported, mode is permissive')
      with self.assertRaises(ValueError):
        self.createBenchmarkSpec(config, NEVER_SUPPORTED)
      self.assertTrue(self.createBenchmarkSpec(config, NO_SUPPORT_INFO))

      mocked_flags.benchmark_compatibility_checking = 'none'
      self.assertTrue(self.createBenchmarkSpec(config, ALWAYS_SUPPORTED))
      self.assertTrue(self.createBenchmarkSpec(config, NEVER_SUPPORTED))
      self.assertTrue(self.createBenchmarkSpec(config, NO_SUPPORT_INFO))
