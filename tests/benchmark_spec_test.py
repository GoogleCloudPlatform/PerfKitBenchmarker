# Copyright 2015 Google Inc. All rights reserved.
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

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import static_virtual_machine as static_vm
from perfkitbenchmarker.aws import aws_virtual_machine as aws_vm
from perfkitbenchmarker.gcp import gce_virtual_machine as gce_vm

NAME = 'name'
RUN_URI = 'runuri'
UID = '0'
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
          not_a_vm_parameter: 4
"""


class ConstructVmsTestCase(unittest.TestCase):

  def setUp(self):
    # Reset the current benchmark spec.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def testSimpleConfig(self):
    config = configs.LoadConfig(SIMPLE_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, RUN_URI, UID)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 1)
    vm = spec.vms[0]

    self.assertEqual(vm.machine_type, 'n1-standard-4')
    self.assertEqual(vm.zone, 'us-central1-c')
    self.assertEqual(vm.project, 'my-project')

  def testMultiCloud(self):
    config = configs.LoadConfig(MULTI_CLOUD_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, RUN_URI, UID)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 2)
    self.assertIsInstance(spec.vm_groups['group1'][0], aws_vm.AwsVirtualMachine)
    self.assertIsInstance(spec.vm_groups['group2'][0], gce_vm.GceVirtualMachine)

  def testStaticVms(self):
    config = configs.LoadConfig(STATIC_VM_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, RUN_URI, UID)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 4)

    vm0 = spec.vm_groups['group1'][0]
    vm1, vm2, vm3 = spec.vm_groups['group2']

    self.assertIsInstance(vm0, gce_vm.GceVirtualMachine)
    self.assertIsInstance(vm1, static_vm.StaticVirtualMachine)
    self.assertIsInstance(vm2, static_vm.RhelBasedStaticVirtualMachine)
    self.assertIsInstance(vm3, gce_vm.GceVirtualMachine)

    self.assertEqual(vm2.disk_specs[0].mount_point, '/scratch')

  def testBadParameter(self):
    config = configs.LoadConfig(BAD_VM_PARAMETER_CONFIG, {}, NAME)
    spec = benchmark_spec.BenchmarkSpec(config, NAME, RUN_URI, UID)
    with self.assertRaises(ValueError):
      spec.ConstructVirtualMachines()
