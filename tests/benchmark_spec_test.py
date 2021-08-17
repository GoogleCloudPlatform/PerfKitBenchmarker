# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

import inspect
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import pkb  # pylint: disable=unused-import # noqa
from perfkitbenchmarker import providers
from perfkitbenchmarker import static_virtual_machine as static_vm
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import iperf_benchmark
from perfkitbenchmarker.providers.aws import aws_virtual_machine as aws_vm
from perfkitbenchmarker.providers.gcp import gce_virtual_machine as gce_vm
from perfkitbenchmarker.providers.gcp import gcp_spanner
from tests import pkb_common_test_case


flags.DEFINE_integer('benchmark_spec_test_flag', 0, 'benchmark_spec_test flag.')

FLAGS = flags.FLAGS

NAME = 'cluster_boot'
UID = 'name0'
SIMPLE_CONFIG = """
cluster_boot:
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-central1-c
          project: my-project
"""
MULTI_CLOUD_CONFIG = """
cluster_boot:
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
cluster_boot:
  vm_groups:
    group1:
      vm_spec: *default_single_core
    group2:
      vm_count: 3
      vm_spec: *default_single_core
      static_vms:
       - *vm1
       - ip_address: 2.2.2.2
         os_type: rhel7
         ssh_private_key: /path/to/key2
         user_name: user2
         disk_specs:
           - mount_point: /scratch
"""
VALID_CONFIG_WITH_DISK_SPEC = """
cluster_boot:
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
NEVER_SUPPORTED = 'sysbench'

_SIMPLE_EDW_CONFIG = """
edw_benchmark:
  description: Sample edw benchmark
  edw_service:
    type: snowflake_aws
    cluster_identifier: _fake_cluster_id_
  vm_groups:
    client:
      vm_spec: *default_single_core
"""


class _BenchmarkSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(_BenchmarkSpecTestCase, self).setUp()
    FLAGS.cloud = providers.GCP
    FLAGS.temp_dir = 'tmp'
    FLAGS.ignore_package_requirements = True
    self.addCleanup(context.SetThreadBenchmarkSpec, None)


class ConstructEdwServiceTestCase(_BenchmarkSpecTestCase):

  def testSimpleConfig(self):
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=_SIMPLE_EDW_CONFIG, benchmark_name='edw_benchmark')
    spec.ConstructEdwService()
    self.assertEqual('snowflake_aws', spec.edw_service.SERVICE_TYPE)
    self.assertIsInstance(spec.edw_service, providers.aws.snowflake.Snowflake)


class ConstructSpannerTestCase(_BenchmarkSpecTestCase):

  def setUp(self):
    super().setUp()
    test_spec = inspect.cleandoc("""
    cloud_spanner_ycsb:
      description: Sample spanner benchmark
      spanner:
        service_type: default
        enable_freeze_restore: True
        delete_on_freeze_error: True
        create_on_restore_error: True
    """)
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_spanner_ycsb')

  def testInitialization(self):
    self.test_bm_spec.ConstructSpanner()
    spanner_instance = self.test_bm_spec.spanner
    self.assertIsInstance(spanner_instance, gcp_spanner.GcpSpannerInstance)
    self.assertEqual(spanner_instance.SERVICE_TYPE, 'default')
    self.assertTrue(spanner_instance.enable_freeze_restore)
    self.assertTrue(spanner_instance.delete_on_freeze_error)
    self.assertTrue(spanner_instance.create_on_restore_error)

  @flagsaver.flagsaver
  def testRestoreInstanceCopiedFromPreviousSpec(self):
    restore_spanner_spec = inspect.cleandoc("""
    cloud_spanner_ycsb:
      spanner:
        name: restore_spanner
        service_type: default
    """)
    # Set up the restore spec Spanner instance
    self.test_bm_spec.restore_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=restore_spanner_spec, benchmark_name='cloud_spanner_ycsb')
    self.test_bm_spec.restore_spec.ConstructSpanner()

    self.test_bm_spec.ConstructSpanner()

    self.assertEqual(self.test_bm_spec.spanner.name, 'restore_spanner')


class ConstructVmsTestCase(_BenchmarkSpecTestCase):

  def testSimpleConfig(self):
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(SIMPLE_CONFIG)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 1)
    vm = spec.vms[0]

    self.assertEqual(vm.machine_type, 'n1-standard-4')
    self.assertEqual(vm.zone, 'us-central1-c')
    self.assertEqual(vm.project, 'my-project')
    self.assertEqual(vm.disk_specs, [])

  def testMultiCloud(self):
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(MULTI_CLOUD_CONFIG)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 2)
    self.assertIsInstance(spec.vm_groups['group1'][0], aws_vm.AwsVirtualMachine)
    self.assertIsInstance(spec.vm_groups['group2'][0], gce_vm.GceVirtualMachine)

  def testStaticVms(self):
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(STATIC_VM_CONFIG)
    spec.ConstructVirtualMachines()

    self.assertEqual(len(spec.vms), 4)

    vm0 = spec.vm_groups['group1'][0]
    vm1, vm2, vm3 = spec.vm_groups['group2']

    self.assertIsInstance(vm0, gce_vm.GceVirtualMachine)
    self.assertIsInstance(vm1, static_vm.StaticVirtualMachine)
    self.assertIsInstance(vm2, static_vm.Rhel7BasedStaticVirtualMachine)
    self.assertIsInstance(vm3, gce_vm.GceVirtualMachine)

    self.assertEqual(vm2.disk_specs[0].mount_point, '/scratch')

  def testValidConfigWithDiskSpec(self):
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        VALID_CONFIG_WITH_DISK_SPEC)
    spec.ConstructVirtualMachines()
    vms = spec.vm_groups['default']
    self.assertEqual(len(vms), 2)
    for vm in vms:
      self.assertEqual(len(vm.disk_specs), 3)
      self.assertTrue(all(disk_spec.disk_size == 75
                          for disk_spec in vm.disk_specs))

  def testZonesFlag(self):
    FLAGS.zones = ['us-east-1b', 'zone2']
    FLAGS.extra_zones = []
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(MULTI_CLOUD_CONFIG)
    spec.ConstructVirtualMachines()
    self.assertEqual(len(spec.vms), 2)
    self.assertEqual(spec.vm_groups['group1'][0].zone, 'us-east-1b')
    self.assertEqual(spec.vm_groups['group2'][0].zone, 'zone2')

  def testZonesFlagWithZoneFlag(self):
    FLAGS.zones = ['us-east-1b']
    FLAGS.extra_zones = []
    FLAGS.zone = ['us-west-2b']
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(MULTI_CLOUD_CONFIG)
    spec.ConstructVirtualMachines()
    self.assertEqual(len(spec.vms), 2)
    self.assertEqual(spec.vm_groups['group1'][0].zone, 'us-east-1b')
    self.assertEqual(spec.vm_groups['group2'][0].zone, 'us-west-2b')


class BenchmarkSupportTestCase(_BenchmarkSpecTestCase):

  def createBenchmarkSpec(self, config, benchmark):
    spec = pkb_common_test_case.CreateBenchmarkSpecFromConfigDict(
        config, benchmark)
    spec.ConstructVirtualMachines()
    return True

  def testBenchmarkSupportFlag(self):
    """Test the benchmark_compatibility_checking flag.

    We use Kubernetes as our test cloud platform because it has
    supported benchmarks (IsBenchmarkSupported returns true)
    unsupported benchmarks (IsBenchmarkSupported returns false)
    and returns None if the benchmark isn't in either list.
    """
    FLAGS.cloud = 'Kubernetes'
    config = configs.LoadConfig(iperf_benchmark.BENCHMARK_CONFIG, {},
                                ALWAYS_SUPPORTED)
    self.assertTrue(self.createBenchmarkSpec(config, ALWAYS_SUPPORTED))
    with self.assertRaises(ValueError):
      self.createBenchmarkSpec(config, NEVER_SUPPORTED)

    FLAGS.benchmark_compatibility_checking = 'permissive'
    self.assertTrue(
        self.createBenchmarkSpec(config, ALWAYS_SUPPORTED),
        'benchmark is supported, mode is permissive')
    with self.assertRaises(ValueError):
      self.createBenchmarkSpec(config, NEVER_SUPPORTED)

    FLAGS.benchmark_compatibility_checking = 'none'
    self.assertTrue(self.createBenchmarkSpec(config, ALWAYS_SUPPORTED))
    self.assertTrue(self.createBenchmarkSpec(config, NEVER_SUPPORTED))


class RedirectGlobalFlagsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testNoFlagOverride(self):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=FLAGS, vm_groups={})
    spec = benchmark_spec.BenchmarkSpec(mock.MagicMock(), config_spec, UID)
    self.assertEqual(FLAGS.benchmark_spec_test_flag, 0)
    with spec.RedirectGlobalFlags():
      self.assertEqual(FLAGS.benchmark_spec_test_flag, 0)
    self.assertEqual(FLAGS.benchmark_spec_test_flag, 0)

  def testFlagOverride(self):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        NAME, flag_values=FLAGS, flags={'benchmark_spec_test_flag': 1},
        vm_groups={})
    spec = benchmark_spec.BenchmarkSpec(mock.MagicMock(), config_spec, UID)
    self.assertEqual(FLAGS.benchmark_spec_test_flag, 0)
    with spec.RedirectGlobalFlags():
      self.assertEqual(FLAGS.benchmark_spec_test_flag, 1)
      FLAGS.benchmark_spec_test_flag = 2
      self.assertEqual(FLAGS.benchmark_spec_test_flag, 2)
    self.assertEqual(FLAGS.benchmark_spec_test_flag, 0)


if __name__ == '__main__':
  unittest.main()
