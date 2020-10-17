# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import providers
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_emr
from perfkitbenchmarker.providers.gcp import gcp_dataproc
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

NAME = 'name'
UID = 'name0'

SERVICE_CONFIG = """
name:
  spark_service:
    service_type: managed
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          boot_disk_size: 500
          zone: us-west1-a
        AWS:
          machine_type: m4.xlarge
          zone: us-west-1
      vm_count: 4
"""

PKB_MANAGED_CONFIG = """
name:
  spark_service:
    service_type: pkb_managed
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          boot_disk_size: 500
        AWS:
          machine_type: m4.xlarge
      vm_count: 2
"""


class _BenchmarkSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(_BenchmarkSpecTestCase, self).setUp()
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

    p = mock.patch(vm_util.__name__ + '.GetTempDir', return_value='/tmp/dir')
    p.start()
    self.addCleanup(p.stop)

  def _CreateBenchmarkSpecFromYaml(self, yaml_string, benchmark_name=NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    return self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)

  def _CreateBenchmarkSpecFromConfigDict(self, config_dict, benchmark_name):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        benchmark_name, flag_values=FLAGS, **config_dict)
    return benchmark_spec.BenchmarkSpec(mock.MagicMock(), config_spec, UID)


class ConstructSparkServiceTestCase(_BenchmarkSpecTestCase):

  def testDataprocConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(SERVICE_CONFIG)
    spec.ConstructSparkService()
    spec.ConstructVirtualMachines()
    self.assertTrue(hasattr(spec, 'spark_service'))
    self.assertTrue(spec.spark_service is not None)
    self.assertEqual(len(spec.vms), 0)
    machine_type = spec.config.spark_service.worker_group.vm_spec.machine_type
    self.assertEqual(spec.config.spark_service.worker_group.vm_count, 4,
                     str(spec.config.spark_service.__dict__))
    self.assertEqual(spec.config.spark_service.service_type,
                     spark_service.PROVIDER_MANAGED)
    self.assertEqual(machine_type,
                     'n1-standard-4', str(spec.config.spark_service.__dict__))
    self.assertTrue(isinstance(spec.spark_service,
                               gcp_dataproc.GcpDataproc))

  def testEMRConfig(self):
    FLAGS.cloud = providers.AWS
    FLAGS.zones = 'us-west-2'
    spec = self._CreateBenchmarkSpecFromYaml(SERVICE_CONFIG)
    spec.ConstructSparkService()
    spec.ConstructVirtualMachines()
    self.assertTrue(hasattr(spec, 'spark_service'))
    self.assertTrue(spec.spark_service is not None)
    self.assertEqual(len(spec.vms), 0)
    self.assertEqual(spec.config.spark_service.worker_group.vm_count, 4,
                     str(spec.config.spark_service.__dict__))
    machine_type = spec.config.spark_service.worker_group.vm_spec.machine_type
    self.assertEqual(spec.config.spark_service.service_type,
                     spark_service.PROVIDER_MANAGED)
    self.assertEqual(machine_type, 'm4.xlarge',
                     str(spec.config.spark_service.__dict__))
    self.assertTrue(isinstance(spec.spark_service, aws_emr.AwsEMR))

  def testPkbManaged(self):
    spec = self._CreateBenchmarkSpecFromYaml(PKB_MANAGED_CONFIG)
    self.assertEqual(spec.config.spark_service.worker_group.vm_count, 2,
                     str(spec.config.spark_service.__dict__))
    self.assertEqual(spec.config.spark_service.service_type,
                     spark_service.PKB_MANAGED)
    spec.ConstructSparkService()
    spec.ConstructVirtualMachines()
    self.assertEqual(len(spec.vms), 3)
    self.assertEqual(len(spec.vm_groups['master_group']), 1)
    self.assertEqual(len(spec.vm_groups['worker_group']), 2)
    self.assertEqual(len(spec.spark_service.vms['worker_group']), 2)
    self.assertEqual(len(spec.spark_service.vms['master_group']), 1)
    self.assertTrue(isinstance(spec.spark_service,
                               spark_service.PkbSparkService))


if __name__ == '__main__':
  unittest.main()
