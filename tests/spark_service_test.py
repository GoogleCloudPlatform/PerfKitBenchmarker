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

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import pkb
from perfkitbenchmarker import providers
from perfkitbenchmarker import spark_service
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_emr
from perfkitbenchmarker.providers.gcp import gcp_dataproc
from tests import mock_flags


FLAGS = flags.FLAGS

NAME = 'name'
UID = 'name0'
DATAPROC_CONFIG = """
name:
  spark_service:
    service_type: managed
    machine_type: n1-standard-4
    num_workers: 4
"""

EMR_CONFIG = """
name:
  spark_service:
    service_type: managed
    machine_type: m1.large
    num_workers: 4
"""

PKB_MANAGED_CONFIG = """
name:
  spark_service:
    service_type: pkb_managed
    num_workers: 5
"""


class _BenchmarkSpecTestCase(unittest.TestCase):

  def setUp(self):
    self._mocked_flags = mock_flags.MockFlags()
    self._mocked_flags.cloud = providers.GCP
    self._mocked_flags.os_type = os_types.DEBIAN
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CreateBenchmarkSpecFromYaml(self, yaml_string, benchmark_name=NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    return self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)

  def _CreateBenchmarkSpecFromConfigDict(self, config_dict, benchmark_name):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        benchmark_name, flag_values=self._mocked_flags, **config_dict)
    return benchmark_spec.BenchmarkSpec(config_spec, benchmark_name, UID)


class ConstructSparkServiceTestCase(_BenchmarkSpecTestCase):

  def setUp(self):
    super(ConstructSparkServiceTestCase, self).setUp()
    pkb._InitializeRunUri()

  def testDataprocConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(DATAPROC_CONFIG)
    spec.ConstructVirtualMachines()
    spec.ConstructSparkService()
    self.assertTrue(hasattr(spec, 'spark_service'))
    self.assertTrue(spec.spark_service is not None)
    self.assertEqual(len(spec.vms), 0)
    self.assertEqual(spec.config.spark_service.num_workers, 4,
                     str(spec.config.spark_service.__dict__))
    self.assertEqual(spec.config.spark_service.service_type,
                     spark_service.PROVIDER_MANAGED)
    self.assertEqual(spec.config.spark_service.machine_type, 'n1-standard-4',
                     str(spec.config.spark_service.__dict__))
    self.assertTrue(isinstance(spec.spark_service,
                               gcp_dataproc.GcpDataproc))

  def testEMRConfig(self):
    self._mocked_flags.cloud = providers.AWS
    spec = self._CreateBenchmarkSpecFromYaml(EMR_CONFIG)
    spec.ConstructVirtualMachines()
    spec.ConstructSparkService()
    self.assertTrue(hasattr(spec, 'spark_service'))
    self.assertTrue(spec.spark_service is not None)
    self.assertEqual(len(spec.vms), 0)
    self.assertEqual(spec.config.spark_service.num_workers, 4,
                     str(spec.config.spark_service.__dict__))
    self.assertEqual(spec.config.spark_service.service_type,
                     spark_service.PROVIDER_MANAGED)
    self.assertEqual(spec.config.spark_service.machine_type, 'm1.large',
                     str(spec.config.spark_service.__dict__))
    self.assertTrue(isinstance(spec.spark_service,
                               aws_emr.AwsEMR))

  def testPkbManaged(self):
    spec = self._CreateBenchmarkSpecFromYaml(PKB_MANAGED_CONFIG)
    self.assertEqual(spec.config.spark_service.num_workers, 5,
                     str(spec.config.spark_service.__dict__))
    self.assertEqual(spec.config.spark_service.service_type,
                     spark_service.PKB_MANAGED)

    spec.ConstructVirtualMachines()
    self.assertEqual(len(spec.vms), 0)
    spec.ConstructSparkService()
    self.assertEqual(spec.spark_service.num_workers, 5)
    self.assertTrue(isinstance(spec.spark_service,
                               spark_service.PkbSparkService))


if __name__ == '__main__':
  unittest.main()
