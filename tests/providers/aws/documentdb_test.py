# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

import inspect
import unittest

from absl import flags
from perfkitbenchmarker.providers.aws import documentdb
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class ConstructDocumentDbTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc("""
    example_resource:
      non_relational_db:
        service_type: documentdb
        name: test-database
        zones: [us-east-2a,us-east-2b]
        db_instance_class: test-instance-class
        replica_count: 1
        tls: true
    """)
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='example_resource'
    )

    test_bm_spec.ConstructNonRelationalDb()

    instance = test_bm_spec.non_relational_db
    with self.subTest('service_type'):
      self.assertIsInstance(instance, documentdb.DocumentDb)
      self.assertEqual(instance.SERVICE_TYPE, 'documentdb')
    with self.subTest('name'):
      self.assertEqual(instance.name, 'test-database')
    with self.subTest('zones'):
      self.assertEqual(instance.zones, ['us-east-2a', 'us-east-2b'])
    with self.subTest('db_instance_class'):
      self.assertEqual(instance.db_instance_class, 'test-instance-class')
    with self.subTest('replica_count'):
      self.assertEqual(instance.replica_count, 1)
    with self.subTest('tls'):
      self.assertTrue(instance.tls_enabled)

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc("""
    example_resource:
      non_relational_db:
        service_type: documentdb
    """)
    FLAGS['aws_documentdb_cluster_name'].parse('test-database')
    FLAGS['aws_documentdb_zones'].parse(['us-east-2a', 'us-east-2b'])
    FLAGS['aws_documentdb_instance_class'].parse('test-instance-class')
    FLAGS['aws_documentdb_replica_count'].parse(1)
    FLAGS['aws_documentdb_tls'].parse(True)
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='example_resource'
    )

    test_bm_spec.ConstructNonRelationalDb()

    instance = test_bm_spec.non_relational_db
    with self.subTest('service_type'):
      self.assertIsInstance(instance, documentdb.DocumentDb)
      self.assertEqual(instance.SERVICE_TYPE, 'documentdb')
    with self.subTest('name'):
      self.assertEqual(instance.name, 'test-database')
    with self.subTest('zones'):
      self.assertEqual(instance.zones, ['us-east-2a', 'us-east-2b'])
    with self.subTest('db_instance_class'):
      self.assertEqual(instance.db_instance_class, 'test-instance-class')
    with self.subTest('replica_count'):
      self.assertEqual(instance.replica_count, 1)
    with self.subTest('tls'):
      self.assertTrue(instance.tls_enabled)


if __name__ == '__main__':
  unittest.main()
