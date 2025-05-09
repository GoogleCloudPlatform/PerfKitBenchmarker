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
from perfkitbenchmarker.providers.gcp import firestore
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class ConstructFirestoreTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc("""
    example_resource:
      non_relational_db:
        service_type: firestore
        database_id: test-database
        location: test-location
    """)
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='example_resource'
    )

    test_bm_spec.ConstructNonRelationalDb()

    instance = test_bm_spec.non_relational_db
    with self.subTest('service_type'):
      self.assertIsInstance(instance, firestore.Firestore)
      self.assertEqual(instance.SERVICE_TYPE, 'firestore')
    with self.subTest('database_id'):
      self.assertEqual(instance.database_id, 'test-database')
    with self.subTest('location'):
      self.assertEqual(instance.location, 'test-location')

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc("""
    example_resource:
      non_relational_db:
        service_type: firestore
    """)
    FLAGS['gcp_firestore_database_id'].parse('test-database')
    FLAGS['gcp_firestore_location'].parse('test-location')
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='example_resource'
    )

    test_bm_spec.ConstructNonRelationalDb()

    instance = test_bm_spec.non_relational_db
    with self.subTest('service_type'):
      self.assertIsInstance(instance, firestore.Firestore)
      self.assertEqual(instance.SERVICE_TYPE, 'firestore')
    with self.subTest('database_id'):
      self.assertEqual(instance.database_id, 'test-database')
    with self.subTest('location'):
      self.assertEqual(instance.location, 'test-location')


if __name__ == '__main__':
  unittest.main()
