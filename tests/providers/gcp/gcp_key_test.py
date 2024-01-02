"""Tests for providers/gcp/gcp_key.py."""

import inspect
import unittest
from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker.providers.gcp import gcp_key
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def _ConstructKeyFromSpec(spec):
  test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
      yaml_string=spec, benchmark_name='provision_key'
  )
  test_bm_spec.ConstructKey()
  return test_bm_spec.key


class ConstructKeyTestCase(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(run_uri='test_uri')
  def testInitialization(self):
    test_spec = inspect.cleandoc("""
        provision_key:
          description: Sample key benchmark
          key:
            cloud: GCP
            purpose: mac
            algorithm: test_algorithm
            protection_level: software
            location: test_location
        """)
    FLAGS['cloud_kms_keyring_name'].parse('test_keyring')
    key_instance = _ConstructKeyFromSpec(test_spec)
    with self.subTest(name='ClassIsCorrect'):
      self.assertIsInstance(key_instance, gcp_key.GcpKey)
    with self.subTest(name='Purpose'):
      self.assertEqual(key_instance.purpose, 'mac')
    with self.subTest(name='Algorithm'):
      self.assertEqual(key_instance.algorithm, 'test_algorithm')
    with self.subTest(name='ProtectionLevel'):
      self.assertEqual(key_instance.protection_level, 'software')
    with self.subTest(name='Location'):
      self.assertEqual(key_instance.location, 'test_location')
    with self.subTest(name='keyring_name'):
      self.assertEqual(key_instance.keyring_name, 'test_keyring')

  @flagsaver.flagsaver(run_uri='test_uri')
  def testInitializationDefaults(self):
    test_spec = inspect.cleandoc("""
        provision_key:
          description: Sample key benchmark
          key:
            cloud: GCP
        """)
    key_instance = _ConstructKeyFromSpec(test_spec)
    with self.subTest(name='ClassIsCorrect'):
      self.assertIsInstance(key_instance, gcp_key.GcpKey)
    with self.subTest(name='Purpose'):
      self.assertEqual(key_instance.purpose, gcp_key._DEFAULT_PURPOSE)
    with self.subTest(name='Algorithm'):
      self.assertEqual(key_instance.algorithm, gcp_key._DEFAULT_ALGORITHM)
    with self.subTest(name='ProtectionLevel'):
      self.assertEqual(
          key_instance.protection_level, gcp_key._DEFAULT_PROTECTION_LEVEL
      )
    with self.subTest(name='Location'):
      self.assertEqual(key_instance.location, gcp_key._DEFAULT_LOCATION)
    with self.subTest(name='keyring_name'):
      self.assertEqual(key_instance.keyring_name, gcp_key._DEFAULT_KEYRING)


if __name__ == '__main__':
  unittest.main()
