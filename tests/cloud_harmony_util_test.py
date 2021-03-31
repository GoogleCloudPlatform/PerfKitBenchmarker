"""Tests for perfkitbenchmarker.cloud_harmony_util."""
import os
import unittest

from absl import flags
# Imported for cloud flag
from perfkitbenchmarker import benchmark_spec  # pylint: disable=unused-import
from perfkitbenchmarker import cloud_harmony_util
# Imported for machine_type and zones flag
from perfkitbenchmarker import pkb  # pylint: disable=unused-import
from perfkitbenchmarker import providers
from tests import pkb_common_test_case


class CloudHarmonyTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    flags.FLAGS['cloud'].parse(providers.GCP)
    flags.FLAGS['zone'].parse(['us-east3'])
    flags.FLAGS['machine_type'].parse('n1-standard-2')
    flags.FLAGS.run_uri = None

  def test_ParseCsvResultsIntoMetadata(self):
    prefix = 'foobar'
    path = os.path.join(os.path.dirname(__file__),
                        'data/cloud_harmony_sample.csv')
    with open(path, mode='r') as fp:
      sample_csv = fp.read()
    metadata = cloud_harmony_util.ParseCsvResultsFromString(sample_csv, prefix)

    self.assertNotEmpty(metadata)
    self.assertLen(metadata, 2)

    for result in metadata:
      for key in result.keys():
        self.assertTrue(key.startswith(prefix))

  def test_GetCommonMetadata(self):
    no_override = cloud_harmony_util.GetCommonMetadata()
    expected_no_override = (
        '--meta_compute_service Google Compute Engine '
        '--meta_compute_service_id google:compute '
        '--meta_instance_id n1-standard-2 '
        '--meta_provider Google Cloud Platform '
        '--meta_provider_id google '
        '--meta_region us-east3 '
        '--meta_zone us-east3 '
        '--meta_test_id None'
        )
    self.assertEqual(no_override, expected_no_override)

    expected_override = (
        '--meta_compute_service Google Compute Engine '
        '--meta_compute_service_id google:compute '
        '--meta_instance_id n1-standard-8 '
        '--meta_provider Google Cloud Platform '
        '--meta_provider_id google '
        '--meta_region us-east3 '
        '--meta_zone us-east3 '
        '--meta_test_id None'
        )
    override = cloud_harmony_util.GetCommonMetadata(
        {'meta_instance_id': 'n1-standard-8'})
    self.assertEqual(override, expected_override)

if __name__ == '__main__':
  unittest.main()
