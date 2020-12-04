"""Tests for google3.third_party.py.perfkitbenchmarker.providers.gcp.gcp_spanner."""

import unittest
from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker.providers.gcp import gcp_spanner
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class SpannerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    pass

  @flagsaver.flagsaver
  def testInitFromSpec(self):
    FLAGS.zone = ['us-east1-a']
    spec_args = {
        'service_type': gcp_spanner.DEFAULT_SPANNER_TYPE,
        'name': 'test_instance',
        'description': 'test_description',
        'database': 'test_database',
        'ddl': 'test_schema',
        'nodes': 2,
        'project': 'test_project',
    }
    test_spec = gcp_spanner.SpannerSpec('test_component', None, **spec_args)

    spanner = gcp_spanner.GcpSpannerInstance.FromSpec(test_spec)

    self.assertEqual(spanner._name, 'test_instance')
    self.assertEqual(spanner._description, 'test_description')
    self.assertEqual(spanner._database, 'test_database')
    self.assertEqual(spanner._ddl, 'test_schema')
    self.assertEqual(spanner._nodes, 2)
    self.assertEqual(spanner.project, 'test_project')
    self.assertEqual(spanner._config, 'regional-us-east1')


if __name__ == '__main__':
  unittest.main()
