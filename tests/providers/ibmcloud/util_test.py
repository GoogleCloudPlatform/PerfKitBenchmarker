# Lint as: python3
"""Tests for perfkitbenchmarker.providers.ibmcloud.util."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import mock

from perfkitbenchmarker.providers.ibmcloud import util


class IbmcloudUtilTest(unittest.TestCase):

  def setUp(self):
    super(IbmcloudUtilTest, self).setUp()
    self.expected_subet_index = 2

  def testGetSubnetIndex(self):
    subnetx = '10.103.20.0/24'
    self.assertEqual(self.expected_subet_index,
                     util.GetSubnetIndex(subnetx))

  def testGetBaseOs(self):
    data = {'name': 'debian10'}
    self.assertEqual('debian',
                     util.GetBaseOs(data))
    data = {'name': 'myos'}
    self.assertEqual('unknown',
                     util.GetBaseOs(data))

  def testGetOsInfo(self):
    data = {'name': 'Debian 10', 'operating_system': {'name': 'debian10'}}
    self.assertEqual('debian',
                     util.GetOsInfo(data)['base_os'])


if __name__ == '__main__':
  unittest.main()
