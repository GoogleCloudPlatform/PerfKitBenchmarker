# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for the GCPLocation class."""


import unittest

from perfkitbenchmarker.providers.gcp import util


class GCPLocationTest(unittest.TestCase):
  def testAsMultiRegion(self):
    loc = util.GCPLocation('us')

    self.assertEquals(loc.asMultiRegion(), 'us')
    with self.assertRaises(ValueError):
      loc.asRegion()
    with self.assertRaises(ValueError):
      loc.asZone()

    self.assertEquals(str(loc), 'us')
    self.assertEquals(loc.kind(), util.GCPLocation.MULTIREGION)

  def testAsRegion(self):
    loc = util.GCPLocation('europe-west1')

    self.assertEquals(loc.asMultiRegion(), 'europe')
    self.assertEquals(loc.asRegion(), 'europe-west1')
    with self.assertRaises(ValueError):
      loc.asZone()

    self.assertEquals(str(loc), 'europe-west1')
    self.assertEquals(loc.kind(), util.GCPLocation.REGION)

  def testAsZone(self):
    loc = util.GCPLocation('asia-east1-a')

    self.assertEquals(loc.asMultiRegion(), 'asia')
    self.assertEquals(loc.asRegion(), 'asia-east1')
    self.assertEquals(loc.asZone(), 'asia-east1-a')

    self.assertEquals(str(loc), 'asia-east1-a')
    self.assertEquals(loc.kind(), util.GCPLocation.ZONE)

  def testAsGCSContinent(self):
    loc = util.GCPLocation('europe-west1-b')

    self.assertEquals(loc.asGCSMultiRegion(), 'eu')


if __name__ == '__main__':
  unittest.main()
