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

"""Tests for the object_storage_service benchmark worker process."""

import itertools
import random
import time
import unittest

import mock

import object_storage_api_tests


class TestSizeDistributionIterator(unittest.TestCase):
  def testPointDistribution(self):
    dist = {10: 100.0}

    iter = object_storage_api_tests.SizeDistributionIterator(dist)

    values = list(itertools.islice(iter, 5))

    self.assertEqual(values, [10, 10, 10, 10, 10])

  def testTwoElementDistribution(self):
    dist = {1: 50.0, 10: 50.0}
    iter = object_storage_api_tests.SizeDistributionIterator(dist)

    with mock.patch(random.__name__ + '.random') as rand:
      rand.side_effect = [0.2, 0.7, 0.2]
      values = list(itertools.islice(iter, 3))
      self.assertTrue(values == [1, 10, 1])

  def testNonTerminatingBinaryPercent(self):
    # 20/100 = 1/5 does not terminate in binary
    dist = {1: 20.0, 10: 80.0}
    iter = object_storage_api_tests.SizeDistributionIterator(dist)

    with mock.patch(random.__name__ + '.random') as rand:
      rand.side_effect = [0.1, 0.9]
      values = list(itertools.islice(iter, 2))

      self.assertTrue(values == [1, 10])


class TestMaxSizeInDistribution(unittest.TestCase):
  def testPointDistribution(self):
    dist = {10: 100.0}
    dist[10] = 100.0

    self.assertEqual(object_storage_api_tests.MaxSizeInDistribution(dist),
                     10)

  def testTwoElementDistribution(self):
    dist = {1: 50.0, 10: 50.0}
    self.assertEqual(object_storage_api_tests.MaxSizeInDistribution(dist),
                     10)


class TestPrefixCounterIterator(unittest.TestCase):
  def testIterator(self):
    iterator = object_storage_api_tests.PrefixCounterIterator('foo')
    values = list(itertools.islice(iterator, 3))
    self.assertEqual(values, ['foo_0', 'foo_1', 'foo_2'])


class TestPrefixTimestampSuffixIterator(unittest.TestCase):
  def testIterator(self):
    iterator = object_storage_api_tests.PrefixTimestampSuffixIterator(
        'foo', 'bar')
    with mock.patch(time.__name__ + '.time',
                    side_effect=[0, 1, 2]):
      values = list(itertools.islice(iterator, 3))
    self.assertEqual(values, ['foo_0.000000_bar',
                              'foo_1.000000_bar',
                              'foo_2.000000_bar'])


if __name__ == '__main__':
  unittest.main()
