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

"""Tests for perfkitbenchmarker.units."""

import pickle
import unittest

from perfkitbenchmarker import units


class UnitRegistryTestCase(unittest.TestCase):

  def testUnitNotEqual(self):
    # See https://github.com/hgrecco/pint/issues/372
    self.assertFalse(units.byte != units.Unit('byte'))

  def testKB(self):
    self.assertEqual(units.ParseExpression('12KB'),
                     units.ParseExpression('12000 bytes'))

  def testIntPercent(self):
    q = units.ParseExpression('10%')
    self.assertEqual(q.magnitude, 10)
    self.assertEqual(q.units, units.percent)

  def testFloatPercent(self):
    q = units.ParseExpression('12.5%')
    self.assertEqual(q.magnitude, 12.5)
    self.assertEqual(q.units, units.percent)


class TestPintPickling(unittest.TestCase):

  def testSameUnitRegistry(self):
    q_prepickle = 1.0 * units.Unit('second')
    q_pickled = pickle.dumps(q_prepickle)
    q_postpickle = pickle.loads(q_pickled)

    self.assertEqual(q_prepickle, q_postpickle)

  def testNewUnitRegistry(self):
    # The fundamental issue with pickling Pint Quantities is that you
    # need all of your Quantities to point to the same UnitRegistry
    # object, and when we close and reopen PKB, we create a new
    # UnitRegistry. So to test it, we create a new UnitRegistry.
    q_prepickle = 1.0 * units.Unit('second')
    q_pickled = pickle.dumps(q_prepickle)

    units._UNIT_REGISTRY = units._UnitRegistry()

    q_postpickle = pickle.loads(q_pickled)

    new_second = 1.0 * units.Unit('second')
    self.assertEqual(q_postpickle, new_second)
    # This next line checks that q_postpickle is in the same "Pint
    # universe" as new_second, because we can convert q_postpickle to
    # the units of new_second.
    q_postpickle.to(new_second)

  def testPickleKB(self):
    # Make sure we can pickle and unpickle quantities with the unit we
    # defined ourselves.
    q_prepickle = units.ParseExpression('1KB')
    q_pickled = pickle.dumps(q_prepickle)
    q_postpickle = pickle.loads(q_pickled)

    self.assertEqual(q_prepickle, q_postpickle)

  def testPicklePercent(self):
    q = units.ParseExpression('10%')
    self.assertEqual(q, pickle.loads(pickle.dumps(q)))


if __name__ == '__main__':
  unittest.main()
