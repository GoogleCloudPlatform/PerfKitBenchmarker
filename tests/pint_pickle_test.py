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

"""Test that we can pickle and unpickle Pint objects."""

import pickle
import unittest

import pint

import perfkitbenchmarker


class TestPintPickling(unittest.TestCase):

  def testSameUnitRegistry(self):
    q_prepickle = perfkitbenchmarker.UNIT_REGISTRY.second
    q_pickled = pickle.dumps(q_prepickle)
    q_postpickle = pickle.loads(q_pickled)

    self.assertEqual(q_prepickle, q_postpickle)

  def testNewUnitRegistry(self):
    # The fundamental issue with pickling Pint Quantities is that you
    # need all of your Quantities to point to the same UnitRegistry
    # object, and when we close and reopen PKB, we create a new
    # UnitRegistry. So to test it, we create a new UnitRegistry.
    q_prepickle = perfkitbenchmarker.UNIT_REGISTRY.second
    q_pickled = pickle.dumps(q_prepickle)

    perfkitbenchmarker.UNIT_REGISTRY = pint.UnitRegistry()

    q_postpickle = pickle.loads(q_pickled)

    with self.assertRaises(TypeError):
      # We're checking that Pint can't convert the q_prepickle to the
      # same units as q_postpickle, even though the units are both
      # seconds, because they come from different UnitRegistries. For
      # some reason the self.assertNotEqual check doesn't catch this.
      q_prepickle.to(q_postpickle)

    new_second = perfkitbenchmarker.UNIT_REGISTRY.second
    self.assertEqual(q_postpickle, new_second)
    # This next line checks that q_postpickle is in the same "Pint
    # universe" as new_second, because we can convert q_postpickle to
    # the units of new_second.
    q_postpickle.to(new_second)


if __name__ == '__main__':
  unittest.main()
