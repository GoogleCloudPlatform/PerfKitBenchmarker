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
"""Tests for perfkitbenchmarker.stages."""

import unittest

from perfkitbenchmarker import stages


class RunStageParserTestCase(unittest.TestCase):

  def setUp(self):
    self._parser = stages.RunStageParser()

  def testEmpty(self):
    with self.assertRaises(ValueError):
      self._parser.parse('')

  def testInvalidItem(self):
    with self.assertRaises(ValueError):
      self._parser.parse('provision,fake_stage')

  def testAllAndIndividualStages(self):
    with self.assertRaises(ValueError):
      self._parser.parse('provision,all')

  def testIncorrectOrder(self):
    with self.assertRaises(ValueError):
      self._parser.parse('provision,run')
    with self.assertRaises(ValueError):
      self._parser.parse('teardown,provision')

  def testAll(self):
    self.assertEqual(
        self._parser.parse('all'),
        ['provision', 'prepare', 'run', 'cleanup', 'teardown'],
    )

  def testIndividual(self):
    self.assertEqual(self._parser.parse('prepare'), ['prepare'])

  def testMultiple(self):
    self.assertEqual(self._parser.parse('prepare,run'), ['prepare', 'run'])

  def testList(self):
    self.assertEqual(self._parser.parse(['prepare', 'run']), ['prepare', 'run'])

  def testThreePartPrepare(self):
    self.assertEqual(
        self._parser.parse('install_packages,prepare_system,start_services'),
        ['install_packages', 'prepare_system', 'start_services'],
    )

  def testInstallPackagesWithoutStartServices(self):
    self.assertEqual(
        self._parser.parse('install_packages,run'),
        ['install_packages', 'run'],
    )

  def testProvisionAndInstallPackages(self):
    self.assertEqual(
        self._parser.parse('provision,install_packages'),
        ['provision', 'install_packages'],
    )

  def testPrepareIsNotAllowedWithThreePartPrepare(self):
    with self.assertRaises(ValueError):
      self._parser.parse('prepare,prepare_system')
    with self.assertRaises(ValueError):
      self._parser.parse('prepare,install_packges')
    with self.assertRaises(ValueError):
      self._parser.parse('prepare,start_services')


if __name__ == '__main__':
  unittest.main()
