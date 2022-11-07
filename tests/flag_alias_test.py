# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for flag_util.py."""

import unittest

from absl.testing import parameterized
from perfkitbenchmarker import flag_alias


class TestFlagAlias(parameterized.TestCase):

  @parameterized.named_parameters(
      ('BaseCase1', ['--a=cat'], ['--ab=cat']),
      ('BaseCase2', ['-a=cat'], ['-ab=cat']),
      ('BaseCase3', ['--a', 'cat'], ['--ab', 'cat']),
      ('BaseCase4', ['-a', 'cat'], ['-ab', 'cat']),
      ('TestValueUnchanged1', ['-ab=a'], ['-ab=a']),
      ('TestValueUnchanged2', ['-a=-a'], ['-ab=-a']),
      ('TestNoPrefix1', ['-noa'], ['-noab']),
      ('TestNoPrefix2', ['-noab'], ['-noab']))
  def testAliasFlagsFromArgs(self, argv, expected_argv):
    flags = [{'a': 'ab'}]
    self.assertListEqual(
        flag_alias.AliasFlagsFromArgs(argv, flags), expected_argv)

  @parameterized.named_parameters(('BaseCase1', {'a': 'cat'}, {'ab': 'cat'}),
                                  ('BaseCase2', {}, {}), ('BaseCase3', {
                                      'a': 'ab',
                                      'b': 'ab'
                                  }, {
                                      'ab': 'ab',
                                      'd': 'ab'
                                  }))
  def testAliasFlagsFromYaml(self, dic, expected_dic):
    flags = [{'a': 'ab'}, {'b': 'd'}]
    self.assertDictEqual(
        flag_alias.AliasFlagsFromYaml(dic, flags), expected_dic)


if __name__ == '__main__':
  unittest.main()
