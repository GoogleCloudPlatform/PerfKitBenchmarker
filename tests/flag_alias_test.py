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
from tests import pkb_common_test_case


class TestFlagAlias(parameterized.TestCase):

  @parameterized.named_parameters(
      ('BaseCase1', ['--a=cat'], ['--ab=cat']),
      ('BaseCase2', ['-a=cat'], ['-ab=cat']),
      ('BaseCase3', ['--a', 'cat'], ['--ab', 'cat']),
      ('BaseCase4', ['-a', 'cat'], ['-ab', 'cat']),
      ('TestValueUnchanged1', ['-ab=a'], ['-ab=a']),
      ('TestValueUnchanged2', ['-a=-a'], ['-ab=-a']),
      ('TestNoPrefix1', ['-noa'], ['-noab']),
      ('TestNoPrefix2', ['-noab'], ['-noab']),
  )
  def testAliasFlagsFromArgs(self, argv, expected_argv):
    flags = [{'a': 'ab'}]
    self.assertListEqual(
        flag_alias.AliasFlagsFromArgs(argv, flags), expected_argv
    )

  @parameterized.named_parameters(
      ('BaseCase1', {'a': 'cat'}, {'ab': 'cat'}),
      ('BaseCase2', {}, {}),
      ('BaseCase3', {'a': 'ab', 'b': 'ab'}, {'ab': 'ab', 'd': 'ab'}),
      ('Unchanged', {'ab': 'a'}, {'ab': 'a'}),
  )
  def testAliasFlagsFromYaml(self, dic, expected_dic):
    flags = [{'a': 'ab'}, {'b': 'd'}]
    self.assertDictEqual(
        flag_alias.AliasFlagsFromYaml(dic, flags), expected_dic
    )


class TestZonesFlagAlias(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.parameters(
      (['-zones=test1'], ['--zone=test1']),
      (['--zones=test1,test2'], ['--zone=test1', '--zone=test2']),
      (
          ['--zone=test0', '--zones=test1,test2'],
          ['--zone=test0', '--zone=test1', '--zone=test2'],
      ),
      (['--extra_zones=test1,test2'], ['--zone=test1', '--zone=test2']),
      (
          ['--zones=test1,test2', '--extra_zones=test3,test4'],
          ['--zone=test1', '--zone=test2', '--zone=test3', '--zone=test4'],
      ),
  )
  def testZoneFlagsFromArgs(self, argv, expected_argv):
    self.assertEqual(flag_alias.AliasFlagsFromArgs(argv), expected_argv)

  @parameterized.parameters(
      (
          {'zones': 'test1', 'extra_zones': 'test2'},
          {'zone': ['test1', 'test2']},
      ),
      ({'zones': ['test1', 'test2']}, {'zone': ['test1', 'test2']}),
      ({'extra_zones': ['test1', 'test2']}, {'zone': ['test1', 'test2']}),
      (
          {'zones': ['test1', 'test2'], 'extra_zones': ['test3', 'test4']},
          {'zone': ['test1', 'test2', 'test3', 'test4']},
      ),
  )
  def testZoneFlagsFromYaml(self, dic, expected_dic):
    self.assertEqual(flag_alias.AliasFlagsFromYaml(dic), expected_dic)


if __name__ == '__main__':
  unittest.main()
