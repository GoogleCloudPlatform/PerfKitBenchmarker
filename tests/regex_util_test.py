# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.lib.regex_util."""

import unittest

from perfkitbenchmarker import regex_util


class ExtractGroupTestCase(unittest.TestCase):

  def testMatches(self):
    regex = r'test ([\da-f]+) text'
    string = 'test 12a3de text'
    self.assertEqual('12a3de', regex_util.ExtractGroup(regex, string, group=1))

  def testNoMatch(self):
    regex = r'test ([\da-f]+) text'
    string = 'test text'
    self.assertRaises(regex_util.NoMatchError, regex_util.ExtractGroup, regex,
                      string, group=1)

  def testMatches_Unanchored(self):
    regex = r'([\da-f]+) text'
    string = 'test 12a3de text'
    self.assertEqual('12a3de', regex_util.ExtractGroup(regex, string, group=1))

  def testNamedGroup(self):
    regex = r'test (?P<hex>[\da-f]+) text'
    string = 'test 12a3de text'
    self.assertEqual('12a3de', regex_util.ExtractGroup(regex, string,
                                                       group='hex'))

  def testNumberedGroup_Invalid(self):
    regex = r'test ([\da-f]+) (.*)'
    string = 'test 12a3de text'
    self.assertRaisesRegexp(IndexError, 'No such group 3 in',
                            regex_util.ExtractGroup, regex, string, group=3)

  def testNumberedGroup_Valid(self):
    regex = r'test ([\da-f]+) (.*)'
    string = 'test 12a3de text'
    self.assertEqual('text', regex_util.ExtractGroup(regex, string, group=2))

  def testNumberedGroup_WholeMatch(self):
    regex = r'test [\da-f]+ (.*)'
    string = 'test 12a3de text'
    self.assertEqual(string, regex_util.ExtractGroup(regex, string, group=0))


class ExtractFloatTestCase(unittest.TestCase):

  def testParsesSuccessfully(self):
    regex = r'test (\d+|\.\d+|\d+\.\d+) string'
    string = 'test 12.435 string'
    self.assertAlmostEqual(12.435, regex_util.ExtractFloat(regex, string,
                                                           group=1))

  def testRaisesValueErrorOnInvalidInput(self):
    regex = r'test (invalid_float) string'
    string = 'test invalid_float string'
    self.assertRaises(ValueError, regex_util.ExtractFloat, regex, string,
                      group=1)


class ExtractAllFloatMetricsTestCase(unittest.TestCase):

  def testParseSuccessful(self):
    matches = regex_util.ExtractAllFloatMetrics(
        """
        metric=value
        a=1
        b=2.0
        c=.3
        d=-4.5
        ef=3.2e+2
        """)
    self.assertEqual(len(matches), 5)
    self.assertEqual(1.0, matches['a'])
    self.assertEqual(2.0, matches['b'])
    self.assertEqual(0.3, matches['c'])
    self.assertEqual(-4.5, matches['d'])
    self.assertEqual(3.2e+2, matches['ef'])

  def testInvalidMetricRegex(self):
    self.assertRaises(
        NotImplementedError,
        regex_util.ExtractAllFloatMetrics,
        'metric=1.0',
        metric_regex=r'\w(\w)')

  def testIntegerValueRegex(self):
    matches = regex_util.ExtractAllFloatMetrics(
        'a=1.2,b=3', value_regex=r'\d+')
    self.assertEqual(len(matches), 2)
    self.assertEqual(1.0, matches['a'])
    self.assertEqual(3, matches['b'])


class ExtractAllMatchesTestCase(unittest.TestCase):

  def testParseSuccessfully(self):
    regex = r'(\d+) (\w+)'
    string = 'test 10 sec 33 Mbps multiple matching'
    matches = regex_util.ExtractAllMatches(regex, string)
    self.assertEqual(len(matches), 2)
    self.assertEqual(matches[0][0], '10')
    self.assertEqual(matches[0][1], 'sec')
    self.assertEqual(matches[1][0], '33')
    self.assertEqual(matches[1][1], 'Mbps')

  def testNoMatch(self):
    regex = r'test (\d\w\d) no match'
    string = 'test no match'
    self.assertRaises(regex_util.NoMatchError, regex_util.ExtractAllMatches,
                      regex, string)


class ExtractExactlyOneMatchTestCase(unittest.TestCase):
  def testNoMatch(self):
    with self.assertRaises(regex_util.NoMatchError):
      regex_util.ExtractExactlyOneMatch('foo', 'bar')

  def testNonUniqueMatch(self):
    with self.assertRaises(regex_util.TooManyMatchesError):
      regex_util.ExtractExactlyOneMatch('spam', 'spam spam spam')

  def testNoCapturingGroup(self):
    self.assertEqual(regex_util.ExtractExactlyOneMatch('bar+', 'foo barrr baz'),
                     'barrr')

  def testCapturingGroup(self):
    self.assertEqual(
        regex_util.ExtractExactlyOneMatch('ba(r+)', 'foo barrr baz'),
        'rrr')


class SubstituteTestCase(unittest.TestCase):

  def testSubstituteSuccess(self):
    pattern = r'<(\w+)>'
    repl = r'[\1]'
    text = 'foo <bar> <foo> bar'
    sub_text = regex_util.Substitute(pattern, repl, text)
    self.assertEqual(sub_text, 'foo [bar] [foo] bar')

  def testNoMatch(self):
    pattern = r'\[(\w+)\]'
    repl = r'\1'
    text = 'foo <bar> <foo> bar'
    self.assertRaises(regex_util.NoMatchError, regex_util.Substitute,
                      pattern, repl, text)


if __name__ == '__main__':
  unittest.main()
