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

"""Utilities for extracting benchmark results using regular expression."""

import re

_IPV4_REGEX = r'[0-9]+(?:\.[0-9]+){3}'

# From https://docs.python.org/2/library/re.html#simulating-scanf.
FLOAT_REGEX = r'[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?'


class NoMatchError(ValueError):
  """Raised when no matches for a regex are found within a string."""
  pass


class TooManyMatchesError(ValueError):
  """Raised when a regex matches a string more times than expected."""
  pass


def ExtractGroup(regex, text, group=1, flags=0):
  """Extracts a float from a regular expression matched to 'text'.

  Args:
    regex: string or regexp pattern. Regular expression.
    text: string. Text to search.
    group: int. Group containing a floating point value. Use '0' for the whole
      string.
    flags: int. Flags to pass to re.search().
  Returns:
    A floating point number matched by 'regex' on 'text'.
  Raises:
    NoMatchError: when 'regex' does not match 'text'.
    IndexError: when 'group' is not present in the match.
  """
  match = re.search(regex, text, flags=flags)
  if not match:
    raise NoMatchError('No match for pattern "{0}" in "{1}"'.format(
        regex, text))

  try:
    return match.group(group)
  except IndexError:
    raise IndexError('No such group {0} in "{1}".'.format(group, regex))


def ExtractFloat(regex, text, group=1):
  """Extracts a float from a regular expression matched to 'text'."""
  return float(ExtractGroup(regex, text, group=group))


def ExtractAllFloatMetrics(text,
                           metric_regex=r'\w+',
                           value_regex=FLOAT_REGEX,
                           delimiter_regex='='):
  """Extracts metrics and their values into a dict.

  Args:
    text: The text to parse to find metric and values.
    metric_regex: A regular expression to find metric names. The metric regex
        should not contain any parenthesized groups.
    value_regex: A regular expression to find float values. By default, this
        works well for floating-point numbers found via scanf.
    delimiter_regex: A regular expression between the metric name and value.

  Returns:
    A dict mapping metrics to values.
  """
  if '(' in metric_regex:
    raise NotImplementedError('ExtractAllFloatMetrics does not support a '
                              'metric regex with groups.')
  matches = re.findall('(%s)%s(%s)' % (metric_regex, delimiter_regex,
                                       value_regex), text)
  return {match[0]: float(match[1]) for match in matches}


def ExtractIpv4Addresses(text):
  """Extracts all ipv4 addresses within 'text'.

  Args:
    text: string. Text to search.
  Returns:
    A list of ipv4 strings.
  RaisesL
    NoMatchError: when no ipv4 address is found.
  """
  match = re.findall(_IPV4_REGEX, text)
  if not match:
    raise NoMatchError('No match for ipv4 addresses in "{0}"'.format(text))
  return match


def ExtractAllMatches(regex, text, flags=0):
  """Extracts all matches from a regular expression matched within 'text'.

  Extracts all matches from a regular expression matched within 'text'. Please
  note that this function will return a list of strings if regex does not
  contain any capturing groups, matching the behavior of re.findall:
  >>> re.findall(r'bar', 'foo foo bar foo bar foo')
  ['bar', 'bar']

  Args:
    regex: string. Regular expression.
    text: string. Text to search.
    flags: int. Flags to pass to re.findall().
  Returns:
    A list of tuples of strings that matched by 'regex' within 'text'.
  Raises:
    NoMatchError: when 'regex' does not match 'text'.
  """
  match = re.findall(regex, text, flags=flags)
  if not match:
    raise NoMatchError('No match for pattern "{0}" in "{1}"'.format(
        regex, text))
  return match


def ExtractExactlyOneMatch(regex, text):
  """Extracts exactly one match of a regular expression from 'text'.

  Args:
    regex: string. Regular expression, possibly with capturing group.
    text: string. The text to search.

  Returns:
    The contents of the capturing group in the regex. If no capturing
    group is present, the text that matched the expression.

  Raises:
    NoMatchError if 'regex' does not match 'text'.
    TooManyMatchesError if 'regex' matches 'text' more than once.
  """

  matches = ExtractAllMatches(regex, text)
  if len(matches) > 1:
    raise TooManyMatchesError(
        'Pattern "{0}" matched "{1}" non-uniquely.'.format(regex, text))
  return matches[0]


def Substitute(pattern, repl, text):
  """Substitute all 'pattern' in 'text' with 'repl'.

  Args:
    pattern: string. Pattern to be replaced.
    repl: string. Replacement pattern.
    text: string. Text to search.
  Returns:
    A string after replacing all patterns with repl.
  Raises:
    NoMatchError: when 'pattern' isn't found in string.
  """
  if not re.search(pattern, text):
    raise NoMatchError('No match for pattern "{0}" in "{1}"'.format(
        pattern, text))
  return re.sub(pattern, repl, text)
