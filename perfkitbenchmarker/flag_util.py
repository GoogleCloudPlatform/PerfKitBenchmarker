# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utility functions for working with user-supplied flags."""

import logging
import re

from perfkitbenchmarker import flags

INTEGER_GROUP_REGEXP = re.compile(r'(\d+)(-(\d+))?$')


class IntegerList(object):
  """An immutable list of nonnegative integers.

  The list contains either single integers (ex: 5) or ranges (ex:
  8-12). The list can include as many elements as will fit in
  memory. Furthermore, the memory required to hold a range will not
  grow with the size of the range.

  Make a list with
    lst = IntegerList(groups)

  where groups is a list whose elements are either single integers or
  2-tuples holding the low and high bounds of a range
  (inclusive). (Ex: [5, (8,12)] represents the integer list
  5,8,9,10,11,12.)

  """

  def __init__(self, groups):
    self.groups = groups

    length = 0
    for elt in groups:
      if isinstance(elt, int) or isinstance(elt, long):
        length += 1
      if isinstance(elt, tuple):
        length += elt[1] - elt[0] + 1

    self.length = length

  def __len__(self):
    return self.length

  def __getitem__(self, idx):
    if not isinstance(idx, int):
      raise TypeError()
    if idx < 0 or idx >= self.length:
      raise IndexError()

    group_idx = 0
    while idx > 0:
      group = self.groups[group_idx]

      if not isinstance(group, tuple):
        group_idx += 1
        idx -= 1
      else:
        group_len = group[1] - group[0] + 1
        if idx >= group_len:
          group_idx += 1
          idx -= group_len
        else:
          return group[0] + idx

    if isinstance(self.groups[group_idx], tuple):
      return self.groups[group_idx][0]
    else:
      return self.groups[group_idx]

  def __iter__(self):
    for group in self.groups:
      if isinstance(group, int) or isinstance(group, long):
        yield group
      else:
        low, high = group
        for val in xrange(low, high + 1):
          yield val


class IntegerListParser(flags.ArgumentParser):
  """Parse a string containing a comma-separated list of nonnegative integers.

  The list may contain single integers and dash-separated ranges. For
  example, "1,3,5-7" parses to [1,3,5,6,7].

  Can pass the flag on_nonincreasing to the constructor to tell it
  what to do if the list is nonincreasing. Options are
    - None: do nothing.
    - IntegerListParser.WARN: log a warning.
    - IntegerListParser.EXCEPTION: raise a ValueError.

  As a special case, instead of a string, can pass a list of integers
  or an IntegerList. In these cases, the return value iterates over
  the same integers as were in the argument.
  """

  syntactic_help = ('A comma-separated list of nonnegative integers or integer '
                    'ranges. Ex: 1,3,5-7 is read as 1,3,5,6,7.')

  WARN = 'warn'
  EXCEPTION = 'exception'

  def __init__(self, on_nonincreasing=None):
    super(IntegerListParser, self).__init__()

    self.on_nonincreasing = on_nonincreasing

  def Parse(self, inp):
    """Parse an integer list.

    Args:
      inp: a string, a list, or an IntegerList.

    Returns:
      An iterable of integers.

    Raises:
      ValueError if inp doesn't follow a format it recognizes.
    """

    if isinstance(inp, IntegerList):
      return inp
    elif isinstance(inp, list):
      return IntegerList(inp)

    def HandleNonIncreasing():
      if self.on_nonincreasing == IntegerListParser.WARN:
        logging.warning('Integer list %s is not increasing', inp)
      elif self.on_nonincreasing == IntegerListParser.EXCEPTION:
        raise ValueError('Integer list %s is not increasing', inp)

    groups = inp.split(',')
    result = []

    for group in groups:
      match = INTEGER_GROUP_REGEXP.match(group)
      if match is None:
        raise ValueError('Invalid integer list %s', inp)
      elif match.group(2) is None:
        val = int(match.group(1))

        if len(result) > 0 and val <= result[-1]:
          HandleNonIncreasing()

        result.append(val)
      else:
        low = int(match.group(1))
        high = int(match.group(3))

        if high <= low or (len(result) > 0 and low <= result[-1]):
          HandleNonIncreasing()

        result.append((low, high))

    return IntegerList(result)

  def Type(self):
    return 'integer list'


class IntegerListSerializer(flags.ArgumentSerializer):
  def Serialize(self, il):
    return ','.join([str(val) if isinstance(val, int) or isinstance(val, long)
                     else '%s-%s' % (val[0], val[1])
                     for val in il.groups])


def DEFINE_integerlist(name, default, help, on_nonincreasing=None,
                       flag_values=flags.GLOBAL_FLAGS, **args):
  """Register a flag whose value must be an integer list."""

  parser = IntegerListParser(on_nonincreasing=on_nonincreasing)
  serializer = IntegerListSerializer()

  flags.DEFINE(parser, name, default, help, flag_values, serializer, **args)
