# Copyright 2015 Google Inc. All rights reserved.
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

import functools
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


def DEFINE_integerlist(name, default, help,
                       on_nonincreasing=None, flag_values=flags.FLAGS, **args):
  """Register a flag whose value must be an integer list."""

  parser = IntegerListParser(on_nonincreasing=on_nonincreasing)
  serializer = IntegerListSerializer()

  flags.DEFINE(parser, name, default, help, flag_values, serializer, **args)


SI_SUFFIXES = ['', 'K', 'M', 'G', 'T', 'P']


@functools.total_ordering
class ObjectSize(object):
  """Holds the size of data, i.e. numbers of bytes.

  Knows how to format itself to be input to different tools.
  """

  def __init__(self, bytes=0):
    self.bytes = bytes

  def BytesInScientificNotation(self, base, max_exp):
    """Write bytes in scientific notation.

    Find mult and exp so that self.bytes = mult * base ^ exp,
    where exp is as large as possible but <= max_exp.

    Returns:
      A tuple (mult, exp).
    """

    num = self.bytes
    exp = 0

    if (num == 0):
      return (0, 0)

    while num % base == 0 and exp < max_exp:
      num = num // base
      exp += 1

    return (num, exp)

  def fioFormat(self):
    max_exp = len(SI_SUFFIXES) - 1
    mult_ten, exp_ten = self.BytesInScientificNotation(1000, max_exp)
    mult_two, exp_two = self.BytesInScientificNotation(1024, max_exp)

    if exp_ten > exp_two:
      mult = mult_ten
      exp = exp_ten
      base = 1000
    else:
      mult = mult_two
      exp = exp_two
      base = 1024

    # fio reverses the usual meaning of the SI suffixes, so 'k' means
    # 1024 and 'ki' means 1000.
    return '%s%s%s' % (str(mult), SI_SUFFIXES[exp].lower(),
                       'i' if base == 1000 else '')

  def __str__(self):
    max_exp = len(SI_SUFFIXES) - 1
    mult_ten, exp_ten = self.BytesInScientificNotation(1000, max_exp)
    mult_two, exp_two = self.BytesInScientificNotation(1024, max_exp)

    if exp_ten >= exp_two:
      mult = mult_ten
      exp = exp_ten
      base = 1000
    else:
      mult = mult_two
      exp = exp_two
      base = 1024

    return '%s%s%sB' % (str(mult), SI_SUFFIXES[exp],
                        'i' if base == 1024 else '')


  def __eq__(self, other):
    return isinstance(other, ObjectSize) and self.bytes == other.bytes

  def __lt__(self, other):
    if isinstance(other, ObjectSize):
      return self.bytes < other.bytes
    else:
      raise NotImplemented()


INTEGER_REGEXP = re.compile('\d+')


class ObjectSizeParser(flags.ArgumentParser):
  """Parse an object size.

  The user writes a decimal followed by a suffix. Allowable base
  suffixes are k,m,g,t, and p, meaning kilo, mega, giga, tera, and
  peta. Each suffix may have an 'i' immediately after it, which means
  to use the base-2 interpretation instead of base-10. All
  measurements are bytes, not bits.
  """

  def Parse(self, inp):
    integer_match = INTEGER_REGEXP.match(inp)
    if integer_match is None:
      raise ValueError('Object size must start with positive integer')

    num = int(integer_match.group())
    suffix = inp[integer_match.end():]

    if len(suffix) == 0:
      return ObjectSize(bytes=num)

    base_suffix = suffix[0]
    suffix_modifier = suffix[1] if len(suffix) > 1 else None

    if len(suffix) >= 3:
      raise ValueError('Suffix %s not valid in object size', suffix)

    suffix_idx = None
    for i in range(len(SI_SUFFIXES)):
      if base_suffix.lower() == SI_SUFFIXES[i].lower():
        suffix_idx = i
        break

    if suffix_idx is None:
      raise ValueError('Suffix %s not valid in object size', suffix)

    if suffix_modifier:
      if suffix_modifier is not 'i':
        raise ValueError('Suffix %s not valid in object size', suffix)
      else:
        multiplier = 1024 ** suffix_idx
    else:
      multiplier = 1000 ** suffix_idx

    return ObjectSize(bytes=num * multiplier)


  def Type(self):
    return 'object size'


class ObjectSizeSerializer(flags.ArgumentSerializer):
  def Serialize(self, os):
    return str(os)
