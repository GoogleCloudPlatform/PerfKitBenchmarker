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

import pint

import perfkitbenchmarker
from perfkitbenchmarker import flags


FLAGS = flags.FLAGS

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

  def __str__(self):
      return IntegerListSerializer().Serialize(self)


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
                       flag_values=FLAGS, **kwargs):
  """Register a flag whose value must be an integer list."""

  parser = IntegerListParser(on_nonincreasing=on_nonincreasing)
  serializer = IntegerListSerializer()

  flags.DEFINE(parser, name, default, help, flag_values, serializer, **kwargs)


class FlagDictSubstitution(object):
  """Context manager that redirects flag reads and writes."""

  def __init__(self, flag_values, substitute):
    """Initializes a FlagDictSubstitution.

    Args:
      flag_values: FlagValues that is temporarily modified such that all its
          flag reads and writes are redirected.
      substitute: Callable that temporarily replaces the FlagDict function of
          flag_values. Accepts no arguments and returns a dict mapping flag
          name string to Flag object.
    """
    self._flags = flag_values
    self._substitute = substitute

  def __enter__(self):
    """Begins the flag substitution."""
    self._original_flagdict = self._flags.FlagDict
    self._flags.__dict__['FlagDict'] = self._substitute

  def __exit__(self, *unused_args, **unused_kwargs):
    """Stops the flag substitution."""
    self._flags.__dict__['FlagDict'] = self._original_flagdict


class UnitsParser(flags.ArgumentParser):
  """Parse a flag containing a unit expression.

  The user may require that the provided expression is convertible to
  a particular unit. The parser will throw an error if the condition
  is not satisfied. For instance, if a unit parser requires that its
  arguments are convertible to bits, then KiB and GB are valid units
  to input, but meters are not. If the user does not require this,
  than *any* unit expression is allowed.
  """

  syntactic_help = ('A quantity with a unit. Ex: 12.3MB.')

  def __init__(self, convertible_to=None):
    """Initialize the UnitsParser.

    Args:
      convertible_to: perfkitbenchmarker.UNIT_REGISTRY.Unit or
        None. If a unit, the input must be convertible to this unit or
        the Parse() method will raise a ValueError.
    """

    self.convertible_to = convertible_to

  def Parse(self, inp):
    """Parse the input.

    Args:
      inp: a string or a perfkitbenchmarker.UNIT_REGISTRY.Quantity. If a string,
        string has the format "<number><units>", as in "12KB", or "2.5GB".

    Returns:
      A perfkitbenchmarker.UNIT_REGISTRY.Quantity.

    Raises:
      ValueError if it can't parse its input.
    """

    if isinstance(inp, perfkitbenchmarker.UNIT_REGISTRY.Quantity):
      quantity = inp
    else:
      try:
        quantity = perfkitbenchmarker.UNIT_REGISTRY.parse_expression(inp)
      except Exception as e:
        raise ValueError("Couldn't parse unit expresion %s: %s" %
                         (inp, e.message))

    if self.convertible_to is not None:
      try:
        quantity.to(self.convertible_to)
      except pint.DimensionalityError:
        raise ValueError("Expression %s is not convertible to %s" %
                         (inp, self.convertible_to))

    return quantity


class UnitsSerializer(flags.ArgumentSerializer):
  def Serialize(self, units):
    return str(units)


def DEFINE_units(name, default, help, convertible_to=None,
                 flag_values=flags.FLAGS, **kwargs):
  """Register a flag whose value is a units expression.

  Args:
    name: string. The name of the flag.
    default: perfkitbenchmarker.UNIT_REGISTRY.Quantity. The default value.
    help: string. A help message for the user.
    convertible_to: perfkitbenchmarker.UNIT_REGISTRY.Unit or None. If
      a unit is provided, the input must be convertible to this unit
      to be considered valid.
    flag_values: the gflags.FlagValues object to define the flag in.
  """

  parser = UnitsParser(convertible_to=convertible_to)
  serializer = UnitsSerializer()

  flags.DEFINE(parser, name, default, help, flag_values, serializer, **kwargs)
