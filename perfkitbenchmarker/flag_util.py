# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

import yaml

from perfkitbenchmarker import flags
from perfkitbenchmarker import units


FLAGS = flags.FLAGS

INTEGER_GROUP_REGEXP = re.compile(r'(\d+)(-(\d+))?(-(\d+))?$')


class IntegerList(object):
  """An immutable list of nonnegative integers.

  The list contains either single integers (ex: 5) or ranges (ex:
  8-12). Additionally, the user can provide a step to the range like so:
  8-24-2. The list can include as many elements as will fit in
  memory. Furthermore, the memory required to hold a range will not
  grow with the size of the range.

  Make a list with
    lst = IntegerList(groups)

  where groups is a list whose elements are either single integers,
  2-tuples holding the low and high bounds of a range
  (inclusive), or 3-tuples holding the low and high bounds, followed
  by the step size. (Ex: [5, (8,12)] represents the integer list
  5,8,9,10,11,12, and [(8-14-2)] represents the list 8,10,12,14.)

  """

  def __init__(self, groups):
    self.groups = groups

    length = 0
    for elt in groups:
      if isinstance(elt, int) or isinstance(elt, long):
        length += 1
      if isinstance(elt, tuple):
        length += len(self._CreateXrangeFromTuple(elt))

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
        group_len = len(self._CreateXrangeFromTuple(group))
        if idx >= group_len:
          group_idx += 1
          idx -= group_len
        else:
          step = 1 if len(group) == 2 else group[2]
          return group[0] + idx * step

    if isinstance(self.groups[group_idx], tuple):
      return self.groups[group_idx][0]
    else:
      return self.groups[group_idx]

  def __eq__(self, other):
    return tuple(self) == tuple(other)

  def __ne__(self, other):
    return tuple(self) != tuple(other)

  def __iter__(self):
    for group in self.groups:
      if isinstance(group, int) or isinstance(group, long):
        yield group
      else:
        for val in self._CreateXrangeFromTuple(group):
          yield val

  def __str__(self):
      return IntegerListSerializer().serialize(self)

  def _CreateXrangeFromTuple(self, input_tuple):
    start = input_tuple[0]
    stop_inclusive = input_tuple[1] + 1
    step = 1 if len(input_tuple) == 2 else input_tuple[2]
    return xrange(start, stop_inclusive, step)


class IntegerListParser(flags.ArgumentParser):
  """Parse a string containing a comma-separated list of nonnegative integers.

  The list may contain single integers and dash-separated ranges. For
  example, "1,3,5-7" parses to [1,3,5,6,7] and "1-7-3" parses to
  [1,4,7].

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

  def parse(self, inp):
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
    elif isinstance(inp, int):
      return IntegerList([inp])

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
        step = int(match.group(5)) if match.group(5) is not None else 1

        if high <= low or (len(result) > 0 and low <= result[-1]):
          HandleNonIncreasing()

        result.append((low, high, step))

    return IntegerList(result)

  def flag_type(self):
    return 'integer list'


class IntegerListSerializer(flags.ArgumentSerializer):

  def _SerializeRange(self, val):
    if len(val) == 2:
      return '%s-%s' % (val[0], val[1])
    return '%s-%s-%s' % (val[0], val[1], val[2])

  def serialize(self, il):
    return ','.join([str(val) if isinstance(val, int) or isinstance(val, long)
                     else self._SerializeRange(val)
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
    self._flag_dict_func_name = (
        '_flags' if hasattr(self._flags, '_flags') else 'FlagDict')

  def __enter__(self):
    """Begins the flag substitution."""
    self._original_flagdict = getattr(self._flags, self._flag_dict_func_name)
    self._flags.__dict__[self._flag_dict_func_name] = self._substitute

  def __exit__(self, *unused_args, **unused_kwargs):
    """Stops the flag substitution."""
    self._flags.__dict__[self._flag_dict_func_name] = self._original_flagdict


class UnitsParser(flags.ArgumentParser):
  """Parse a flag containing a unit expression.

  Attributes:
    convertible_to: list of units.Unit instances. A parsed expression must be
        convertible to at least one of the Units in this list. For example,
        if the parser requires that its inputs are convertible to bits, then
        values expressed in KiB and GB are valid, but values expressed in meters
        are not.
  """

  syntactic_help = ('A quantity with a unit. Ex: 12.3MB.')

  def __init__(self, convertible_to):
    """Initialize the UnitsParser.

    Args:
      convertible_to: Either an individual unit specification or a series of
          unit specifications, where each unit specification is either a string
          (e.g. 'byte') or a units.Unit. The parser input must be convertible to
          at least one of the specified Units, or the parse() method will raise
          a ValueError.
    """
    if isinstance(convertible_to, (basestring, units.Unit)):
      self.convertible_to = [units.Unit(convertible_to)]
    else:
      self.convertible_to = [units.Unit(u) for u in convertible_to]

  def parse(self, inp):
    """Parse the input.

    Args:
      inp: a string or a units.Quantity. If a string, it has the format
          "<number><units>", as in "12KB", or "2.5GB".

    Returns:
      A units.Quantity.

    Raises:
      ValueError: If the input cannot be parsed, or if it parses to a value with
          improper units.
    """
    if isinstance(inp, units.Quantity):
      quantity = inp
    else:
      try:
        quantity = units.ParseExpression(inp)
      except Exception as e:
        raise ValueError("Couldn't parse unit expression %r: %s" %
                         (inp, e.message))
      if not isinstance(quantity, units.Quantity):
        raise ValueError('Expression %r evaluates to a unitless value.' % inp)

    for unit in self.convertible_to:
      try:
        quantity.to(unit)
        break
      except units.DimensionalityError:
        pass
    else:
      raise ValueError(
          'Expression {0!r} is not convertible to an acceptable unit '
          '({1}).'.format(inp, ', '.join(str(u) for u in self.convertible_to)))

    return quantity


class UnitsSerializer(flags.ArgumentSerializer):
  def serialize(self, units):
    return str(units)


def DEFINE_units(name, default, help, convertible_to,
                 flag_values=flags.FLAGS, **kwargs):
  """Register a flag whose value is a units expression.

  Args:
    name: string. The name of the flag.
    default: units.Quantity. The default value.
    help: string. A help message for the user.
    convertible_to: Either an individual unit specification or a series of unit
        specifications, where each unit specification is either a string (e.g.
        'byte') or a units.Unit. The flag value must be convertible to at least
        one of the specified Units to be considered valid.
    flag_values: the absl.flags.FlagValues object to define the flag in.
  """
  parser = UnitsParser(convertible_to=convertible_to)
  serializer = UnitsSerializer()
  flags.DEFINE(parser, name, default, help, flag_values, serializer, **kwargs)


def StringToBytes(string):
  """Convert an object size, represented as a string, to bytes.

  Args:
    string: the object size, as a string with a quantity and a unit.

  Returns:
    an integer. The number of bytes in the size.

  Raises:
    ValueError, if either the string does not represent an object size
    or if the size does not contain an integer number of bytes.
  """

  try:
    quantity = units.ParseExpression(string)
  except Exception:
    # Catching all exceptions is ugly, but we don't know what sort of
    # exception pint might throw, and we want to turn any of them into
    # ValueError.
    raise ValueError("Couldn't parse size %s" % string)

  try:
    bytes = quantity.m_as(units.byte)
  except units.DimensionalityError:
    raise ValueError("Quantity %s is not a size" % string)

  if bytes != int(bytes):
    raise ValueError("Size %s has a non-integer number (%s) of bytes!" %
                     (string, bytes))

  if bytes < 0:
    raise ValueError("Size %s has a negative number of bytes!" % string)

  return int(bytes)


def StringToRawPercent(string):
  """Convert a string to a raw percentage value.

  Args:
    string: the percentage, with '%' on the end.

  Returns:
    A floating-point number, holding the percentage value.

  Raises:
    ValueError, if the string can't be read as a percentage.
  """

  if len(string) <= 1:
    raise ValueError("String '%s' too short to be percentage." % string)

  if string[-1] != '%':
    raise ValueError("Percentage '%s' must end with '%%'" % string)

  # This will raise a ValueError if it can't convert the string to a float.
  val = float(string[:-1])

  if val < 0.0 or val > 100.0:
    raise ValueError('Quantity %s is not a valid percentage' % val)

  return val


# The YAML flag type is necessary because flags can be read either via
# the command line or from a config file. If they come from a config
# file, they will already be parsed as YAML, but if they come from the
# command line, they will be raw strings. The point of this flag is to
# guarantee a consistent representation to the rest of the program.
class YAMLParser(flags.ArgumentParser):
  """Parse a flag containing YAML."""

  syntactic_help = 'A YAML expression.'

  def parse(self, inp):
    """Parse the input.

    Args:
      inp. A string or the result of yaml.load. If a string, should be
      a valid YAML document.
    """

    if isinstance(inp, basestring):
      # This will work unless the user writes a config with a quoted
      # string that, if unquoted, would be parsed as a non-string
      # Python type (example: '123'). In that case, the first
      # yaml.load() in the config system will strip away the quotation
      # marks, and this second yaml.load() will parse it as the
      # non-string type. However, I think this is the best we can do
      # without significant changes to the config system, and the
      # problem is unlikely to occur in PKB.
      try:
        return yaml.load(inp)
      except yaml.YAMLError as e:
        raise ValueError("Couldn't parse YAML string '%s': %s" %
                         (inp, e.message))
    else:
      return inp


class YAMLSerializer(flags.ArgumentSerializer):

  def serialize(self, val):
    return yaml.dump(val)


def DEFINE_yaml(name, default, help, flag_values=flags.FLAGS, **kwargs):
  """Register a flag whose value is a YAML expression.

  Args:
    name: string. The name of the flag.
    default: object. The default value of the flag.
    help: string. A help message for the user.
    flag_values: the absl.flags.FlagValues object to define the flag in.
    kwargs: extra arguments to pass to absl.flags.DEFINE().
  """

  parser = YAMLParser()
  serializer = YAMLSerializer()

  flags.DEFINE(parser, name, default, help, flag_values, serializer, **kwargs)


def ParseKeyValuePairs(strings):
  """Parses colon separated key value pairs from a list of strings.

  Pairs should be separated by a comma and key and value by a colon, e.g.,
  ['k1:v1', 'k2:v2,k3:v3'].

  Args:
    strings: A list of strings.

  Returns:
    A dict populated with keys and values from the flag.
  """
  pairs = {}
  for pair in [kv for s in strings for kv in s.split(',')]:
    try:
      key, value = pair.split(':', 1)
      pairs[key] = value
    except ValueError:
      logging.error('Bad key value pair format. Skipping "%s".', pair)
      continue

  return pairs


def GetProvidedCommandLineFlags():
  """Return flag names and values that were specified on the command line.

  Returns:
    A dictionary of provided flags in the form: {flag_name: flag_value}.
  """
  return {k: FLAGS[k].value for k in FLAGS if FLAGS[k].present}
