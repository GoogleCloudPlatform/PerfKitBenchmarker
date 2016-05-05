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
"""Module that provides access to pint functionality.

Forwards access to pint Quantity and Unit classes built around a customized
unit registry.
"""

import copy
import copy_reg
import numbers

import pint


class _UnitRegistry(pint.UnitRegistry):
  """A customized pint.UnitRegistry used by PerfKit Benchmarker.

  Supports 'K' prefix for 'kilo' (in addition to pint's default 'k').
  Supports '%' as a unit, whereas pint tokenizes it as an operator.
  """

  def __init__(self):
    super(_UnitRegistry, self).__init__()
    self.define('K- = 1000')
    self.define('% = [percent] = percent')

  def parse_expression(self, input_string, *args, **kwargs):
    result = super(_UnitRegistry, self).parse_expression(input_string, *args,
                                                         **kwargs)
    if (isinstance(result, numbers.Number) and
        input_string.strip().endswith('%')):
      return self.Quantity(result, self.Unit('percent'))
    return result


# Pint recommends one global UnitRegistry for the entire program, so
# we create it here.
_UNIT_REGISTRY = _UnitRegistry()


# The Pint documentation suggests serializing Quantities as tuples. We
# supply serializers to make sure that Quantities are unpickled with
# our UnitRegistry, where we have added the K- unit.
def _PickleQuantity(q):
  return _UnPickleQuantity, (q.to_tuple(),)


def _UnPickleQuantity(inp):
  return _UNIT_REGISTRY.Quantity.from_tuple(inp)


copy_reg.pickle(_UNIT_REGISTRY.Quantity, _PickleQuantity)


# The following monkey-patch has been submitted to upstream Pint as
# pull request 357.
# TODO: once that PR is merged, get rid of this workaround.
def _unit_deepcopy(self, memo):
  ret = self.__class__(copy.deepcopy(self._units))
  return ret

_UNIT_REGISTRY.Unit.__deepcopy__ = _unit_deepcopy


# Fix for https://github.com/hgrecco/pint/issues/372
_UNIT_REGISTRY.Unit.__ne__ = lambda self, other: not self.__eq__(other)


# Forward access to pint's classes and functions.
DimensionalityError = pint.DimensionalityError
ParseExpression = _UNIT_REGISTRY.parse_expression
Quantity = _UNIT_REGISTRY.Quantity
Unit = _UNIT_REGISTRY.Unit
byte = Unit('byte')
bit = Unit('bit')
second = Unit('second')
percent = Unit('percent')
