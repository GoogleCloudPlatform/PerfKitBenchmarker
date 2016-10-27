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
"""Classes for verifying and decoding config option values."""

import abc
import types

from perfkitbenchmarker import errors


class ConfigOptionDecoder(object):
  """Verifies and decodes a config option value.

  Attributes:
    option: None or string. Name of the config option.
    required: boolean. True if the config option is required. False if not.
  """

  __metaclass__ = abc.ABCMeta

  def __init__(self, option=None, **kwargs):
    """Initializes a ConfigOptionDecoder.

    Args:
      option: None or string. Name of the config option.
      **kwargs: May optionally contain a 'default' key mapping to a value or
          callable object. If a value is provided, the config option is
          optional, and the provided value is the default if the user does not
          set a value for the config option. If a callable object is provided,
          the config option is optional, and the provided object is called to
          determine the value if the user does not set a value for the config
          option. If not provided, the config option is required.
    """
    self.option = option
    self.required = 'default' not in kwargs
    if not self.required:
      self._default = kwargs.pop('default')
    assert not kwargs, ('__init__() received unexpected keyword arguments: '
                        '{0}'.format(kwargs))

  def _GetOptionFullName(self, component_full_name):
    """Returns the fully qualified name of a config option.

    Args:
      component_full_name: string. Fully qualified name of a configurable object
          to which the option belongs.
    """
    return (component_full_name if self.option is None
            else '{0}.{1}'.format(component_full_name, self.option))

  @property
  def default(self):
    """Gets the config option's default value.

    Returns:
      Default value of an optional config option.
    """
    assert not self.required, (
        'Attempted to get the default value of required config option '
        '"{0}".'.format(self.option))
    if hasattr(self._default, '__call__'):
      return self._default()
    return self._default

  @abc.abstractmethod
  def Decode(self, value, component_full_name, flag_values):
    """Verifies and decodes a config option value.

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      The decoded value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    raise NotImplementedError()


class EnumDecoder(ConfigOptionDecoder):
  """ Verifies that the config options value is in the allowed set.

  Passes through the value unmodified
  """

  def __init__(self, valid_values, **kwargs):
    """Initializes the EnumVerifier.

    Args:
      valid_values: list of the allowed values
      **kwargs: Keyword arguments to pass to the base class.
    """
    super(EnumDecoder, self).__init__(**kwargs)
    self.valid_values = valid_values

  def Decode(self, value, component_full_name, flag_values):
    """ Verifies that the provided value is in the allowed set.

    Args:
      value: The value specified in the config.
      component_full_name: string.  Fully qualified name of the
          configurable component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be
          propagated to the BaseSpec constructors.

    Returns:
      The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if value in self.valid_values:
     return value
    else:
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}". Value must be one of the following: '
          '{2}.'.format(self._GetOptionFullName(component_full_name), value,
                        ', '.join(str(t) for t in self.valid_values)))


class TypeVerifier(ConfigOptionDecoder):
  """Verifies that a config option value's type belongs to an allowed set.

  Passes value through unmodified.
  """

  def __init__(self, valid_types, none_ok=False, **kwargs):
    """Initializes a TypeVerifier.

    Args:
      valid_types: tuple of allowed types.
      none_ok: boolean. If True, None is also an allowed option value.
      **kwargs: Keyword arguments to pass to the base class.
    """
    super(TypeVerifier, self).__init__(**kwargs)
    if none_ok:
      self._valid_types = (types.NoneType,) + valid_types
    else:
      self._valid_types = valid_types

  def Decode(self, value, component_full_name, flag_values):
    """Verifies that the provided value is of an allowed type.

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if not isinstance(value, self._valid_types):
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}" (of type "{2}"). Value must be one of the '
          'following types: {3}.'.format(
              self._GetOptionFullName(component_full_name), value,
              value.__class__.__name__,
              ', '.join(t.__name__ for t in self._valid_types)))
    return value


class BooleanDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a boolean is expected."""

  def __init__(self, **kwargs):
    super(BooleanDecoder, self).__init__((bool,), **kwargs)


class IntDecoder(TypeVerifier):
  """Verifies and decodes a config option value when an integer is expected.

  Attributes:
    max: None or int. If provided, it specifies the maximum accepted value.
    min: None or int. If provided, it specifies the minimum accepted value.
  """

  def __init__(self, max=None, min=None, **kwargs):
    super(IntDecoder, self).__init__((int,), **kwargs)
    self.max = max
    self.min = min

  def Decode(self, value, component_full_name, flag_values):
    """Verifies that the provided value is an int.

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      int. The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    value = super(IntDecoder, self).Decode(value, component_full_name,
                                           flag_values)
    if value is not None:
      if self.max is not None and value > self.max:
        raise errors.Config.InvalidValue(
            'Invalid {0} value: "{1}". Value must be at most {2}.'.format(
                self._GetOptionFullName(component_full_name), value, self.max))
      if self.min is not None and value < self.min:
        raise errors.Config.InvalidValue(
            'Invalid {0} value: "{1}". Value must be at least {2}.'.format(
                self._GetOptionFullName(component_full_name), value, self.min))
    return value


class FloatDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a float is expected.

  Attributes:
    max: None or float. If provided, it specifies the maximum accepted value.
    min: None or float. If provided, it specifies the minimum accepted value.
  """

  def __init__(self, max=None, min=None, **kwargs):
    super(FloatDecoder, self).__init__((float, int), **kwargs)
    self.max = max
    self.min = min

  def Decode(self, value, component_full_name, flag_values):
    """Verifies that the provided value is a float.

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      float. The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    value = super(FloatDecoder, self).Decode(value, component_full_name,
                                             flag_values)
    if value is not None:
      if self.max is not None and value > self.max:
        raise errors.Config.InvalidValue(
            'Invalid {0} value: "{1}". Value must be at most {2}.'.format(
                self._GetOptionFullName(component_full_name), value, self.max))
      if self.min is not None and value < self.min:
        raise errors.Config.InvalidValue(
            'Invalid {0} value: "{1}". Value must be at least {2}.'.format(
                self._GetOptionFullName(component_full_name), value, self.min))
    return value


class StringDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a string is expected."""

  def __init__(self, **kwargs):
    super(StringDecoder, self).__init__((basestring,), **kwargs)


class ListDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a list is expected."""

  def __init__(self, item_decoder, **kwargs):
    """Initializes a ListDecoder.

    Args:
      item_decoder: ConfigOptionDecoder. Used to decode the items of an input
          list.
      **kwargs: Keyword arguments to pass to the base class.
    """
    super(ListDecoder, self).__init__((list,), **kwargs)
    self._item_decoder = item_decoder

  def Decode(self, value, component_full_name, flag_values):
    """Verifies that the provided value is a list with appropriate items.

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      None if the input value was None. Otherwise, a list containing the decoded
      value of each item in the input list.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    input_list = super(ListDecoder, self).Decode(value, component_full_name,
                                                 flag_values)
    if input_list is None:
      return None
    list_full_name = self._GetOptionFullName(component_full_name)
    result = []
    for index, input_item in enumerate(input_list):
      item_full_name = '{0}[{1}]'.format(list_full_name, index)
      result.append(self._item_decoder.Decode(input_item, item_full_name,
                                              flag_values))
    return result
