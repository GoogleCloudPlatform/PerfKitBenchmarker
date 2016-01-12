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
    option: string. Name of the config option.
    required: boolean. True if the config option is required. False if not.
  """

  __metaclass__ = abc.ABCMeta

  def __init__(self, option, **kwargs):
    """Initializes a ConfigOptionDecoder.

    Args:
      option: string. Name of the config option.
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

  def __init__(self, option, valid_values, **kwargs):
    """Initializes the EnumVerifier.

    Args:
    option: string.  Name of the config option.
    valid_values: list of the allowed values
    **kwargs: Keyword arguments to pass to the base class.
    """
    super(EnumDecoder, self).__init__(option, **kwargs)
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
          'Invalid {0}.{1} value: "{2}". Value must be one '
          'of the following: {3}.'.format(
              component_full_name, self.option, value,
              ', '.join(str(t) for t in self.valid_values)))



class TypeVerifier(ConfigOptionDecoder):
  """Verifies that a config option value's type belongs to an allowed set.

  Passes value through unmodified.
  """

  def __init__(self, option, valid_types, none_ok=False, **kwargs):
    """Initializes a TypeVerifier.

    Args:
      option: string. Name of the config option.
      valid_types: tuple of allowed types.
      none_ok: boolean. If True, None is also an allowed option value.
      **kwargs: Keyword arguments to pass to the base class.
    """
    super(TypeVerifier, self).__init__(option, **kwargs)
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
          'Invalid {0}.{1} value: "{2}" (of type "{3}"). Value must be one '
          'of the following types: {4}.'.format(
              component_full_name, self.option, value, value.__class__.__name__,
              ', '.join(t.__name__ for t in self._valid_types)))
    return value


class BooleanDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a boolean is expected."""

  def __init__(self, option, **kwargs):
    super(BooleanDecoder, self).__init__(option, (bool,), **kwargs)


class IntDecoder(TypeVerifier):
  """Verifies and decodes a config option value when an integer is expected.

  Attributes:
    max: None or int. If provided, it specifies the maximum accepted value.
    min: None or int. If provided, it specifies the minimum accepted value.
  """

  def __init__(self, option, max=None, min=None, **kwargs):
    super(IntDecoder, self).__init__(option, (int,), **kwargs)
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
      if self.max and value > self.max:
        raise errors.Config.InvalidValue(
            'Invalid {0}.{1} value: "{2}". Value must be at most '
            '{3}.'.format(component_full_name, self.option, value, self.max))
      if self.min and value < self.min:
        raise errors.Config.InvalidValue(
            'Invalid {0}.{1} value: "{2}". Value must be at least '
            '{3}.'.format(component_full_name, self.option, value, self.min))
    return value


class FloatDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a float is expected.

  Attributes:
    max: None or float. If provided, it specifies the maximum accepted value.
    min: None or float. If provided, it specifies the minimum accepted value.
  """

  def __init__(self, option, max=None, min=None, **kwargs):
    super(FloatDecoder, self).__init__(option, (float, int), **kwargs)
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
      if self.max and value > self.max:
        raise errors.Config.InvalidValue(
            'Invalid {0}.{1} value: "{2}". Value must be at most '
            '{3}.'.format(component_full_name, self.option, value, self.max))
      if self.min and value < self.min:
        raise errors.Config.InvalidValue(
            'Invalid {0}.{1} value: "{2}". Value must be at least '
            '{3}.'.format(component_full_name, self.option, value, self.min))
    return value


class StringDecoder(TypeVerifier):
  """Verifies and decodes a config option value when a string is expected."""

  def __init__(self, option, **kwargs):
    super(StringDecoder, self).__init__(option, (basestring,), **kwargs)
