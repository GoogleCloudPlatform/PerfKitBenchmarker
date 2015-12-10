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

from perfkitbenchmarker import errors


class ConfigOptionDecoder(object):
  """Verifies and decodes a config option value.

  Attributes:
    component: string. Description of the component to which the option applies.
    option: string. Name of the config option.
    required: boolean. True if the config option is required. False if not.
  """

  __metaclass__ = abc.ABCMeta

  def __init__(self, component, option, **kwargs):
    """Initializes a ConfigOptionDecoder.

    Args:
      component: string. Description of the component to which the option
          applies.
      option: string. Name of the config option.
      **kwargs: May optionally contain a 'default' key mapping to a value or
          callable object. If a value is provided, the config option is
          optional, and the provided value is the default if the user does not
          set a value for the config option. If a callable object is provided,
          the config option is optional, and the provided object is called to
          determine the value if the user does not set a value for the config
          option. If not provided, the config option is required.
    """
    self.component = component
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
        'Attempted to get the default value of {0} required config option '
        '"{1}".'.format(self.component, self.option))
    if hasattr(self._default, '__call__'):
      return self._default()
    return self._default

  @abc.abstractmethod
  def Decode(self, value):
    """Verifies and decodes a config option value.

    Args:
      value: The value specified in the config.

    Returns:
      The decoded value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    raise NotImplementedError()


class BooleanDecoder(ConfigOptionDecoder):
  """Verifies and decodes a config option value when a boolean is expected."""

  def Decode(self, value):
    """Verifies that the provided value is a boolean.

    Args:
      value: The value specified in the config.

    Returns:
      boolean. The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if not isinstance(value, bool):
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}" (of type "{3}"). Value must be a '
          'boolean.'.format(self.component, self.option, value,
                            value.__class__.__name__))
    return value


class IntDecoder(ConfigOptionDecoder):
  """Verifies and decodes a config option value when an integer is expected.

  Attributes:
    max: None or int. If provided, it specifies the maximum accepted value.
    min: None or int. If provided, it specifies the minimum accepted value.
  """

  def __init__(self, component, option, max=None, min=None, **kwargs):
    super(IntDecoder, self).__init__(component, option, **kwargs)
    self.max = max
    self.min = min

  def Decode(self, value):
    """Verifies that the provided value is an int.

    Args:
      value: The value specified in the config.

    Returns:
      int. The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if not isinstance(value, int):
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}" (of type "{3}"). Value must be an '
          'integer.'.format(self.component, self.option, value,
                            value.__class__.__name__))
    elif self.max and value > self.max:
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}". Value must be at most {3}.'.format(
              self.component, self.option, value, self.max))
    elif self.min and value < self.min:
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}". Value must be at least {3}.'.format(
              self.component, self.option, value, self.min))
    return value


class FloatDecoder(ConfigOptionDecoder):
  """Verifies and decodes a config option value when a float is expected.

  Attributes:
    max: None or float. If provided, it specifies the maximum accepted value.
    min: None or float. If provided, it specifies the minimum accepted value.
  """

  def __init__(self, component, option, max=None, min=None, **kwargs):
    super(FloatDecoder, self).__init__(component, option, **kwargs)
    self.max = max
    self.min = min

  def Decode(self, value):
    """Verifies that the provided value is a float.

    Args:
      value: The value specified in the config.

    Returns:
      float. The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if not isinstance(value, float):
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}" (of type "{3}"). Value must be a '
          'float.'.format(self.component, self.option, value,
                          value.__class__.__name__))
    elif self.max and value > self.max:
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}". Value must be at most {3}.'.format(
              self.component, self.option, value, self.max))
    elif self.min and value < self.min:
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}". Value must be at least {3}.'.format(
              self.component, self.option, value, self.min))
    return value


class StringDecoder(ConfigOptionDecoder):
  """Verifies and decodes a config option value when a string is expected."""

  def Decode(self, value):
    """Verifies that the provided value is a string.

    Args:
      value: The value specified in the config.

    Returns:
      string. The valid value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if not isinstance(value, basestring):
      raise errors.Config.InvalidValue(
          'Invalid {0} "{1}" value: "{2}" (of type "{3}"). Value must be a '
          'string.'.format(self.component, self.option, value,
                           value.__class__.__name__))
    return value
