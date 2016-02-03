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
"""Base class for objects decoded from a YAML config."""

from collections import OrderedDict
import threading

from perfkitbenchmarker import errors


class BaseSpecMetaClass(type):
  """Metaclass that allows each BaseSpec derived class to have its own decoders.
  """

  def __init__(cls, name, bases, dct):
    super(BaseSpecMetaClass, cls).__init__(name, bases, dct)
    cls._init_decoders_lock = threading.Lock()
    cls._decoders = OrderedDict()
    cls._required_options = set()


class BaseSpec(object):
  """Object decoded from a YAML config."""

  __metaclass__ = BaseSpecMetaClass

  # Each derived class has its own copy of the following three variables. They
  # are initialized by BaseSpecMetaClass.__init__ and later populated by
  # _InitDecoders when the first instance of the derived class is created.
  _init_decoders_lock = None  # threading.Lock that protects the next two vars.
  _decoders = None  # dict mapping config option name to ConfigOptionDecoder.
  _required_options = None  # set of strings. Required config options.

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    """Initializes a BaseSpec.

    Translates keyword arguments via the class's decoders and assigns the
    corresponding instance attribute. Derived classes can register decoders
    for additional attributes by overriding _GetOptionDecoderConstructions
    and can add support for additional flags by overriding _ApplyFlags.

    Args:
      component_full_name: string. Fully qualified name of the configurable
          component containing the config options.
      flag_values: None or flags.FlagValues. Runtime flags that may override
          the provided config option values in kwargs.
      **kwargs: dict mapping config option names to provided values.

    Raises:
      errors.Config.MissingOption: If a config option is required, but a value
          was not provided in kwargs.
      errors.Config.UnrecognizedOption: If an unrecognized config option is
          provided with a value in kwargs.
    """
    if not self._decoders:
      self._InitDecoders()
    if flag_values:
      self._ApplyFlags(kwargs, flag_values)
    missing_options = self._required_options.difference(kwargs)
    if missing_options:
      raise errors.Config.MissingOption(
          'Required options were missing from {0}: {1}.'.format(
              component_full_name, ', '.join(sorted(missing_options))))
    unrecognized_options = frozenset(kwargs).difference(self._decoders)
    if unrecognized_options:
      raise errors.Config.UnrecognizedOption(
          'Unrecognized options were found in {0}: {1}.'.format(
              component_full_name, ', '.join(sorted(unrecognized_options))))
    self._DecodeAndInit(component_full_name, kwargs, self._decoders,
                        flag_values)

  @classmethod
  def _InitDecoders(cls):
    """Creates a ConfigOptionDecoder for each config option.

    Populates cls._decoders and cls._required_options.
    """
    with cls._init_decoders_lock:
      if not cls._decoders:
        constructions = cls._GetOptionDecoderConstructions()
        for option, decoder_construction in sorted(constructions.iteritems()):
          decoder_class, init_args = decoder_construction
          decoder = decoder_class(option=option, **init_args)
          cls._decoders[option] = decoder
          if decoder.required:
            cls._required_options.add(option)

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    pass

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    return {}

  def _DecodeAndInit(self, component_full_name, config, decoders, flag_values):
    """Initializes spec attributes from provided config option values.

    Args:
      component_full_name: string. Fully qualified name of the configurable
          component containing the config options.
      config: dict mapping option name string to option value.
      flag_values: flags.FlagValues. Runtime flags that may override provided
          config option values. These flags have already been applied to the
          current config, but they may be passed to the decoders for propagation
          to deeper spec constructors.
      decoders: OrderedDict mapping option name string to ConfigOptionDecoder.
    """
    assert isinstance(decoders, OrderedDict), (
        'decoders must be an OrderedDict. The order in which options are '
        'decoded must be guaranteed.')
    for option, decoder in decoders.iteritems():
      if option in config:
        value = decoder.Decode(config[option], component_full_name, flag_values)
      else:
        value = decoder.default
      setattr(self, option, value)
