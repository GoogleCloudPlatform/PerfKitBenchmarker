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
"""Configuration files for benchmarks."""

import copy
import yaml

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS
CONFIG_CONSTANTS = 'default_config_constants.yaml'

flags.DEFINE_string('benchmark_config_file', None,
                    'The file path to the user config file which will '
                    'override benchmark defaults.')
flags.DEFINE_multistring(
    'config_override', None,
    'This flag can be used to override any config value. It is applied after '
    'the user config (specified via --benchmark_config_file_path), so it has '
    'a higher priority than that config. The value of the flag should be '
    'fully.qualified.key=value (e.g. --config_override=cluster_boot.vm_groups.'
    'default.vm_count=4). This flag can be repeated.')


def _LoadUserConfig(path):
  """Loads a user config from the supplied path."""
  with open(data.ResourcePath(path)) as fp:
    return yaml.load(fp.read())


def _GetConfigFromOverrides(overrides):
  """Converts a list of overrides into a config."""
  config = {}

  for override in overrides:
    if override.count('=') != 1:
      raise ValueError('--config_override flag value has incorrect number of '
                       '"=" characters. The value must take the form '
                       'fully.qualified.key=value.')
    full_key, value = override.split('=')
    keys = full_key.split('.')
    new_config = {keys.pop(): yaml.load(value)}
    while keys:
      new_config = {keys.pop(): new_config}
    config = MergeConfigs(config, new_config)

  return config


def MergeConfigs(default_config, delta_config):
  """Merges the delta config into the default config."""
  def _Merge(d1, d2):
    merged_dict = copy.deepcopy(d1)
    for k, v in d2.iteritems():
      if k not in d1:
        merged_dict[k] = copy.deepcopy(v)
      elif isinstance(v, dict):
        merged_dict[k] = _Merge(d1[k], v)
      else:
        merged_dict[k] = v
    return merged_dict

  if delta_config:
    return _Merge(default_config, delta_config)
  else:
    return default_config


def LoadMinimalConfig(benchmark_config, benchmark_name):
  """Loads a benchmark config without using any flags in the process.

  This function will prepend a constants file to the benchmark config prior
  to loading it. This allows the config to use references to anchors defined
  in the constants file.

  Args:
    benchmark_config: A string containing the default config in yaml format.

  Returns:
    A tuple of the benchmark name and config.
  """
  yaml_config = []
  with open(data.ResourcePath(CONFIG_CONSTANTS)) as fp:
    yaml_config.append(fp.read())
  yaml_config.append(benchmark_config)

  config = yaml.load('\n'.join(yaml_config))

  return config[benchmark_name]


def LoadConfig(benchmark_config, benchmark_name):
  """Loads a benchmark config.

  Args:
    benchmark_config: A string containing the default config in yaml format.

  Returns:
    A tuple of the benchmark name and config.
  """
  try:
    config = LoadMinimalConfig(benchmark_config, benchmark_name)

    if FLAGS.benchmark_config_file:
      user_config = _LoadUserConfig(FLAGS.benchmark_config_file)
      config = MergeConfigs(config, user_config.get(benchmark_name))

    if FLAGS.config_override:
      override_config = _GetConfigFromOverrides(FLAGS.config_override)
      config = MergeConfigs(config, override_config.get(benchmark_name))

  except yaml.parser.ParserError as e:
    raise errors.Config.ParseError(
        'Encountered a problem loading config. Please ensure that the config '
        'is valid YAML. Error received:\n%s' % e)

  return config
