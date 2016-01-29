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
"""Configuration files for benchmarks.

Each benchmark has a default configuration defined inside its module.
The configuration is written in YAML (www.yaml.org) and specifies what
resources are needed to run the benchmark. Users can write their own
config files, which will be merged with the default configuration. These
config files specify overrides to the default configuration. Users can also
specify which benchmarks to run in the same config file.

Valid top level keys:
  benchmarks: A YAML array of dictionaries mapping benchmark names to their
      configs. This also determines which benchmarks to run.
  *any_benchmark_name*: If the 'benchmarks' key is not specified, then
      specifying a benchmark name mapped to a config will override
      that benchmark's default configuration in the event that that
      benchmark is run.

Valid config keys:
  vm_groups: A YAML dictionary mapping the names of VM groups to the groups
      themselves. These names can be any string.
  description: A description of the benchmark.
  flags: A YAML dictionary with overrides for default flag values.

Valid VM group keys:
  vm_spec: A YAML dictionary mapping names of clouds (e.g. AWS) to the
      actual VM spec.
  disk_spec: A YAML dictionary mapping names of clouds to the actual
      disk spec.
  vm_count: The number of VMs to create in this group. If this key isn't
      specified, it defaults to 1.
  disk_count: The number of disks to attach to VMs of this group. If this key
      isn't specified, it defaults to 1.
  cloud: The name of the cloud to create the group in. This is used for
      multi-cloud configurations.
  os_type: The OS type of the VMs to create (see the flag of the same name for
      more information). This is used if you want to run a benchmark using VMs
      with different OS types (e.g. Debian and RHEL).
  static_vms: A YAML array of Static VM specs. These VMs will be used before
      any Cloud VMs are created. The total number of VMs will still add up to
      the number specified by the 'vm_count' key.

For valid VM spec keys, see virtual_machine.BaseVmSpec and derived classes.
For valid disk spec keys, see disk.BaseDiskSpec and derived classes.

See configs.spec.BaseSpec for more information about adding additional keys to
VM specs, disk specs, or any component of the benchmark configuration
dictionary.
"""

import copy
import logging
import yaml

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS
CONFIG_CONSTANTS = 'default_config_constants.yaml'

flags.DEFINE_string('benchmark_config_file', None,
                    'The file path to the user config file which will '
                    'override benchmark defaults. This should either be '
                    'a path relative to the current working directory, '
                    'an absolute path, or just the name of a file in the '
                    'configs/ directory.')
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


def GetUserConfig():
  """Returns the user config with any overrides applied.

  This loads config from --benchmark_config_file and merges it with
  any overrides specified via --config_override and returns the result.

  Returns:
    dict. The result of merging the loaded config from the
    --benchmark_config_file flag with the config generated from the
    --config override flag.
  """
  try:
    if FLAGS.benchmark_config_file:
      config = _LoadUserConfig(FLAGS.benchmark_config_file)
    else:
      config = {}

    if FLAGS.config_override:
      override_config = _GetConfigFromOverrides(FLAGS.config_override)
      config = MergeConfigs(config, override_config)

  except yaml.parser.ParserError as e:
    raise errors.Config.ParseError(
        'Encountered a problem loading config. Please ensure that the config '
        'is valid YAML. Error received:\n%s' % e)
  except yaml.composer.ComposerError as e:
    raise errors.Config.ParseError(
        'Encountered a problem loading config. Please ensure that all '
        'references are defined. Error received:\n%s' % e)

  return config


def MergeConfigs(default_config, override_config, warn_new_key=False):
  """Merges the override config into the default config.

  This function will recursively merge two nested dicts.
  The override_config represents overrides to the default_config dict, so any
  leaf key/value pairs which are present in both dicts will take their value
  from the override_config.

  Args:
    default_config: The dict which will have its values overridden.
    override_config: The dict wich contains the overrides.
    warn_new_key: Determines whether we warn the user if the override config
      has a key that the default config did not have.

  Returns:
    A dict containing the values from the default_config merged with those from
    the override_config.
  """
  def _Merge(d1, d2):
    merged_dict = copy.deepcopy(d1)
    for k, v in d2.iteritems():
      if k not in d1:
        merged_dict[k] = copy.deepcopy(v)
        if warn_new_key:
          logging.warning('The key "%s" was not in the default config, '
                          'but was in user overrides. This may indicate '
                          'a typo.' % k)
      elif isinstance(v, dict):
        merged_dict[k] = _Merge(d1[k], v)
      else:
        merged_dict[k] = v
    return merged_dict

  if override_config:
    return _Merge(default_config, override_config)
  else:
    return default_config


def LoadMinimalConfig(benchmark_config, benchmark_name):
  """Loads a benchmark config without using any flags in the process.

  This function will prepend configs/default_config_constants.yaml to the
  benchmark config prior to loading it. This allows the config to use
  references to anchors defined in the constants file.

  Args:
    benchmark_config: str. The default config in YAML format.
    benchmark_name: str. The name of the benchmark.

  Returns:
    dict. The loaded config.
  """
  yaml_config = []
  with open(data.ResourcePath(CONFIG_CONSTANTS, False)) as fp:
    yaml_config.append(fp.read())
  yaml_config.append(benchmark_config)

  try:
    config = yaml.load('\n'.join(yaml_config))
  except yaml.parser.ParserError as e:
    raise errors.Config.ParseError(
        'Encountered a problem loading the default benchmark config. Please '
        'ensure that the config is valid YAML. Error received:\n%s' % e)
  except yaml.composer.ComposerError as e:
    raise errors.Config.ParseError(
        'Encountered a problem loading the default benchmark config. Please '
        'ensure that all references are defined. Error received:\n%s' % e)

  return config[benchmark_name]


def LoadConfig(benchmark_config, user_config, benchmark_name):
  """Loads a benchmark configuration.

  This function loads a benchmark's default configuration (in YAML format),
  then merges it with any overrides the user provided, and returns the result.
  This loaded config is then passed to the benchmark_spec.BenchmarkSpec
  constructor in order to create a BenchmarkSpec.

  Args:
    benchmark_config: str. The default configuration in YAML format.
    user_config: dict. The loaded user config for the benchmark.
    benchmark_name: str. The name of the benchmark.

  Returns:
    dict. The loaded config.
  """
  config = LoadMinimalConfig(benchmark_config, benchmark_name)
  config = MergeConfigs(config, user_config, warn_new_key=True)
  return config
