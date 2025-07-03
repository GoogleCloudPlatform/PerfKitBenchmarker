# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing mongodb installation and cleanup functions."""

import logging

from absl import flags
import yaml


FLAGS = flags.FLAGS

VERSION = flags.DEFINE_string(
    'mongodb_version', '8.0', 'Version of mongodb package.'
)

MONGOD_CONF_WIRED_TIGER_CACHE_SIZE_GB = flags.DEFINE_integer(
    'mongod_conf_wired_tiger_cache_size_gb',
    None,
    'The maximum amount of RAM, in gigabytes, that the WiredTiger storage'
    ' engine uses for its internal cache to hold data and indexes. This is the'
    ' most critical memory tuning parameter. Data in this cache is'
    ' uncompressed. Default Value: 50% of (Total System RAM - 1 GB), or 256 MB,'
    ' whichever is larger.',
)

MONGOD_CONF_WIRED_TIGER_SESSION_MAX = flags.DEFINE_string(
    'mongod_conf_wired_tiger_session_max',
    None,
    'An internal WiredTiger parameter that sets the maximum number of'
    ' concurrent sessions (or connections) the storage engine can handle. This'
    ' is often aligned with the maximum number of client connections. Default'
    ' value is 100 based on WiredTiger documentation.',
)

MONGOD_CONF_NETWORK_MAX_INCOMING_CONNECTIONS = flags.DEFINE_integer(
    'mongod_conf_network_max_incoming_connections',
    None,
    'The maximum number of simultaneous client connections that the mongod'
    ' process will accept on network layer. Default for Windows is 1,000,000.'
    ' Default for Linux is (RLIMIT_NOFILE) * 0.8, which is OS-dependent.',
)

MONGOD_CONF_LOCK_CODE_SEGMENTS_IN_MEMORY = flags.DEFINE_boolean(
    'mongod_conf_lock_code_segments_in_memory',
    None,
    'A hint to the operating system to try and keep the MongoDB server program'
    ' code locked in physical RAM, which can prevent it from being paged out to'
    ' disk and improve performance consistency. No mentioning in Monodb public'
    ' doc. Possibly an Atlas-specific setting.',
)

MONGOD_CONF_INTERNAL_QUERY_STATS_RATE_LIMIT = flags.DEFINE_integer(
    'mongod_conf_internal_query_stats_rate_limit',
    None,
    'The maximum rate at which query statistics are recorded. This helps limit'
    ' the performance overhead of diagnostic logging. No mentioning in Monodb'
    ' public doc. Possibly an Atlas-specific setting.',
)

MONGOD_CONF_MIN_SNAPSHOT_HISTORY_WINDOW_IN_SECONDS = flags.DEFINE_integer(
    'mongod_conf_min_snapshot_history_window_in_seconds',
    None,
    'The minimum amount of time, in seconds, that the WiredTiger storage'
    ' engine will retain snapshot history data. This data is used for things'
    ' like replica set rollbacks. Default value is 300.',
)

MONGODB_LOG_LEVEL = flags.DEFINE_integer(
    'mongodb_log_level',
    None,
    'MongoDB log level, verbosity increases with level. Default value is 0.',
    0,
    5,
)

MONGODB_QUERY_STATS_COMPONENT_LOG_LEVEL = flags.DEFINE_integer(
    'mongodb_query_stats_component_log_level',
    None,
    'MongoDB log level for queryStats component, verbosity increases with'
    ' level. Default value is 0.',
    0,
    5,
)

# For flags that are used to modify mongod.conf file, map the unique flag name
# (a string) to its corresponding YAML path.
FLAG_NAME_TO_PATH_MAP = {
    MONGOD_CONF_WIRED_TIGER_CACHE_SIZE_GB.name: (
        'storage',
        'wiredTiger',
        'engineConfig',
        'cacheSizeGB',
    ),
    MONGOD_CONF_WIRED_TIGER_SESSION_MAX.name: (
        'storage',
        'wiredTiger',
        'engineConfig',
        'configString',
    ),
    MONGOD_CONF_NETWORK_MAX_INCOMING_CONNECTIONS.name: (
        'net',
        'maxIncomingConnections',
    ),
    MONGOD_CONF_LOCK_CODE_SEGMENTS_IN_MEMORY.name: (
        'setParameter',
        'lockCodeSegmentsInMemory',
    ),
    MONGOD_CONF_INTERNAL_QUERY_STATS_RATE_LIMIT.name: (
        'setParameter',
        'internalQueryStatsRateLimit',
    ),
    MONGOD_CONF_MIN_SNAPSHOT_HISTORY_WINDOW_IN_SECONDS.name: (
        'setParameter',
        'minSnapshotHistoryWindowInSeconds',
    ),
    MONGODB_LOG_LEVEL.name: ('systemLog', 'verbosity'),
    MONGODB_QUERY_STATS_COMPONENT_LOG_LEVEL.name: (
        'systemLog',
        'component',
        'queryStats',
        'verbosity',
    ),
}


def _GetServiceName():
  """Returns the name of the mongodb service."""
  return 'mongod'


def _GetConfigPath():
  """Returns the path to the mongodb config file."""
  return '/etc/mongod.conf'


def _Setup(vm):
  """Setup mongodb."""
  config_path = _GetConfigPath()

  # Read the existing config file from the VM
  stdout, _ = vm.RemoteCommand(f'sudo cat {config_path}')

  # Initialize the PyYAML parser. Note: This will strip comments.
  try:
    config_data = yaml.safe_load(stdout)
    if not config_data:
      logging.warning(
          'Existing mongod.conf file with basic settings should exist.'
      )
      config_data = {}
  except yaml.YAMLError as e:
    logging.warning('Could not parse existing mongod.conf: %s', e)
    config_data = {}

  # Apply static configurations
  config_data.setdefault('net', {})['bindIp'] = '::,0.0.0.0'
  if not FLAGS.mongodb_primary_only:
    config_data.setdefault('replication', {})['replSetName'] = 'rs0'

  # Apply flag-driven configurations

  def SetNestedValue(d, path, value):
    """Navigates a dictionary path and sets a value, creating keys if needed."""
    for key in path[:-1]:
      d = d.setdefault(key, {})
    d[path[-1]] = value

  # A list of the flags to be processed.
  flags_to_process = [
      MONGOD_CONF_WIRED_TIGER_CACHE_SIZE_GB,
      MONGOD_CONF_WIRED_TIGER_SESSION_MAX,
      MONGOD_CONF_NETWORK_MAX_INCOMING_CONNECTIONS,
      MONGOD_CONF_LOCK_CODE_SEGMENTS_IN_MEMORY,
      MONGOD_CONF_INTERNAL_QUERY_STATS_RATE_LIMIT,
      MONGOD_CONF_MIN_SNAPSHOT_HISTORY_WINDOW_IN_SECONDS,
      MONGODB_LOG_LEVEL,
      MONGODB_QUERY_STATS_COMPONENT_LOG_LEVEL,
  ]

  # Loop through the flags and apply any that have been set by the user.
  for flag in flags_to_process:
    if flag.value is not None:
      path = FLAG_NAME_TO_PATH_MAP.get(flag.name)
      if path:
        SetNestedValue(config_data, path, flag.value)

  # Convert the data structure back to a YAML string
  # default_flow_style=False ensures block-style output for readability.
  new_config_content = yaml.dump(config_data, default_flow_style=False)

  # Write the new configuration back to the file on the VM
  vm.RemoteCommand(f'echo """{new_config_content}""" | sudo tee {config_path}')


def YumSetup(vm):
  """Performs common pre-install setup for mongodb .rpm packages."""
  vm.RemoteCommand('sudo setenforce 0')
  releasever, _ = vm.RemoteCommand(
      "distro=$(sed -n 's/^distroverpkg=//p' /etc/yum.conf);"
      'echo $(rpm -q --qf "%{version}" -f /etc/$distro)'
  )
  mongodb_repo = (
      f'[mongodb-org-{VERSION.value}]\nname=MongoDB Repository\nbaseurl='
      f'https://repo.mongodb.org/yum/redhat/{releasever.strip()}/mongodb-org/{VERSION.value}/x86_64/'
      '\ngpgcheck=0\nenabled=1'
  )
  vm.RemoteCommand(
      f'echo "{mongodb_repo}" | sudo tee'
      f' /etc/yum.repos.d/mongodb-org-{VERSION.value}.repo'
  )


def YumInstall(vm):
  """Installs the mongodb package on the VM."""
  YumSetup(vm)
  vm.InstallPackages('mongodb-org')
  _Setup(vm)


def AptSetup(vm):
  """Performs common pre-install setup for mongodb .deb packages."""
  vm.InstallPackages('gnupg curl')
  vm.RemoteCommand(
      f'sudo rm -rf /usr/share/keyrings/mongodb-server-{VERSION.value}.gpg'
  )
  vm.RemoteCommand(
      f'curl -fsSL https://pgp.mongodb.com/server-{VERSION.value}.asc | sudo'
      f' gpg -o /usr/share/keyrings/mongodb-server-{VERSION.value}.gpg'
      ' --dearmor'
  )
  vm.RemoteCommand(
      'echo "deb [ arch=amd64,arm64'
      f' signed-by=/usr/share/keyrings/mongodb-server-{VERSION.value}.gpg ]'
      ' https://repo.mongodb.org/apt/ubuntu'
      f' jammy/mongodb-org/{VERSION.value} multiverse" | sudo tee'
      f' /etc/apt/sources.list.d/mongodb-org-{VERSION.value}.list'
  )
  vm.AptUpdate()


def AptInstall(vm):
  """Installs the mongodb package on the VM."""
  AptSetup(vm)
  vm.InstallPackages('mongodb-org')
  _Setup(vm)


def YumUninstall(vm):
  """Uninstalls the mongodb package on the VM."""
  vm.RemoteCommand(f'sudo rm /etc/yum.repos.d/mongodb-org-{VERSION.value}.repo')


def YumGetServiceName(vm):
  """Returns the name of the mongodb service."""
  del vm
  return _GetServiceName()


def AptGetServiceName(vm):
  """Returns the name of the mongodb service."""
  del vm
  return _GetServiceName()


def YumGetPathToConfig(vm):
  """Returns the path to the mongodb config file."""
  del vm
  return _GetConfigPath()


def AptGetPathToConfig(vm):
  """Returns the path to the mongodb config file."""
  del vm
  return _GetConfigPath()
