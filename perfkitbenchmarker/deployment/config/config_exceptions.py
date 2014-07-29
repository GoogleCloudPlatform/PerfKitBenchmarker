#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
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

"""Exceptions raised by the deployment helper library."""




class ConfigError(Exception):
  """Top level exception for all Deploy errors."""
  pass


class DeployEmptyConfigError(ConfigError):
  """Either config file not found, or empty config file."""
  pass


class BadStaticReferenceError(ConfigError):
  """Could not resolve the static reference at parse time."""
  pass


class InvalidPdError(ConfigError):
  """The PD name or specs are invalid in the configuration file."""
  pass


class InvalidVmNameError(ConfigError):
  """The node names are not valid VM names."""
  pass


class NoClusterSectionInConfigError(ConfigError):
  """No cluster section was specified in the configuration file."""
  pass


class NoClusterTypeInConfigError(ConfigError):
  """No cluster type was specified in the configuration file."""
  pass


class NoSetupModulesInConfigError(ConfigError):
  """No setup module list was specified in the configuration file."""
  pass


class NoProjectInConfigError(ConfigError):
  """No project specified in the configuration file."""
  pass


class NoZoneInConfigError(ConfigError):
  """No zone specified in the configuration file."""
  pass


class NoAdminUserInConfigError(ConfigError):
  """No administrator user specified in the configuration file."""
  pass


class NoNetworkSectionInConfigError(ConfigError):
  """No network section was specified in the configuration file."""
  pass


class NoNetworkNameInConfigError(ConfigError):
  """No network name specified in the configuration file."""
  pass


class NoTcpPortsInConfigError(ConfigError):
  """No TCP ports specified in the network section of the configuration file."""
  pass


class NoUdpPortsInConfigError(ConfigError):
  """No UDP ports specified in the network section of the configuration file."""
  pass


class InvalidNetworkPortInConfigError(ConfigError):
  """Not a single node definition was found on configuration file."""
  pass


class NoNodeTypesInConfigError(ConfigError):
  """Not a single node definition was found on configuration file."""
  pass
