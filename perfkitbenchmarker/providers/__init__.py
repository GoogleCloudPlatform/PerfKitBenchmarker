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

import importlib
import logging
import os

from perfkitbenchmarker import events
from perfkitbenchmarker import import_util
from perfkitbenchmarker import requirements


GCP = 'GCP'
AZURE = 'Azure'
AWS = 'AWS'
ALICLOUD = 'AliCloud'
KUBERNETES = 'Kubernetes'
DIGITALOCEAN = 'DigitalOcean'
OPENSTACK = 'OpenStack'
CLOUDSTACK = 'CloudStack'
RACKSPACE = 'Rackspace'
MESOS = 'Mesos'
PROFITBRICKS = 'ProfitBricks'
YC = 'YandexCloud'
# Though Docker is not a cloud provider, it's inclusion is useful
# for performing on premise to cloud benchmarks
DOCKER = 'Docker'

VALID_CLOUDS = (GCP, AZURE, AWS, DIGITALOCEAN, YC, KUBERNETES, OPENSTACK,
                RACKSPACE, CLOUDSTACK, ALICLOUD, MESOS, PROFITBRICKS, DOCKER)


_imported_providers = set()


def LoadProviderFlags(providers):
  """Imports just the flags module for each provider.

  This allows PKB to load flag definitions from each provider to include in the
  help text without actually loading any other provider-specific modules.

  Args:
    providers: series of strings. Each element is a value from VALID_CLOUDS
        indicating a cloud provider for which to import the flags module.
  """
  for provider_name in providers:
    normalized_name = provider_name.lower()
    flags_module_name = '.'.join((__name__, normalized_name, 'flags'))
    importlib.import_module(flags_module_name)


# Import flag definitions for all cloud providers.
LoadProviderFlags(VALID_CLOUDS)


def LoadProvider(provider_name, ignore_package_requirements=True):
  """Loads the all modules in the 'provider_name' package.

  This function first checks the specified provider's Python package
  requirements file, if one exists, and verifies that all requirements are met.
  Next, it loads all modules in the specified provider's package. By loading
  these modules, relevant classes (e.g. VMs) will register themselves.

  Args:
    provider_name: string chosen from VALID_CLOUDS. The name of the provider
        whose modules should be loaded.
    ignore_package_requirements: boolean. If True, the provider's Python package
        requirements file is ignored.
  """
  if provider_name in _imported_providers:
    return

  # Check package requirements from the provider's pip requirements file.
  normalized_name = provider_name.lower()
  if not ignore_package_requirements:
    requirements.CheckProviderRequirements(normalized_name)

  # Load all modules in the provider's directory. Simply loading those modules
  # will cause relevant classes (e.g. VM and disk classes) to register
  # themselves so that they can be instantiated during resource provisioning.
  provider_package_path = os.path.join(__path__[0], normalized_name)
  try:
    modules = tuple(import_util.LoadModulesForPath(
        [provider_package_path], __name__ + '.' + normalized_name))
    if not modules:
      raise ImportError('No modules found for provider %s.' % provider_name)
  except Exception:
    logging.error('Unable to load provider %s.', provider_name)
    raise

  # Signal that the provider's modules have been imported.
  _imported_providers.add(provider_name)
  events.provider_imported.send(provider_name)
