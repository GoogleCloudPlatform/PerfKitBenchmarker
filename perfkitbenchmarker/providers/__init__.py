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

import logging
import os

from perfkitbenchmarker import import_util
from perfkitbenchmarker import requirements

# This unconditionally loads any modules in any provider
# directory with the name 'flags'. It is expected that providers
# add all flags into a separate file called 'flags.py'. This enables
# us to correctly show the flags as part of the help text without
# actually loading any other provider specific modules.
import_util.LoadModulesWithName(__path__, __name__, 'flags')

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

VALID_CLOUDS = [GCP, AZURE, AWS, DIGITALOCEAN, KUBERNETES, OPENSTACK,
                RACKSPACE, CLOUDSTACK, ALICLOUD, MESOS]


def LoadProvider(provider_name, ignore_package_requirements=True):
  """Loads the all modules in the 'provider_name' package.

  This function first checks the specified provider's Python package
  requirements file, if one exists, and verifies that all requirements are met.
  Next, it loads all modules in the specified provider's package. By loading
  these modules, relevant classes (e.g. VMs) will register themselves.

  Args:
    provider_name: The name of the package whose modules should be loaded.
        Usually the name of the provider in lower case (e.g. the package name
        for the 'GCP' provider is 'gcp'.
    ignore_package_requirements: boolean. If True, the provider's Python package
        requirements file is ignored.
  """
  if not ignore_package_requirements:
    requirements.CheckProviderRequirements(provider_name)
  provider_package_path = os.path.join(__path__[0], provider_name)
  try:
    # Iterating through this generator will load all modules in the provider
    # directory. Simply loading those modules will cause relevant classes
    # to register themselves so that we can run with that provider.
    modules = [module for module in
               import_util.LoadModulesForPath([provider_package_path],
                                              __name__ + '.' + provider_name)]
    if not modules:
      raise ImportError('No modules found for provider.')
  except Exception:
    logging.error('Unable to load provider %s.', provider_name)
    raise
