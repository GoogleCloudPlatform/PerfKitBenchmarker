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
"""Utilities for working with Rackspace Cloud Platform resources."""

import os
import re

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS

PROPERTY_VALUE_ROW_REGEX = r'\|\s+(:?\S+\s\S+|\S+)\s+\|\s+(.*?)\s+\|'
PROP_VAL_PATTERN = re.compile(PROPERTY_VALUE_ROW_REGEX)


def ParseNovaTable(output):
  """Returns a dict with key/values returned from a Nova CLI formatted table.

  Returns:
  dict with key/values of the resource requested.
  """
  stdout_lines = output.split('\n')
  groups = (PROP_VAL_PATTERN.match(line) for line in stdout_lines)
  tuples = (g.groups() for g in groups if g)
  filtered_tuples = ((key, val) for (key, val) in tuples
                     if key and key not in ('', 'Property',))
  return dict(filtered_tuples)


def GetDefaultRackspaceCommonEnv(zone='IAD'):
  """Return common set of environment variables for using any OpenStack client
  against the Rackspace Public Cloud.

  Args:
  zone: string specifying Rackspace region to use.

  Returns:
  dict of environment variables
  """
  env = {
      'OS_AUTH_URL': os.getenv('OS_AUTH_URL',
                               'https://identity.api.rackspacecloud.com/v2.0/'),
      'OS_AUTH_SYSTEM': os.getenv('OS_AUTH_SYSTEM', 'rackspace'),
      'OS_SERVICE_NAME': os.getenv('OS_SERVICE_NAME', 'cloudServersOpenStack'),
      'OS_REGION_NAME': zone,
      'OS_USERNAME': os.getenv('OS_USERNAME'),
      'OS_PASSWORD': os.getenv('OS_PASSWORD'),
      'OS_TENANT_NAME': os.getenv('OS_TENANT_NAME'),
      'OS_NO_CACHE': os.getenv('OS_NO_CACHE', '1'),
  }

  environment_vars_missing = []
  for key, val in env.items():
    if val is None:
      environment_vars_missing.append(key)

  if len(environment_vars_missing) != 0:
    msg = ('The following required environment variables were not found:\n',
           '\n'.join(environment_vars_missing),
           '\n\nPlease make sure to source them into your environment, and try',
           ' again.',)
    raise errors.Error(''.join(msg))

  return env


def GetDefaultRackspaceNovaEnv(zone):
  """Return common set of environment variables for using the Nova client
  against the Rackspace Public Cloud.

  Args:
  zone: string specifying Rackspace region to use.

  Returns:
  dict of environment variables for Nova
  """
  env = GetDefaultRackspaceCommonEnv(zone)
  env.update({
      'NOVA_RAX_AUTH': os.getenv('NOVA_RAX_AUTH', '1'),
  })

  return env


def GetDefaultRackspaceNeutronEnv(zone):
  """Return common set of environment variables for using the Neutron client
  against the Rackspace Public Cloud.

  Args:
  zone: string specifying Rackspace region to use.

  Returns:
  dict of environment variables for Neutron
  """
  env = GetDefaultRackspaceCommonEnv(zone)

  return env
