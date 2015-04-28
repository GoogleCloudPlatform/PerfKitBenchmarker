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
"""Utilities for working with Rackspace Cloud Platform resources."""

import os
from perfkitbenchmarker import flags

flags.DEFINE_string('nova_path',
                    'nova',
                    'The path for the rackspace-novaclient tool.')

flags.DEFINE_string('cinder_path',
                    'cinder',
                    'The path for the python-cinderclient tool.')

flags.DEFINE_string('neutron_path',
                    'neutron',
                    'The path for the rackspace-neutronclient tool.')


flags.DEFINE_list('additional_rackspace_flags',
                  [],
                  'Additional flags to pass to Rackspace.')

FLAGS = flags.FLAGS


def GetDefaultRackspaceNovaFlags(resource):
    """Return common set of flags for using Nova on the Rackspace Cloud.

    Args:
    resource: A Rackspace resource of type BaseResource.

    Returns:
    A common set of Nova options.
    """
    options = ['--os-password', os.getenv('RS_API_KEY')]
    options.extend(FLAGS.additional_rackspace_flags)

    return options


def GetDefaultRackspaceCinderFlags(resource):
    """Return common set of flags for using Cinder on the Rackspace Cloud.

    Args:
    resource: A Rackspace resource of type BaseResource.

    Returns:
    A common set of Cinder options.
    """
    options = []
    options.extend(FLAGS.additional_rackspace_flags)

    return options


def GetDefaultRackspaceNeutronFlags(resource):
  """Return common set of flags for using Neutron on the Rackspace Cloud.

  Args:
    resource: A Rackspace resource of type BaseResource.

  Returns:
    A common set of Neutron options.
  """
  options = []
  options.extend(['--os-password', os.getenv('RS_API_KEY')])
  options.extend(FLAGS.additional_rackspace_flags)

  return options
