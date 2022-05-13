# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

"""Class to represent an GCP Placement Group object.

GCP specific implementations of Placement Group.
https://cloud.google.com/compute/docs/instances/define-instance-placement
"""

import json
import logging

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import util as gcp_util

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'gce_availability_domain_count',
    1,
    'Number of fault domains to create for SPREAD placement group',
    lower_bound=1,
    upper_bound=8)


class GcePlacementGroupSpec(placement_group.BasePlacementGroupSpec):
  """Object containing the information needed to create an GcePlacementGroup.

  Attributes:
      project: GCE project, used in creating the resource policy URL.
      region: GCE region, used in creating the resource policy URL.
      num_vms: Number of VMs to put into the resource group.
  """

  CLOUD = providers.GCP

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(GcePlacementGroupSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'project': (option_decoders.StringDecoder, {'none_ok': False}),
        'num_vms': (option_decoders.IntDecoder, {'none_ok': False}),
        'placement_group_style': (option_decoders.EnumDecoder, {
            'valid_values': set([placement_group.PLACEMENT_GROUP_SUPERCLUSTER] +
                                list(placement_group.PLACEMENT_GROUP_OPTIONS)),
            'default': placement_group.PLACEMENT_GROUP_NONE,
        })
    })
    return result


class GcePlacementGroup(placement_group.BasePlacementGroup):
  """Object representing an GCE Placement Group."""

  CLOUD = providers.GCP

  def __init__(self, gce_placement_group_spec):
    """Init method for GcePlacementGroup.

    Args:
      gce_placement_group_spec: Object containing the
        information needed to create an GcePlacementGroup.
    """
    super(GcePlacementGroup, self).__init__(gce_placement_group_spec)
    self.project = gce_placement_group_spec.project
    self.region = gcp_util.GetRegionFromZone(gce_placement_group_spec.zone)
    self.zone = None
    self.num_vms = gce_placement_group_spec.num_vms
    self.name = 'perfkit-{}'.format(context.GetThreadBenchmarkSpec().uuid)
    self.style = gce_placement_group_spec.placement_group_style
    self.availability_domain_count = FLAGS.gce_availability_domain_count
    self.metadata.update({
        'placement_group_name': self.name,
        'placement_group_style': self.style
    })

  def _Create(self):
    """Creates the GCE placement group."""

    cmd = gcp_util.GcloudCommand(self, 'compute', 'resource-policies',
                                 'create', 'group-placement', self.name)

    placement_policy = {
        'format': 'json',
        'region': self.region,
    }

    if self.style == placement_group.PLACEMENT_GROUP_CLUSTER:
      placement_policy['collocation'] = 'COLLOCATED'
      placement_policy['vm-count'] = self.num_vms

    elif self.style == placement_group.PLACEMENT_GROUP_SUPERCLUSTER:
      placement_policy['collocation'] = 'CLUSTERED'
      placement_policy['vm-count'] = self.num_vms
      # Only alpha API supported for CLUSTERED.
      cmd = gcp_util.GcloudCommand(self, 'alpha', 'compute',
                                   'resource-policies', 'create',
                                   'group-placement', self.name)

    else:
      placement_policy[
          'availability-domain-count'] = self.availability_domain_count

    cmd.flags.update(placement_policy)

    _, stderr, retcode = cmd.Issue(raise_on_failure=False)

    if retcode and "Quota 'RESOURCE_POLICIES' exceeded" in stderr:
      raise errors.Benchmarks.QuotaFailure(stderr)
    elif retcode:
      raise errors.Resource.CreationError(
          'Failed to create placement group: %s return code: %s' %
          (stderr, retcode))

  def _Exists(self):
    """See base class."""
    cmd = gcp_util.GcloudCommand(self, 'compute', 'resource-policies',
                                 'describe', self.name)
    cmd.flags.update({'region': self.region, 'format': 'json'})
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode:
      return False
    status = json.loads(stdout)['status']
    logging.info('Status of placement group %s: %s', self.name, status)
    return True

  def _Delete(self):
    """See base class."""
    logging.info('Deleting placement group %s', self.name)
    cmd = gcp_util.GcloudCommand(self, 'compute', 'resource-policies',
                                 'delete', self.name)
    cmd.flags.update({'region': self.region, 'format': 'json'})
    cmd.Issue(raise_on_failure=False)
