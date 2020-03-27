# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains classes/functions related to EKS (Elastic Kubernetes Service).

This requires that the eksServiceRole IAM role has already been created and
requires that the aws-iam-authenticator binary has been installed.
See https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html for
instructions.
"""

import logging
import re
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class EksCluster(container_service.KubernetesCluster):
  """Class representing an Elastic Kubernetes Service cluster."""

  CLOUD = providers.AWS

  def __init__(self, spec):
    super(EksCluster, self).__init__(spec)
    # EKS requires a region and optionally a list of zones.
    # Interpret the zone as a comma separated list of zones or a region.
    self.zones = sorted(FLAGS.eks_zones) or (self.zone and self.zone.split(','))
    if not self.zones:
      raise errors.Config.MissingOption(
          'container_cluster.vm_spec.AWS.zone is required.')
    elif len(self.zones) > 1:
      self.region = util.GetRegionFromZone(self.zones[0])
      self.zone = ','.join(self.zones)
    elif util.IsRegion(self.zones[0]):
      self.region = self.zone = self.zones[0]
      self.zones = []
      logging.info("Interpreting zone '%s' as a region", self.zone)
    else:
      raise errors.Config.InvalidValue(
          'container_cluster.vm_spec.AWS.zone must either be a comma separated '
          'list of zones or a region.')
    self.cluster_version = FLAGS.container_cluster_version

  def _CreateDependencies(self):
    """Set up the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _DeleteDependencies(self):
    """Delete the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _Create(self):
    """Creates the control plane and worker nodes."""
    tags = util.MakeDefaultTags()
    eksctl_flags = {
        'kubeconfig': FLAGS.kubeconfig,
        'managed': True,
        'name': self.name,
        'nodegroup-name': 'eks',
        'nodes': self.num_nodes,
        'nodes-min': self.min_nodes,
        'nodes-max': self.max_nodes,
        'node-type': self.machine_type,
        'region': self.region,
        'tags': ' '.join('{}={}'.format(k, v) for k, v in tags.items()),
        'ssh-public-key':
            aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun(),
        'version': self.cluster_version,
        # NAT mode uses an EIP.
        'vpc-nat-mode': 'Disable',
        'zones': ','.join(self.zones),
    }
    cmd = [FLAGS.eksctl, 'create', 'cluster'] + sorted(
        '--{}={}'.format(k, v) for k, v in eksctl_flags.items() if v)
    vm_util.IssueCommand(cmd, timeout=1800)

  def _Delete(self):
    """Deletes the control plane and worker nodes."""
    cmd = [FLAGS.eksctl, 'delete', 'cluster',
           '--name', self.name,
           '--region', self.region]
    vm_util.IssueCommand(cmd, timeout=1800)

  def _IsReady(self):
    """Returns True if the workers are ready, else False."""
    get_cmd = [
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
        'get', 'nodes',
    ]
    stdout, _, _ = vm_util.IssueCommand(get_cmd)
    ready_nodes = len(re.findall('Ready', stdout))
    return ready_nodes >= self.min_nodes
