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
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class EksCluster(container_service.KubernetesCluster):
  """Class representing an Elastic Kubernetes Service cluster."""

  CLOUD = provider_info.AWS

  def __init__(self, spec):
    # EKS requires a region and optionally a list of one or zones.
    # Interpret the zone as a comma separated list of zones or a region.
    self.control_plane_zones = (
        spec.vm_spec.zone and spec.vm_spec.zone.split(','))
    # Do this before super, because commas in zones confuse EC2 virtual machines
    if len(self.control_plane_zones) > 1:
      # This will become self.zone
      spec.vm_spec.zone = self.control_plane_zones[0]
    super().__init__(spec)
    if not self.control_plane_zones:
      raise errors.Config.MissingOption(
          'container_cluster.vm_spec.AWS.zone is required.')
    elif len(self.control_plane_zones) == 1 and util.IsRegion(self.zone):
      self.region = self.zone
      self.control_plane_zones = []
      logging.info("Interpreting zone '%s' as a region", self.zone)
    else:
      self.region = util.GetRegionFromZones(self.control_plane_zones)
    # control_plane_zones must be a superset of the node zones
    for nodepool in self.nodepools.values():
      if (nodepool.zone and
          nodepool.zone not in self.control_plane_zones):
        self.control_plane_zones.append(nodepool.zone)
    if len(self.control_plane_zones) == 1:
      # eksctl essentially requires you pass --zones if you pass --node-zones
      # and --zones must have at least 2 zones
      # https://github.com/weaveworks/eksctl/issues/4735
      self.control_plane_zones.append(self.region +
                                      ('b' if self.zone.endswith('a') else 'a'))
    self.cluster_version = FLAGS.container_cluster_version
    # TODO(user) support setting boot disk type if EKS does.
    self.account = util.GetAccount()

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    nodepool_config.disk_type = vm_config.DEFAULT_ROOT_DISK_TYPE
    nodepool_config.disk_size = vm_config.boot_disk_size

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super(EksCluster, self).GetResourceMetadata()
    result['boot_disk_type'] = self.default_nodepool.disk_type
    result['boot_disk_size'] = self.default_nodepool.disk_size
    return result

  def _CreateDependencies(self):
    """Set up the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _DeleteDependencies(self):
    """Delete the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _Create(self):
    """Creates the control plane and worker nodes."""
    eksctl_flags = {
        'kubeconfig': FLAGS.kubeconfig,
        'managed': True,
        'name': self.name,
        'nodegroup-name': container_service.DEFAULT_NODEPOOL,
        'version': self.cluster_version,
        # NAT mode uses an EIP.
        'vpc-nat-mode': 'Disable',
        'with-oidc': True,
    }
    # If multiple zones are passed use them for the control plane.
    # Otherwise EKS will auto-select control plane zones in the region.
    eksctl_flags['zones'] = ','.join(self.control_plane_zones)
    if self.min_nodes != self.max_nodes:
      eksctl_flags.update({
          'nodes-min': self.min_nodes,
          'nodes-max': self.max_nodes,
      })
    eksctl_flags.update(
        self._GetNodeFlags(self.default_nodepool))

    cmd = [FLAGS.eksctl, 'create', 'cluster'] + sorted(
        '--{}={}'.format(k, v) for k, v in eksctl_flags.items() if v)
    stdout, _, retcode = vm_util.IssueCommand(
        cmd, timeout=1800, raise_on_failure=False)
    if retcode:
      # TODO(pclay): add other quota errors
      if 'The maximum number of VPCs has been reached' in stdout:
        raise errors.Benchmarks.QuotaFailure(stdout)
      else:
        raise errors.Resource.CreationError(stdout)

    for _, node_group in self.nodepools.items():
      self._CreateNodeGroup(node_group)

    # EBS CSI driver is required for creating EBS volumes in version > 1.23
    # https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html

    # Name must be unique.
    ebs_csi_driver_role = f'AmazonEKS_EBS_CSI_DriverRole_{self.name}'

    cmd = [
        FLAGS.eksctl, 'create', 'iamserviceaccount',
        '--name=ebs-csi-controller-sa',
        '--namespace=kube-system',
        f'--region={self.region}',
        f'--cluster={self.name}',
        '--attach-policy-arn=arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy',
        '--approve',
        '--role-only',
        f'--role-name={ebs_csi_driver_role}',
    ]
    vm_util.IssueCommand(cmd)

    cmd = [
        FLAGS.eksctl, 'create', 'addon',
        '--name=aws-ebs-csi-driver',
        f'--region={self.region}',
        f'--cluster={self.name}',
        f'--service-account-role-arn=arn:aws:iam::{self.account}:role/{ebs_csi_driver_role}',
    ]
    vm_util.IssueCommand(cmd)

  def _CreateNodeGroup(
      self, nodepool_config: container_service.BaseNodePoolConfig
  ):
    """Creates a node group."""
    eksctl_flags = {
        'cluster': self.name,
        'name': nodepool_config.name,
        # Support ARM: https://github.com/weaveworks/eksctl/issues/3569
        'skip-outdated-addons-check': True,
    }
    eksctl_flags.update(self._GetNodeFlags(nodepool_config))
    cmd = [FLAGS.eksctl, 'create', 'nodegroup'] + sorted(
        '--{}={}'.format(k, v) for k, v in eksctl_flags.items() if v
    )
    vm_util.IssueCommand(cmd, timeout=600)

  def _GetNodeFlags(
      self, nodepool_config: container_service.BaseNodePoolConfig
  ) -> Dict[str, Any]:
    """Get common flags for creating clusters and node_groups."""
    tags = util.MakeDefaultTags()
    return {
        'nodes':
            nodepool_config.num_nodes,
        'node-labels':
            f'pkb_nodepool={nodepool_config.name}',
        'node-type':
            nodepool_config.machine_type,
        'node-volume-size':
            nodepool_config.disk_size,
        # zone may be split a comma separated list
        'node-zones':
            nodepool_config.zone,
        'region':
            self.region,
        'tags':
            ','.join(f'{k}={v}' for k, v in tags.items()),
        'ssh-public-key':
            aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun(),
    }

  def _Delete(self):
    """Deletes the control plane and worker nodes."""
    super()._Delete()
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

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    # https://docs.aws.amazon.com/eks/latest/userguide/storage-classes.html
    return aws_disk.GP2
