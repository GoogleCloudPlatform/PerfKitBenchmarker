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
from perfkitbenchmarker.providers.aws import flags as aws_flags
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class BaseEksCluster(container_service.KubernetesCluster):
  """Shared base class for Elastic Kubernetes Service cluster auto mode & not."""

  def __init__(self, spec):
    # EKS requires a region and optionally a list of one or zones.
    # Interpret the zone as a comma separated list of zones or a region.
    self.control_plane_zones: list[str] = (
        spec.vm_spec.zone and spec.vm_spec.zone.split(',')
    )
    # Do this before super, because commas in zones confuse EC2 virtual machines
    if len(self.control_plane_zones) > 1:
      # This will become self.zone
      spec.vm_spec.zone = self.control_plane_zones[0]
    super().__init__(spec)
    if not self.control_plane_zones:
      raise errors.Config.MissingOption(
          'container_cluster.vm_spec.AWS.zone is required.'
      )
    self.region: str | None = None
    if len(self.control_plane_zones) == 1 and util.IsRegion(self.zone):
      self.region = self.zone
      self.control_plane_zones = []
      logging.info("Interpreting zone '%s' as a region", self.zone)
    else:
      self.region = util.GetRegionFromZones(self.control_plane_zones)
    self.cluster_version: str = FLAGS.container_cluster_version
    self.account: str = util.GetAccount()

  def _ChooseSecondZone(self):
    """Choose a second zone for the control plane if only one is specified."""
    if len(self.control_plane_zones) == 1:
      # eksctl essentially requires you pass --zones if you pass --node-zones
      # and --zones must have at least 2 zones
      # https://github.com/weaveworks/eksctl/issues/4735
      self.control_plane_zones.append(
          self.region + ('b' if self.zone.endswith('a') else 'a')
      )

  def _CreateDependencies(self):
    """Set up the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _DeleteDependencies(self):
    """Delete the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _EksCtlCreate(self, eksctl_flags: dict[str, Any]):
    """Creates the EKS cluster."""
    # If multiple zones are passed use them for the control plane.
    # Otherwise EKS will auto-select control plane zones in the region.
    if self.control_plane_zones:
      eksctl_flags['zones'] = ','.join(self.control_plane_zones)

    cmd = [FLAGS.eksctl, 'create', 'cluster'] + sorted(
        '--{}={}'.format(k, v) for k, v in eksctl_flags.items() if v
    )
    stdout, _, retcode = vm_util.IssueCommand(
        cmd, timeout=1800, raise_on_failure=False
    )
    if retcode:
      if 'The maximum number of VPCs has been reached' in stdout:
        raise errors.Benchmarks.QuotaFailure(stdout)
      else:
        raise errors.Resource.CreationError(stdout)

  def _Delete(self):
    """Deletes the control plane and worker nodes."""
    super()._Delete()
    cmd = [
        FLAGS.eksctl,
        'delete',
        'cluster',
        '--name',
        self.name,
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, timeout=1800)

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    return aws_disk.GP2


class EksCluster(BaseEksCluster):
  """Class representing an Elastic Kubernetes Service cluster."""

  CLOUD = provider_info.AWS

  def __init__(self, spec):
    super().__init__(spec)
    # control_plane_zones must be a superset of the node zones
    for nodepool in self.nodepools.values():
      if nodepool.zone and nodepool.zone not in self.control_plane_zones:
        self.control_plane_zones.append(nodepool.zone)
    self._ChooseSecondZone()

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    nodepool_config.disk_type = vm_config.DEFAULT_ROOT_DISK_TYPE  # pytype: disable=attribute-error
    nodepool_config.disk_size = vm_config.boot_disk_size  # pytype: disable=attribute-error

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super().GetResourceMetadata()
    result['boot_disk_type'] = self.default_nodepool.disk_type
    result['boot_disk_size'] = self.default_nodepool.disk_size
    return result

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
    if self.min_nodes != self.max_nodes:
      eksctl_flags.update({
          'nodes-min': self.min_nodes,
          'nodes-max': self.max_nodes,
      })
    eksctl_flags.update(self._GetNodeFlags(self.default_nodepool))

    self._EksCtlCreate(eksctl_flags)

    for _, node_group in self.nodepools.items():
      self._CreateNodeGroup(node_group)

    # EBS CSI driver is required for creating EBS volumes in version > 1.23
    # https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html

    # Name must be unique.
    ebs_csi_driver_role = f'AmazonEKS_EBS_CSI_DriverRole_{self.name}'

    cmd = [
        FLAGS.eksctl,
        'create',
        'iamserviceaccount',
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
        FLAGS.eksctl,
        'create',
        'addon',
        '--name=aws-ebs-csi-driver',
        f'--region={self.region}',
        f'--cluster={self.name}',
        f'--service-account-role-arn=arn:aws:iam::{self.account}:role/{ebs_csi_driver_role}',
    ]
    vm_util.IssueCommand(cmd)

    if aws_flags.AWS_EKS_POD_IDENTITY_ROLE.value:
      cmd = util.AWS_PREFIX + [
          'eks',
          'create-addon',
          '--addon-name=eks-pod-identity-agent',
          f'--region={self.region}',
          f'--cluster-name={self.name}',
      ]
      vm_util.IssueCommand(cmd)
      cmd = util.AWS_PREFIX + [
          'eks',
          'create-pod-identity-association',
          '--role-arn',
          (
              f'arn:aws:iam::{self.account}:role/'
              + aws_flags.AWS_EKS_POD_IDENTITY_ROLE.value
          ),
          '--namespace=default',
          '--service-account=default',
          f'--region={self.region}',
          f'--cluster-name={self.name}',
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
        'nodes': nodepool_config.num_nodes,
        'node-labels': f'pkb_nodepool={nodepool_config.name}',
        'node-type': nodepool_config.machine_type,
        'node-volume-size': nodepool_config.disk_size,
        # zone may be split a comma separated list
        'node-zones': nodepool_config.zone,
        'region': self.region,
        'tags': ','.join(f'{k}={v}' for k, v in tags.items()),
        'ssh-public-key': (
            aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun()
        ),
    }

  def _IsReady(self):
    """Returns True if the workers are ready, else False."""
    get_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'get',
        'nodes',
    ]
    stdout, _, _ = vm_util.IssueCommand(get_cmd)
    ready_nodes = len(re.findall('Ready', stdout))
    return ready_nodes >= self.min_nodes

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_service.DEFAULT_NODEPOOL
  ):
    """Change the number of nodes in the node group."""
    cmd = [
        FLAGS.eksctl,
        'scale',
        'nodegroup',
        node_pool,
        f'--nodes={new_size}',
        f'--nodes-min={new_size}',
        f'--nodes-max={new_size}',
        f'--cluster={self.name}',
        f'--region={self.region}',
        '--wait',
    ]
    vm_util.IssueCommand(cmd)


class EksAutoCluster(BaseEksCluster):
  """Class representing an Elastic Kubernetes Service cluster with auto mode.

  Automode supports auto scaling & ignores the concept of nodepools & selecting
  machine types. It also automatically creates some related resources like a
  load balancer & networks.
  """

  CLOUD = provider_info.AWS
  CLUSTER_TYPE = 'Autopilot'

  def __init__(self, spec):
    super().__init__(spec)
    self._ChooseSecondZone()

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    pass

  def _Create(self):
    """Creates the control plane and worker nodes."""
    tags = util.MakeDefaultTags()
    eksctl_flags = {
        'kubeconfig': FLAGS.kubeconfig,
        'name': self.name,
        'version': self.cluster_version,
        'with-oidc': True,
        'enable-auto-mode': True,
        'region': self.region,
        'tags': ','.join(f'{k}={v}' for k, v in tags.items()),
    }
    self._EksCtlCreate(eksctl_flags)

    # Enable public and private access to the cluster.
    vpc_cmd = [
        FLAGS.eksctl,
        'utils',
        'update-cluster-vpc-config',
        f'--cluster={self.name}',
        f'--region={self.region}',
        '--private-access=true',
        '--public-access=true',
        '--approve',
    ]
    vm_util.IssueCommand(vpc_cmd, timeout=900)

  def _Delete(self):
    """Deletes the control plane and worker nodes."""
    super()._Delete()
    cmd = [
        FLAGS.eksctl,
        'delete',
        'cluster',
        '--name',
        self.name,
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, timeout=1800)

  def _IsReady(self):
    """Returns True if cluster is running. Autopilot defaults to 0 nodes."""
    stdout, _, _ = container_service.RunKubectlCommand(['cluster-info'])
    # These two strings are printed in sequence, but with ansi color code
    # escape characters in between.
    return 'Kubernetes control plane' in stdout and 'is running at' in stdout

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    return aws_disk.GP2

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_service.DEFAULT_NODEPOOL
  ):
    """Change the number of nodes in the node group."""
    # Autopilot does not support nodepools & manual resizes.
    pass

  def GetJinjaNodeSelector(self) -> str:
    """Get the node selector section of a yaml for the provider."""
    # Theoretically needed in mixed mode, but deployments fail without it:
    # https://docs.aws.amazon.com/eks/latest/userguide/associate-workload.html#_require_a_workload_is_deployed_to_eks_auto_mode_nodes
    return 'eks.amazonaws.com/compute-type: auto'
