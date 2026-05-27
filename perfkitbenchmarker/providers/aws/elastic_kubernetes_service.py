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

from collections import abc
import copy
import json
import logging
import math
import re
import threading
from typing import Any
from urllib import parse

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import flags as aws_flags
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.resources.container_service import container
from perfkitbenchmarker.resources.container_service import container_cluster
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands

# Flag to skip EBS CSI driver setup during cluster creation.
# The kubernetes_management benchmark does not use persistent volumes, so
# EBS CSI setup (OIDC + IAM role + addon install) is unnecessary and adds
# ~3 minutes to every run. Set to True to skip it and save time.
# Defined before FLAGS = flags.FLAGS so it is registered at import time
# and visible to PKB's flag parser before --cloud/--container_cluster_type
# are resolved.
FLAGS = flags.FLAGS
# GPU types which practically require spot to get.
_RARE_GPU_TYPES = [
    virtual_machine_spec.GPU_H100,
    virtual_machine_spec.GPU_A100,
    virtual_machine_spec.GPU_B200,
]


def _recursively_update_dictionary(
    original: dict[str, Any], updates: dict[str, Any]
) -> dict[str, Any]:
  """Updates a nested dictionary.

  Overwrites values in the original dictionary with the values in the updates
  dictionary, but preserves nested dictionaries even if both have a value.

  Args:
    original: The original dictionary to update.
    updates: The dictionary of updates to apply to the original dictionary.

  Returns:
    The updated dictionary, with keys + values from both.
  """
  # Copied from https://stackoverflow.com/questions/3232943
  for k, v in updates.items():
    if isinstance(v, abc.Mapping):
      original[k] = _recursively_update_dictionary(original.get(k, {}), v)
    else:
      original[k] = v
  return original


class BaseEksCluster(kubernetes_cluster.KubernetesCluster):
  """Shared base class for EKS cluster (auto mode and standard)."""

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
    self.node_to_nodepool: dict[str, container.BaseNodePoolConfig | None] = {}
    self.node_to_machine_type: dict[str, str | None] = {}
    self._cached_subnets: list[str] | None = None
    self._cached_subnets_per_az: dict[str, str] | None = None
    self._cached_node_role_arn: str | None = None

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

  def _EksCtlCreate(self, create_json: dict[str, Any]):
    """Creates the EKS cluster."""
    # Pass all control_plane_zones to the cluster so eksctl creates VPC subnets
    # in every requested AZ. Without this, eksctl may only create subnets in 2
    # AZs even when 3 are requested, preventing round-robin nodegroup placement.
    # This is critical for distributing nodegroups across AZs to avoid per-AZ
    # EC2 capacity limits.
    # availabilityZones is already set in create_json by _CreateDependencies
    # via the EC2 AZ query (bypassing PKB zone flag truncation).
    # Log it here for visibility.
    if 'availabilityZones' in create_json:
      logging.info(
          '[EKS] Creating cluster with AZs: %s — '
          + 'eksctl will auto-assign CIDRs for all %d zones.',
          create_json['availabilityZones'],
          len(create_json['availabilityZones']),
      )
    # Schema for the cluster create command is here:
    # https://schema.eksctl.io/
    create_json = _recursively_update_dictionary(
        {
            'apiVersion': 'eksctl.io/v1alpha5',
            'kind': 'ClusterConfig',
            'metadata': {
                'name': self.name,
                'region': self.region,
                'version': self.cluster_version,
                'tags': util.MakeDefaultTags(),
            },
            'iam': {
                'withOidc': True,
            },
        },
        create_json,
    )
    filename = self._WriteJsonToFile(create_json)
    cmd = [
        FLAGS.eksctl,
        'create',
        'cluster',
        '-f',
        filename,
        f'--kubeconfig={FLAGS.kubeconfig}',
    ]
    stdout, _, retcode = vm_util.IssueCommand(
        cmd, timeout=1800, raise_on_failure=False
    )
    if retcode:
      if 'The maximum number of VPCs has been reached' in stdout:
        raise errors.Benchmarks.QuotaFailure(stdout)
      else:
        raise errors.Resource.CreationError(stdout)

  def _RenderNodeGroupJson(
      self, nodepool: container.BaseNodePoolConfig
  ) -> dict[str, Any]:
    """Constructs the node group json dictionary."""
    group_json = {
        'name': nodepool.name,
        'instanceType': nodepool.machine_type,
        'desiredCapacity': nodepool.num_nodes,
        'amiFamily': 'AmazonLinux2023',
        'tags': util.MakeDefaultTags(),
        'labels': {
            'pkb_nodepool': nodepool.name,
        },
    }
    if nodepool.min_nodes != nodepool.max_nodes:
      group_json['minSize'] = nodepool.min_nodes
      group_json['maxSize'] = nodepool.max_nodes
    # Pin the default nodegroup to control_plane_zones[0] so it stays in a
    # single known AZ. The benchmark nodegroups (pkbma*, pkbmc*) are placed
    # via CreateNodePoolAsync using the round-robin _DiscoverSubnetsPerAZ logic.
    if self.control_plane_zones:
      group_json['availabilityZones'] = [self.control_plane_zones[0]]
    return group_json

  def _WriteJsonToFile(self, json_dict: dict[str, Any]) -> str:
    """Renders the given json dict to a file.

    Args:
      json_dict: The json dict to render.

    Returns:
      The file path of the rendered json.
    """
    with vm_util.NamedTemporaryFile(
        dir=vm_util.GetTempDir(), delete=False, mode='w'
    ) as tf:
      rendered_json = json.dumps(json_dict, indent=2)
      logging.info(
          'Writing to %s rendered eksctl create json: %s',
          tf.name,
          rendered_json,
      )
      tf.write(rendered_json)
      tf.close()
      return tf.name

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

  def GetNodePoolFromNodeName(
      self, node_name: str
  ) -> container.BaseNodePoolConfig | None:
    """Gets the nodepool from the node name.

    This method assumes that the nodepool name is embedded in the node name.
    Better would be a lookup from the cloud provider.

    Args:
      node_name: The name of the node.

    Returns:
      The associated nodepool, or None if not found.
    """
    if node_name in self.node_to_nodepool:
      return self.node_to_nodepool[node_name]
    nodepool_name, err, code = kubectl.RunKubectlCommand(
        [
            'get',
            'node',
            node_name,
            '-o',
            'jsonpath="{.metadata.labels.pkb_nodepool}"',
        ],
        raise_on_failure=False,
    )
    if code:
      logging.warning(
          'Got error when trying to get nodepool name for node %s: %s',
          err,
          node_name,
      )
      nodepool = None
    else:
      nodepool_name = nodepool_name.strip().strip('"')
      if nodepool_name == 'default':
        nodepool = self.default_nodepool
      else:
        if nodepool_name not in self.nodepools:
          logging.warning(
              'Nodepool %s not found in nodepools %s',
              nodepool_name,
              self.nodepools,
          )
          nodepool = None
        else:
          nodepool = self.nodepools[nodepool_name]
    self.node_to_nodepool[node_name] = nodepool
    return nodepool

  def GetMachineTypeFromNodeName(self, node_name: str) -> str | None:
    """Gets the machine type from the node name."""
    if node_name in self.node_to_machine_type:
      return self.node_to_machine_type[node_name]
    out, _, _ = kubectl.RunKubectlCommand(
        [
            'get',
            'nodes',
            '-o',
            (
                "jsonpath='{range"
                r' .items[*]}{.metadata.name},{.metadata.labels.beta\.'
                r'kubernetes\.io/instance-type}{"\n"}{end}\''
            ),
        ],
    )
    for line in out.splitlines():
      pieces = line.split(',')
      if not pieces or len(pieces) != 2:
        continue
      node, machine_type = pieces
      node = node.strip("'")
      machine_type = machine_type.strip("'")
      self.node_to_machine_type[node] = machine_type
    if node_name not in self.node_to_machine_type:
      self.node_to_machine_type[node_name] = None
    return self.node_to_machine_type[node_name]

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    return aws_disk.GP2

  @property
  def _ingress_manifest_path(self) -> str:
    """The path to the ingress manifest template file."""
    return 'container/ingress.yaml.j2'

  def _WaitForIngress(self, name: str, namespace: str, port: int) -> str:
    """Waits for an Ingress resource to be deployed to the cluster."""
    del port
    kubernetes_commands.WaitForResource(
        'ingress',
        kubernetes_cluster.INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
        extra_args=[name],
    )
    stdout, _, _ = kubectl.RunKubectlCommand([
        'get',
        'ingress',
        name,
        '-n',
        namespace,
        '-o',
        f'jsonpath={kubernetes_cluster.INGRESS_JSONPATH}',
    ])
    return self._GetAddressFromIngress(stdout)

  def GetNodePoolNames(self) -> list[str]:
    """Get node pool names for the cluster."""

    cmd = [
        FLAGS.eksctl,
        'get',
        'nodegroup',
        '--cluster',
        self.name,
        '--region',
        self.region,
        '-o',
        'json',
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode:
      logging.warning('Failed to get nodegroups: %s, error: %s', stdout, stderr)
      return []
    nodegroups = json.loads(stdout)
    return [ng['Name'] for ng in nodegroups]

  def AddNodepool(self, batch_name, pool_id):
    pass


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
      vm_config: virtual_machine_spec.BaseVmSpec,
      nodepool_config: container.BaseNodePoolConfig,
  ):
    super().InitializeNodePoolForCloud(vm_config, nodepool_config)
    nodepool_config.disk_type = (
        aws_virtual_machine.AwsVirtualMachine.DEFAULT_ROOT_DISK_TYPE
    )

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
    nodepool_jsons = [self._RenderNodeGroupJson(self.default_nodepool)]
    for _, node_group in self.nodepools.items():
      nodepool_jsons += [self._RenderNodeGroupJson(node_group)]
    create_json: dict[str, Any] = {
        'managedNodeGroups': nodepool_jsons,
        'vpc': {'nat': {'gateway': 'Disable'}},
    }
    # Explicitly set cluster-level availabilityZones so eksctl creates VPC
    # public+private subnets in ALL AZs in the region.
    # IMPORTANT: PKB's deprecated --zones flag gets truncated by its own
    # translation layer to 2 AZs even when 3 are specified. We bypass this
    # by querying EC2 directly for all available AZs in the region and
    # passing all of them to eksctl. This ensures the VPC gets subnets in
    # all AZs, enabling proper round-robin nodegroup placement.
    try:
      az_out, _, az_rc = vm_util.IssueCommand(
          util.AWS_PREFIX + [
              'ec2', 'describe-availability-zones',
              '--region', self.region,
              '--filters', 'Name=state,Values=available',
              '--query', 'AvailabilityZones[*].ZoneName',
              '--output', 'json',
          ],
          raise_on_failure=False,
      )
      if az_rc == 0 and az_out.strip():
        all_azs = json.loads(az_out.strip())
        # Limit to 3 AZs maximum to avoid excessive subnet creation
        cluster_azs = sorted(all_azs)[:3]
      else:
        # Fallback: use control_plane_zones or default to known us-east-1 AZs
        cluster_azs = (
            self.control_plane_zones
            if self.control_plane_zones
            else [f'{self.region}a', f'{self.region}b', f'{self.region}c']
        )
    except Exception:  # pylint: disable=broad-except
      cluster_azs = (
          self.control_plane_zones
          if self.control_plane_zones
          else [f'{self.region}a', f'{self.region}b', f'{self.region}c']
      )

    create_json['availabilityZones'] = cluster_azs
    logging.info(
        '[EKS] Cluster will have subnets in %d AZs: %s '
        + '(queried from EC2, bypassing PKB zone flag truncation)',
        len(cluster_azs), cluster_azs,
    )
    self._EksCtlCreate(create_json)

    # Above create command passes "withOidc=true", but it doesn't seem to work &
    # therefore this command is needed.
    if not FLAGS.eks_skip_ebs_csi:
      cmd = [
          FLAGS.eksctl,
          'utils',
          'associate-iam-oidc-provider',
          f'--cluster={self.name}',
          f'--region={self.region}',
          '--approve',
      ]
      vm_util.IssueCommand(cmd)

    # EBS CSI driver is required for creating EBS volumes in version > 1.23
    # https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html
    # Skip if --eks_skip_ebs_csi is set (saves ~3 min for benchmarks that
    # do not use persistent volumes, such as kubernetes_management).
    if FLAGS.eks_skip_ebs_csi:
      logging.info(
          '[EKS] Skipping EBS CSI driver setup (--eks_skip_ebs_csi=True). '
          + 'Saves ~3 min. Set to False if benchmark needs persistent volumes.'
      )
    else:
      # Name must be unique.
      ebs_csi_driver_role = f'AmazonEKS_EBS_CSI_DriverRole_{self.name}'

      ebs_policy_arn = (
          'arn:aws:iam::aws:policy/service-role/'
          + 'AmazonEBSCSIDriverPolicy')
      cmd = [
          FLAGS.eksctl,
          'create',
          'iamserviceaccount',
          '--name=ebs-csi-controller-sa',
          '--namespace=kube-system',
          f'--region={self.region}',
          f'--cluster={self.name}',
          f'--attach-policy-arn={ebs_policy_arn}',
          '--approve',
          '--role-only',
          f'--role-name={ebs_csi_driver_role}',
      ]
      vm_util.IssueCommand(cmd)

      svc_acct_arn = (
          f'arn:aws:iam::{self.account}:role/{ebs_csi_driver_role}')
      cmd = [
          FLAGS.eksctl,
          'create',
          'addon',
          '--name=aws-ebs-csi-driver',
          f'--region={self.region}',
          f'--cluster={self.name}',
          f'--service-account-role-arn={svc_acct_arn}',
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

  def _RenderNodeGroupJson(
      self, nodepool: container.BaseNodePoolConfig
  ) -> dict[str, Any]:
    """Constructs the node group json dictionary."""
    base_json = super()._RenderNodeGroupJson(nodepool)
    if nodepool.disk_size:
      base_json['volumeSize'] = nodepool.disk_size
    base_json.update({
        'ssh': {
            'allow': True,
            'publicKeyPath': (
                aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun()
            ),
        },
    })
    if self.control_plane_zones:
      # zones can be a comma separated list or simply a region
      if isinstance(nodepool.zone, str):
        zones = [nodepool.zone]
      else:
        zones = nodepool.zone
      base_json['availabilityZones'] = zones
    return base_json

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
      self, new_size: int, node_pool: str = container_cluster.DEFAULT_NODEPOOL
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

  def CreateNodePool(
      self,
      nodepool_config: container.BaseNodePoolConfig,
      node_version: str | None = None,
  ) -> None:
    """Creates a single managed node group on the cluster."""
    ng_json = self._RenderNodeGroupJson(nodepool_config)
    if node_version:
      ng_json['version'] = node_version
    config_json = {
        'apiVersion': 'eksctl.io/v1alpha5',
        'kind': 'ClusterConfig',
        'metadata': {
            'name': self.name,
            'region': self.region,
        },
        'managedNodeGroups': [ng_json],
    }
    filename = self._WriteJsonToFile(config_json)
    cmd = [
        FLAGS.eksctl,
        'create',
        'nodegroup',
        f'--config-file={filename}',
    ]
    _, stderr, retcode = vm_util.IssueCommand(
        cmd, timeout=1800, raise_on_failure=False
    )
    if retcode:
      raise errors.Resource.CreationError(stderr)

  def DeleteNodePool(self, name: str) -> None:
    """Deletes the named node group."""
    cmd = [
        FLAGS.eksctl,
        'delete',
        'nodegroup',
        f'--name={name}',
        f'--cluster={self.name}',
        f'--region={self.region}',
        '--wait',
    ]
    vm_util.IssueCommand(cmd, timeout=1800)

  def UpgradeNodePool(self, name: str, target_version: str) -> None:
    """Upgrades the named node group to target_version."""
    cmd = [
        FLAGS.eksctl,
        'upgrade',
        'nodegroup',
        f'--name={name}',
        f'--cluster={self.name}',
        f'--region={self.region}',
        f'--kubernetes-version={target_version}',
        '--wait',
    ]
    vm_util.IssueCommand(cmd, timeout=1800)

  # ---- Async variants (return opaque handles) -------------------------------

  def _DiscoverSubnets(self) -> list[str]:
    """Returns the EKS cluster's subnet IDs (cached after first call)."""
    if getattr(self, '_cached_subnets', None):
      return self._cached_subnets
    out, _, _ = vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'eks',
            'describe-cluster',
            '--name',
            self.name,
            '--region',
            self.region,
        ]
    )
    info = json.loads(out)
    self._cached_subnets = info['cluster']['resourcesVpcConfig']['subnetIds']
    return self._cached_subnets

  def _DiscoverSubnetsPerAZ(self) -> dict[str, str]:
    """Returns a mapping of {AZ: subnet_id} for the cluster's subnets.

    Used by CreateNodePoolAsync to distribute nodegroups round-robin across
    AZs, avoiding per-AZ EC2 capacity limits when creating many pools.
    Only returns AZs that are in control_plane_zones (if specified).
    Cached after first call.
    """
    if getattr(self, '_cached_subnets_per_az', None) is not None:
      return self._cached_subnets_per_az

    subnet_ids = self._DiscoverSubnets()
    if not subnet_ids:
      self._cached_subnets_per_az = {}
      return {}

    # Describe subnets to get their AZ mapping
    out, _, rc = vm_util.IssueCommand(
        util.AWS_PREFIX + [
            'ec2', 'describe-subnets',
            '--region', self.region,
            '--subnet-ids', *subnet_ids,
            '--query', 'Subnets[*].{SubnetId:SubnetId,AZ:AvailabilityZone}',
            '--output', 'json',
        ],
        raise_on_failure=False,
    )
    if rc:
      logging.warning(
          '[EKS] Could not describe subnets for AZ mapping — '
          + 'falling back to all subnets'
      )
      self._cached_subnets_per_az = {}
      return {}

    subnets = json.loads(out)

    # Do NOT filter by control_plane_zones — PKB truncates it to 2 AZs.
    # Accept all subnets the VPC has across all AZs.
    az_map: dict[str, str] = {}
    for s in subnets:
      az = s['AZ']
      # Keep only one subnet per AZ (prefer public subnets — already filtered
      # by _DiscoverSubnets which returns the cluster's configured subnets)
      if az not in az_map:
        az_map[az] = s['SubnetId']

    logging.info(
        '[EKS] Subnet-per-AZ mapping: %s (from %d total subnets)',
        az_map, len(subnet_ids),
    )
    self._cached_subnets_per_az = az_map
    return az_map

  def _ResolveReleaseVersion(self, minor: str) -> str:
    """Returns the EKS-optimized AMI release version (e.g. '1.33.10-20260124').

    Used to populate `releaseVersion` in the create-nodegroup payload so the
    benchmark can pin specific K8s minors. Thread-safe: at scale we have N
    workers all asking for the same minor; only the first does the SSM
    lookup, the rest read from the cache.
    """
    if getattr(self, '_release_version_lock', None) is None:
      self._release_version_lock = threading.Lock()
    with self._release_version_lock:
      cache = getattr(self, '_cached_release_versions', None) or {}
      if minor in cache:
        return cache[minor]
      cmd = util.AWS_PREFIX + [
          'ssm',
          'get-parameter',
          '--name',
          (
              f'/aws/service/eks/optimized-ami/{minor}/amazon-linux-2023/'
              'x86_64/standard/recommended/release_version'
          ),
          '--region',
          self.region,
          '--query',
          'Parameter.Value',
          '--output',
          'text',
      ]
      out, err, rc = vm_util.IssueCommand(cmd, raise_on_failure=False)
      if rc:
        raise errors.Resource.CreationError(
            f'Failed to resolve EKS release version for minor {minor!r}: {err}'
        )
      cache[minor] = out.strip()
      self._cached_release_versions = cache
      return cache[minor]

  def _DiscoverNodeRoleArn(self) -> str:
    """Returns a node IAM role ARN by inspecting an existing nodegroup."""
    if getattr(self, '_cached_node_role_arn', None):
      return self._cached_node_role_arn
    out, _, _ = vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'eks',
            'list-nodegroups',
            '--cluster-name',
            self.name,
            '--region',
            self.region,
        ]
    )
    for ng_name in json.loads(out).get('nodegroups', []):
      ng_out, _, _ = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'eks',
              'describe-nodegroup',
              '--cluster-name',
              self.name,
              '--nodegroup-name',
              ng_name,
              '--region',
              self.region,
          ]
      )
      role = json.loads(ng_out)['nodegroup'].get('nodeRole')
      if role:
        self._cached_node_role_arn = role
        return role
    raise errors.Resource.CreationError(
        f'No existing nodegroup found to discover node role for '
        f'cluster {self.name}.'
    )

  def CreateNodePoolAsync(
      self,
      nodepool_config: container.BaseNodePoolConfig,
      node_version: str | None = None,
  ) -> str:
    # Pass the full request via --cli-input-json so that we can specify both
    # `version` (e.g. "1.33") and `releaseVersion` (e.g. "1.33.11-...") in
    # the same call. Two reasons this matters:
    #   1. AWS CLI v1 has a bug where the top-level --version flag swallows
    #      the subcommand --version, printing the CLI banner and exiting.
    #      cli-input-json sidesteps CLI argument parsing entirely.
    #   2. EKS rejects a releaseVersion that doesn't match the request's
    #      `version`; if `version` is omitted EKS defaults it to the
    #      cluster's version, which (for the N-1 -> N benchmark path)
    #      produces a "release version X is not valid for kubernetes
    #      version Y" error.

    # ── AZ distribution ────────────────────────────────────────────────────
    # When multiple zones are specified (e.g. us-east-1a,1b,1c), distribute
    # nodegroups round-robin across AZs to avoid per-AZ EC2 capacity limits.
    # Without this, EKS places all nodegroups in a single AZ causing timeouts.
    # Pool name format: pkbma000, pkbma001, ... — extract index from suffix.
    az_subnets = self._DiscoverSubnetsPerAZ()
    if az_subnets and len(az_subnets) > 1:
      # Extract numeric suffix from pool name to determine AZ assignment
      name = nodepool_config.name
      suffix = ''.join(c for c in name if c.isdigit())
      idx = int(suffix) if suffix else 0
      zones = sorted(az_subnets.keys())
      assigned_az = zones[idx % len(zones)]
      subnets = [az_subnets[assigned_az]]
      logging.info(
          '[EKS] CreateNodePool %s -> AZ=%s subnet=%s (round-robin idx=%d)',
          name, assigned_az, subnets[0], idx,
      )
    else:
      subnets = self._DiscoverSubnets()
      logging.info('[EKS] CreateNodePool %s -> using all subnets (single AZ)',
                   nodepool_config.name)

    payload: dict[str, Any] = {
        'clusterName': self.name,
        'nodegroupName': nodepool_config.name,
        'scalingConfig': {
            'minSize': nodepool_config.num_nodes,
            'maxSize': nodepool_config.num_nodes,
            'desiredSize': nodepool_config.num_nodes,
        },
        'subnets': subnets,
        'instanceTypes': [nodepool_config.machine_type],
        'amiType': 'AL2023_x86_64_STANDARD',
        'nodeRole': self._DiscoverNodeRoleArn(),
        'labels': {'pkb_nodepool': nodepool_config.name},
        'tags': util.MakeDefaultTags(),
        # Target open capacity reservations first before falling back to
        # regular on-demand. Ensures EC2 capacity reservations created
        # before the benchmark are actually used by EKS nodegroups.
        'capacityReservationSpecification': {
            'capacityReservationPreference': 'open',
        },
    }
    if node_version:
      payload['version'] = node_version
      payload['releaseVersion'] = self._ResolveReleaseVersion(node_version)
    filename = self._WriteJsonToFile(payload)
    cmd = util.AWS_PREFIX + [
        'eks',
        'create-nodegroup',
        '--region',
        self.region,
        '--cli-input-json',
        f'file://{filename}',
    ]
    _, stderr, retcode = vm_util.IssueCommand(
        cmd, timeout=300, raise_on_failure=False
    )
    if retcode:
      raise errors.Resource.CreationError(stderr)
    return f'ng_active:{nodepool_config.name}'

  def UpgradeNodePoolAsync(self, name: str, target_version: str) -> str:
    cmd = util.AWS_PREFIX + [
        'eks',
        'update-nodegroup-version',
        '--cluster-name',
        self.name,
        '--nodegroup-name',
        name,
        '--region',
        self.region,
        '--kubernetes-version',
        target_version,
    ]
    _, stderr, retcode = vm_util.IssueCommand(
        cmd, timeout=300, raise_on_failure=False
    )
    if retcode:
      raise errors.Resource.CreationError(stderr)
    return f'ng_active:{name}'

  def DeleteNodePoolAsync(self, name: str) -> str:
    cmd = util.AWS_PREFIX + [
        'eks',
        'delete-nodegroup',
        '--cluster-name',
        self.name,
        '--nodegroup-name',
        name,
        '--region',
        self.region,
    ]
    _, stderr, retcode = vm_util.IssueCommand(
        cmd, timeout=300, raise_on_failure=False
    )
    if retcode:
      raise errors.Resource.CreationError(stderr)
    return f'ng_gone:{name}'

  def UpdateClusterAsync(self) -> str:
    """Fires a CloudWatch logging toggle; returns handle 'cluster_update:<id>'.

    Returns a handle carrying the specific update id so WaitForOperation
    can poll *that* update's status (Successful / Failed) rather than the
    cluster's top-level status (which stays ACTIVE during config updates,
    making the wait return instantly and silently mis-reporting latency).
    """
    log_types = ['api', 'audit', 'authenticator', 'controllerManager',
                 'scheduler']
    describe = util.AWS_PREFIX + [
        'eks',
        'describe-cluster',
        '--name',
        self.name,
        '--region',
        self.region,
    ]
    out, _, _ = vm_util.IssueCommand(describe)
    current = (
        json.loads(out)['cluster'].get('logging', {}).get('clusterLogging', [])
    )
    any_enabled = any(e.get('enabled', False) for e in current)
    payload = json.dumps({
        'clusterLogging': [
            {'types': log_types, 'enabled': not any_enabled}
        ]
    })
    upd = util.AWS_PREFIX + [
        'eks',
        'update-cluster-config',
        '--name',
        self.name,
        '--region',
        self.region,
        '--logging',
        payload,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(
        upd, timeout=300, raise_on_failure=False
    )
    if retcode:
      raise errors.Resource.CreationError(stderr)
    update_id = json.loads(stdout)['update']['id']
    return f'cluster_update:{update_id}'

  def ResolveNodePoolVersions(self) -> tuple[str, str]:
    """Returns (initial, target) EKS nodegroup versions.

    Uses cluster_version (already set from FLAGS/describe-cluster) rather than
    querying kubectl, which is faster and avoids a kubectl round-trip.
    initial = N-1 (adjacent minor below cluster version)
    target  = N   (cluster version = latest)
    """
    cluster_ver = self.cluster_version or self.k8s_version
    # Strip any patch suffix e.g. '1.34.7' -> '1.34'
    parts = cluster_ver.lstrip('v').split('.')
    major, minor = int(parts[0]), int(parts[1])
    target  = f'{major}.{minor}'
    initial = f'{major}.{minor - 1}'
    logging.info(
        '[EKS] ResolveNodePoolVersions: cluster=%s initial=%s target=%s',
        cluster_ver, initial, target,
    )
    return initial, target

  def WaitForOperation(self, op_handle: str) -> None:
    """Polls EKS resources until the expected terminal state is observed."""
    kind, _, name = op_handle.partition(':')

    @vm_util.Retry(
        poll_interval=5,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _wait_ng_active():
      out, err, rc = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'eks',
              'describe-nodegroup',
              '--cluster-name',
              self.name,
              '--nodegroup-name',
              name,
              '--region',
              self.region,
          ],
          raise_on_failure=False,
      )
      if rc:
        raise errors.Resource.RetryableCreationError(err)
      status = json.loads(out)['nodegroup']['status']
      if status in ('ACTIVE',):
        return
      if status in ('CREATE_FAILED', 'DELETE_FAILED', 'DEGRADED'):
        raise errors.Resource.CreationError(
            f'nodegroup {name} ended in {status}'
        )
      raise errors.Resource.RetryableCreationError(
          f'nodegroup {name} status={status}'
      )

    @vm_util.Retry(
        poll_interval=5,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableDeletionError,),
    )
    def _wait_ng_gone():
      _, err, rc = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'eks',
              'describe-nodegroup',
              '--cluster-name',
              self.name,
              '--nodegroup-name',
              name,
              '--region',
              self.region,
          ],
          raise_on_failure=False,
      )
      if rc and 'ResourceNotFoundException' in (err or ''):
        return
      if rc:
        raise errors.Resource.RetryableDeletionError(err)
      raise errors.Resource.RetryableDeletionError(
          f'nodegroup {name} still present'
      )

    @vm_util.Retry(
        poll_interval=5,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _wait_cluster_update():
      out, err, rc = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'eks',
              'describe-update',
              '--name',
              self.name,
              '--update-id',
              name,
              '--region',
              self.region,
              '--query',
              'update.status',
              '--output',
              'text',
          ],
          raise_on_failure=False,
      )
      if rc:
        raise errors.Resource.RetryableCreationError(err)
      status = out.strip()
      if status == 'Successful':
        return
      if status in ('Failed', 'Cancelled'):
        raise errors.Resource.CreationError(
            f'cluster update {name} ended in {status}'
        )
      raise errors.Resource.RetryableCreationError(
          f'cluster update {name} status={status}'
      )

    if kind == 'ng_active':
      _wait_ng_active()
    elif kind == 'ng_gone':
      _wait_ng_gone()
    elif kind == 'cluster_update':
      _wait_cluster_update()
    else:
      raise ValueError(f'Unknown EKS op handle: {op_handle!r}')

  def UpdateCluster(self) -> None:
    """Real cluster-level update via a CloudWatch logging toggle.

    Reads the current cluster logging state, flips it (enable->disable or
    vice versa), and waits for the cluster to return to ACTIVE. Enabling all
    five log types is a 5-10 minute control-plane op, giving a meaningful
    overlap window for Scenario B.
    """
    log_types = ['api', 'audit', 'authenticator', 'controllerManager',
                 'scheduler']
    describe = util.AWS_PREFIX + [
        'eks', 'describe-cluster',
        '--name', self.name,
        '--region', self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(describe)
    info = json.loads(stdout)
    current = info['cluster'].get('logging', {}).get('clusterLogging', [])
    any_enabled = any(entry.get('enabled', False) for entry in current)
    new_enabled = not any_enabled
    logging_payload = json.dumps({
        'clusterLogging': [
            {'types': log_types, 'enabled': new_enabled}
        ]
    })
    update = util.AWS_PREFIX + [
        'eks', 'update-cluster-config',
        '--name', self.name,
        '--region', self.region,
        '--logging', logging_payload,
    ]
    vm_util.IssueCommand(update, timeout=900)

    @vm_util.Retry(
        poll_interval=5,
        fuzz=0,
        timeout=900,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _wait_active():
      query = util.AWS_PREFIX + [
          'eks', 'describe-cluster',
          '--name', self.name,
          '--region', self.region,
          '--query', 'cluster.status',
          '--output', 'text',
      ]
      out, _, _ = vm_util.IssueCommand(query)
      status = out.strip()
      if status != 'ACTIVE':
        raise errors.Resource.RetryableCreationError(
            f'cluster status={status}'
        )

    _wait_active()


class EksAutoCluster(BaseEksCluster):
  """Class representing an Elastic Kubernetes Service cluster with auto mode.

  Auto mode supports auto scaling & allows users to not specify nodepools nor
  select machine types if they so wish (but nodepools can still be used to
  specify these if desired). It also automatically creates some related
  resources like a load balancer & networks.
  """

  CLOUD = provider_info.AWS
  CLUSTER_TYPE = 'Auto'

  def __init__(self, spec):
    super().__init__(spec)
    self._ChooseSecondZone()
    is_rare_gpu = virtual_machine.GPU_TYPE.value in _RARE_GPU_TYPES
    self.use_spot: bool = aws_flags.USE_AWS_SPOT_INSTANCES.value or is_rare_gpu

  def _Create(self):
    """Creates the control plane and worker nodes."""
    self._EksCtlCreate({'autoModeConfig': {'enabled': True}})

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
    # Retry esp. "cluster currently has an update in progress" errors.
    vm_util.IssueRetryableCommand(vpc_cmd, timeout=900)

  def _PostCreate(self):
    """Performs post-creation steps for the cluster."""
    super()._PostCreate()
    if self.use_spot:
      kubernetes_commands.ApplyManifest(
          'container/auto/nodepool.yaml.j2',
          CLUSTER_NAME=self.name,
      )

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
    stdout, _, _ = kubectl.RunKubectlCommand(['cluster-info'])
    # These two strings are printed in sequence, but with ansi color code
    # escape characters in between.
    return 'Kubernetes control plane' in stdout and 'is running at' in stdout

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    return aws_disk.GP2

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_cluster.DEFAULT_NODEPOOL
  ):
    """Change the number of nodes in the node group."""
    # Autopilot does not support nodepools & manual resizes.
    pass

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Get the node selectors section of a yaml for the provider."""
    del machine_type  # Unused.
    # Theoretically needed in mixed mode, but deployments fail without it.
    # See: docs.aws.amazon.com/eks/latest/userguide/associate-workload.html
    # #_require_a_workload_is_deployed_to_eks_auto_mode_nodes
    selectors = {'eks.amazonaws.com/compute-type': 'auto'}
    if self.use_spot:
      selectors['karpenter.sh/capacity-type'] = 'spot'
    if virtual_machine.GPU_TYPE.value:
      selectors['eks.amazonaws.com/instance-gpu-name'] = (
          virtual_machine.GPU_TYPE.value
      )
    return selectors


_KARPENTER_NAMESPACE = 'kube-system'
_KARPENTER_VERSION = '1.8.1'
_DEFAULT_K8S_VERSION = '1.34'


class EksKarpenterCluster(BaseEksCluster):
  """Class representing an Elastic Kubernetes Service cluster with karpenter.

  Requires installation of helm: https://helm.sh/docs/intro/install/
  """

  CLOUD = provider_info.AWS
  CLUSTER_TYPE = 'Karpenter'

  def __init__(self, spec):
    super().__init__(spec)
    self._ChooseSecondZone()
    self.stack_name = f'Karpenter-{self.name}'
    self.cluster_version: str = self.cluster_version or _DEFAULT_K8S_VERSION
    # The AMI version for current kubernetes version.
    # See e.g. https://karpenter.sh/docs/tasks/managing-amis/ for not using
    # @latest.
    self.alias_version: str | None = None

  def _Create(self):
    """Creates the control plane and worker nodes."""
    template_filename = vm_util.PrependTempDir('cloud-formation-template.yaml')
    cfn_url = (
        'https://raw.githubusercontent.com/aws/karpenter-provider-aws/'
        + f'v{_KARPENTER_VERSION}/website/content/en/preview/'
        + 'getting-started/getting-started-with-karpenter/'
        + 'cloudformation.yaml')
    vm_util.IssueCommand([
        'curl',
        '-fsSL',
        cfn_url,
        '-o',
        template_filename,
    ])
    # key=value format differs from other service's Key=key,Value=value format
    formation_tags = [f'{k}={v}' for k, v in util.MakeDefaultTags().items()]
    vm_util.IssueCommand(
        [
            'aws',
            'cloudformation',
            'deploy',
            '--stack-name',
            self.stack_name,
            '--template-file',
            template_filename,
            '--capabilities',
            'CAPABILITY_NAMED_IAM',
            '--parameter-overrides',
            f'ClusterName={self.name}',
            '--region',
            f'{self.region}',
            '--tags',
        ]
        + formation_tags,
    )
    # The default control plane nodepool is only used for initial commands
    # to setup Karpenter. Karpenter will setup its own nodepools later.
    bootstrapping_nodepool = copy.copy(self.default_nodepool)
    bootstrapping_nodepool.num_nodes = 1
    bootstrapping_nodepool.min_nodes = 1
    bootstrapping_nodepool.max_nodes = 1
    bootstrapping_nodepool.machine_type = 'm7i.2xlarge'
    karpenter_policy_arn = (
        f'arn:aws:iam::{self.account}:policy/'
        + f'KarpenterControllerPolicy-{self.name}')
    karpenter_node_role_arn = (
        f'arn:aws:iam::{self.account}:role/'
        + f'KarpenterNodeRole-{self.name}')
    create_json: dict[str, Any] = {
        'metadata': {
            'tags': {'karpenter.sh/discovery': self.name},
        },
        'iam': {
            'podIdentityAssociations': [{
                'namespace': _KARPENTER_NAMESPACE,
                'serviceAccountName': 'karpenter',
                'roleName': f'{self.name}-karpenter',
                'permissionPolicyARNs': [
                    karpenter_policy_arn
                ],
            }],
        },
        'iamIdentityMappings': [{
            'arn': karpenter_node_role_arn,
            'username': 'system:node:{{EC2PrivateDNSName}}',
            'groups': ['system:bootstrappers', 'system:nodes'],
        }],
        'addons': [{'name': 'eks-pod-identity-agent'}],
        'managedNodeGroups': [
            self._RenderNodeGroupJson(bootstrapping_nodepool)
        ],
    }
    self._EksCtlCreate(create_json)

  def _InstallAwsLoadBalancerController(self) -> None:
    """Installs AWS Load Balancer Controller for the cluster."""
    # 1) Ensure OIDC provider (IRSA)
    vm_util.IssueCommand(
        [
            FLAGS.eksctl,
            'utils',
            'associate-iam-oidc-provider',
            f'--region={self.region}',
            f'--cluster={self.name}',
            '--approve',
        ],
        suppress_failure=lambda stdout, stderr, retcode: 'already associated'
        in stderr,
    )
    # 2) Ensure the IAM policy exists (reuse by name or create)
    list_cmd = util.AWS_PREFIX + [
        'iam',
        'list-policies',
        '--scope',
        'Local',
        '--query',
        "Policies[?PolicyName=='AWSLoadBalancerControllerIAMPolicy'].Arn | [0]",
        '--output',
        'text',
    ]
    stdout, _, _ = vm_util.IssueCommand(list_cmd)
    policy_arn = (stdout or '').strip()
    if not policy_arn or policy_arn == 'None':
      with vm_util.NamedTemporaryFile(dir=vm_util.GetTempDir(), mode='w') as tf:
        alb_policy_url = (
            'https://raw.githubusercontent.com/kubernetes-sigs/'
            + 'aws-load-balancer-controller/'
            + 'v2.13.4/docs/install/iam_policy.json')
        vm_util.IssueCommand([
            'curl',
            '-sSL',
            '-o',
            tf.name,
            alb_policy_url,
        ])
        stdout, _, _ = vm_util.IssueCommand(
            util.AWS_PREFIX
            + [
                'iam',
                'create-policy',
                '--policy-name',
                'AWSLoadBalancerControllerIAMPolicy',
                '--policy-document',
                f'file://{tf.name}',
                '--query',
                'Policy.Arn',
                '--output',
                'text',
            ]
        )
        policy_arn = (stdout or '').strip()
    # 3) Ensure ServiceAccount
    vm_util.IssueCommand(
        [
            FLAGS.eksctl,
            'create',
            'iamserviceaccount',
            '--cluster',
            self.name,
            '--region',
            self.region,
            '--namespace',
            'kube-system',
            '--name',
            'aws-load-balancer-controller',
            '--attach-policy-arn',
            policy_arn,
            '--approve',
            '--override-existing-serviceaccounts',
        ],
        suppress_failure=lambda stdout, stderr, retcode: 'already exists'
        in stderr,
    )
    # 4) Apply CRDs
    crds_url = (
        'https://raw.githubusercontent.com/aws/eks-charts/master/'
        + 'stable/aws-load-balancer-controller/crds/crds.yaml')
    kubectl.RunKubectlCommand(
        [
            'apply',
            '-f',
            crds_url,
        ],
        suppress_failure=lambda stdout, stderr, retcode: 'already exists'
        in stderr,
    )
    # 5) Install via helm.
    vm_util.IssueCommand(
        ['helm', 'repo', 'add', 'eks', 'https://aws.github.io/eks-charts'],
        suppress_failure=lambda stdout, stderr, retcode: 'already exists'
        in stderr,
    )
    vm_util.IssueCommand(['helm', 'repo', 'update', 'eks'])
    vm_util.IssueCommand([
        'helm',
        'upgrade',
        '--install',
        'aws-load-balancer-controller',
        'eks/aws-load-balancer-controller',
        '-n',
        'kube-system',
        '--kubeconfig',
        FLAGS.kubeconfig,
        '--set',
        f'clusterName={self.name}',
        '--set',
        'serviceAccount.create=false',
        '--set',
        'serviceAccount.name=aws-load-balancer-controller',
        '--set',
        f'region={self.region}',
        '--set',
        'createIngressClassResource=true',
        '--set',
        'ingressClass=alb',
        '--set',
        'replicaCount=1',
    ])
    # 6) Wait for rollout
    kubectl.RunKubectlCommand([
        'rollout',
        'status',
        'deployment/aws-load-balancer-controller',
        '-n',
        'kube-system',
        '--timeout=180s',
    ])

  @property
  def _ingress_manifest_path(self) -> str:
    """The path to the ingress manifest template file.

    Has service + ingress with annotations for AWS Load Balancer Controller
    (without IngressClass).

    Returns:
      The path to the ingress manifest template file.
    """
    return 'container/karpenter/ingress_alb.yaml.j2'

  def _WaitForIngress(self, name: str, namespace: str, port: int) -> str:
    """Wait for the ingress & apply some additional networking fixes."""
    # Wait until the ingress resource gets an address (hostname or IP).
    kubernetes_commands.WaitForResource(
        'ingress',
        kubernetes_cluster.INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
        extra_args=[name],
    )
    # Retrieve the ingress address to return it back.
    stdout, _, _ = kubectl.RunKubectlCommand([
        'get',
        'ingress',
        name,
        '-n',
        namespace,
        '-o',
        f'jsonpath={kubernetes_cluster.INGRESS_JSONPATH}',
    ])
    address = self._GetAddressFromIngress(stdout)

    # Networking fixups for ALB/NODE security groups.
    self._PostIngressNetworkingFixups(
        namespace=namespace, name=name, port=port, address=address
    )

    return address

  def _PostIngressNetworkingFixups(
      self, namespace: str, name: str, port: int, address: str
  ) -> None:
    """Fixes ALB -> node connectivity to prevent 504 errors."""
    del namespace, name  # Unused

    # 1) Get ALB security group from address
    host = (
        parse.urlparse(address).hostname
        if address.startswith('http')
        else address
    )
    normalized = (host or '').replace('dualstack.', '')
    if not normalized:
      raise errors.Config.InvalidValue(
          f'No valid hostname in address: {address}'
      )
    out, _ = vm_util.IssueRetryableCommand(
        util.AWS_PREFIX
        + [
            'elbv2',
            'describe-load-balancers',
            '--region',
            self.region,
            '--query',
            (
                'LoadBalancers[?contains(DNSName,'
                f" '{normalized}')].SecurityGroups[0]"
            ),
            '--output',
            'text',
        ],
        timeout=120,
    )
    alb_sg = (out or '').strip()
    if not alb_sg or alb_sg == 'None':
      raise errors.Resource.GetError(
          f'ALB security group not found for {normalized}'
      )
    # 2) Get node security groups from actual running instances
    ids_out, _ = vm_util.IssueRetryableCommand(
        [
            FLAGS.kubectl,
            '--kubeconfig',
            FLAGS.kubeconfig,
            'get',
            'nodes',
            '-o',
            'jsonpath={.items[*].spec.providerID}',
        ],
        timeout=120,
    )
    if not ids_out.strip():
      raise errors.Resource.GetError('No nodes found in cluster')
    # 3) Extract instance IDs
    instance_ids = [
        pid.split('/')[-1]
        for pid in ids_out.split()
        if '/' in pid and pid.split('/')[-1].startswith('i-')
    ]
    if not instance_ids:
      raise errors.Resource.GetError('No valid instance IDs found from nodes')
    out, _, _ = vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'ec2',
            'describe-instances',
            '--region',
            self.region,
            '--instance-ids',
            *instance_ids,
            '--query',
            'Reservations[].Instances[].SecurityGroups[].GroupId',
            '--output',
            'text',
        ]
    )
    if not out.strip():
      raise errors.Resource.GetError(
          f'No security groups found for instances: {instance_ids}'
      )
    node_sgs = set(out.split())
    # 4) CRITICAL: Allow ALB to reach nodes on app port (fixes 504 errors)
    for sg in node_sgs:
      vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'ec2',
              'authorize-security-group-ingress',
              '--region',
              self.region,
              '--group-id',
              sg,
              '--protocol',
              'tcp',
              '--port',
              str(port),
              '--source-group',
              alb_sg,
          ],
          suppress_failure=lambda stdout, stderr, retcode: 'already exists'
          in stderr,
      )
    logging.info(
        '[PKB][EKS] Allowed ALB SG %s -> node SGs on port %s', alb_sg, port
    )

  def _PostCreate(self):
    """Performs post-creation steps for the cluster."""
    super()._PostCreate()
    if FLAGS.eks_tune_vpc_cni_for_scale:
      logging.info('Tuning aws-node (VPC CNI) for kubernetes_node_scale')
      kubectl.RunKubectlCommand([
          'set',
          'env',
          'daemonset/aws-node',
          '-n',
          'kube-system',
          'WARM_ENI_TARGET=0',
          'WARM_IP_TARGET=1',
          'MINIMUM_IP_TARGET=1',
      ])
      kubectl.RunRetryableKubectlCommand(
          [
              'rollout',
              'status',
              'daemonset/aws-node',
              '-n',
              'kube-system',
              f'--timeout={vm_util.DEFAULT_TIMEOUT}s',
          ],
          timeout=vm_util.DEFAULT_TIMEOUT,
      )
    # Karpenter controller resources: default 1/1Gi; scale up controller when
    # more nodes are expected.
    if self.max_total_nodes > 1000:
      controller_cpu, controller_memory = 4, '16Gi'
    elif self.max_total_nodes >= 500:
      controller_cpu, controller_memory = 2, '8Gi'
    else:
      controller_cpu, controller_memory = 1, '1Gi'
    vm_util.IssueCommand([
        'helm',
        'upgrade',
        '--install',
        'karpenter',
        'oci://public.ecr.aws/karpenter/karpenter',
        '--version',
        str(_KARPENTER_VERSION),
        '--namespace',
        _KARPENTER_NAMESPACE,
        '--kubeconfig',
        FLAGS.kubeconfig,
        '--create-namespace',
        '--set',
        f'settings.clusterName={self.name}',
        '--set',
        f'settings.interruptionQueue={self.name}',
        '--set',
        f'controller.resources.requests.cpu={controller_cpu}',
        '--set',
        f'controller.resources.requests.memory={controller_memory}',
        '--set',
        f'controller.resources.limits.cpu={controller_cpu}',
        '--set',
        f'controller.resources.limits.memory={controller_memory}',
        '--set',
        'logLevel=debug',
        '--wait',
    ])
    # Ensure ALB ingress support: installs AWS Load Balancer Controller.
    if FLAGS.eks_install_alb_controller:
      self._InstallAwsLoadBalancerController()
    if virtual_machine.GPU_TYPE.value:
      # Install NVIDIA drivers.
      vm_util.IssueCommand(
          [
              'helm',
              'repo',
              'add',
              'nvdp',
              'https://nvidia.github.io/k8s-device-plugin',
          ],
          suppress_failure=lambda stdout, stderr, retcode: 'already exists'
          in stderr,
      )
      vm_util.IssueCommand(['helm', 'repo', 'update', 'nvdp'])
      # Allow node discovery pod to schedule even over NoSchedule taint.
      tolerations_string = (
          'tolerations=[{"key":"nvidia.com/gpu","operator":'
          + '"Exists","effect":"NoSchedule"}]'
      )
      vm_util.IssueCommand([
          'helm',
          'upgrade',
          '--install',
          'nvdp',
          'nvdp/nvidia-device-plugin',
          '--namespace',
          'kube-system',
          '--kubeconfig',
          FLAGS.kubeconfig,
          '--set',
          'gfd.enabled=true',
          '--set',
          'node-feature-discovery.enabled=true',
          '--set-json',
          f'nfd.worker.{tolerations_string}',
          '--set-json',
          tolerations_string,
      ])
    # Get the AMI version for current kubernetes version.
    # See e.g. https://karpenter.sh/docs/tasks/managing-amis/ for not using
    # @latest.
    ssm_ami_path = (
        f'/aws/service/eks/optimized-ami/{self.cluster_version}/'
        + 'amazon-linux-2023/x86_64/standard/recommended/image_id')
    image_id, _, _ = vm_util.IssueCommand([
        'aws',
        'ssm',
        'get-parameter',
        '--name',
        ssm_ami_path,
        '--region',
        self.region,
        '--query',
        'Parameter.Value',
    ])
    image_id = image_id.strip().strip('"')
    full_version, _, _ = vm_util.IssueCommand([
        'aws',
        'ec2',
        'describe-images',
        '--query',
        'Images[0].Name',
        '--image-ids',
        image_id,
        '--region',
        self.region,
    ])
    self.alias_version = (
        'v'
        + full_version.strip().strip('"').split(f'{self.cluster_version}-v')[1]
    )
    self._CreateKarpenterNodePool(self.default_nodepool)
    for nodepool in self.nodepools.values():
      self._CreateKarpenterNodePool(nodepool)

  def _CreateKarpenterNodePool(self, nodepool: container.BaseNodePoolConfig):
    """Creates the Karpenter NodePool and EC2NodeClass."""
    yaml_nodepool = kubernetes_commands.ConvertManifestToYamlDicts(
        'container/karpenter/nodepool.yaml.j2',
        NODEPOOL_NAME=nodepool.name,
        CLUSTER_NAME=self.name,
        ALIAS_VERSION=self.alias_version,
    )
    if nodepool.machine_families:
      machine_requirements = [
          {
              'key': 'karpenter.k8s.aws/instance-family',
              'operator': 'In',
              'values': nodepool.machine_families,
          },
          {
              'key': 'karpenter.k8s.aws/instance-generation',
              'operator': 'Gt',
              'values': ['2'],
          },
      ]
    else:
      machine_requirements = [{
          'key': 'node.kubernetes.io/instance-type',
          'operator': 'In',
          'values': [nodepool.machine_type],
      }]
    yaml_nodepool[0]['spec']['template']['spec']['requirements'].extend(
        machine_requirements
    )
    if nodepool.min_nodes == nodepool.max_nodes:
      # Not using autoscaling; set static replica count.
      yaml_nodepool[0]['spec']['replicas'] = nodepool.num_nodes
    else:
      # NodePool CPU limit: max nodes * vCPU + 5%, max 1000.
      vcpu_per_node = FLAGS.eks_karpenter_limits_vcpu_per_node
      cpu_limit = max(
          1000, math.ceil(nodepool.max_nodes * vcpu_per_node * 1.05)
      )
      yaml_nodepool[0]['spec']['limits'] = {'cpu': cpu_limit}
    kubernetes_commands.ApplyYaml(yaml_nodepool)

  def _Delete(self):
    """Deletes the control plane and worker nodes."""
    self._DeleteIngresses()
    self._CleanupKarpenter()
    super()._Delete()
    vm_util.IssueCommand([
        'aws',
        'cloudformation',
        'delete-stack',
        '--stack-name',
        self.stack_name,
        '--region',
        f'{self.region}',
    ])

  def _DeleteDependencies(self):
    """Deletes the CloudFormation stack."""
    super()._DeleteDependencies()
    delete_stack_cmd = [
        'aws',
        'cloudformation',
        'delete-stack',
        '--stack-name',
        self.stack_name,
        '--region',
        f'{self.region}',
    ]
    # Start deleting the stack but likely to fail to delete this role.
    vm_util.IssueCommand(delete_stack_cmd)
    node_role = f'KarpenterNodeRole-{self.name}'
    out, _, retcode = vm_util.IssueCommand(
        [
            'aws',
            'iam',
            'list-instance-profiles-for-role',
            '--role-name',
            node_role,
            '--region',
            f'{self.region}',
        ],
        suppress_failure=lambda stdout, stderr, rc: (
            rc != 0
            and (
                'nosuchentity' in (stderr or '').lower()
                or 'cannot be found' in (stderr or '').lower()
            )
        ),
    )
    if retcode == 0 and out.strip():
      profiles_json = json.loads(out)
    else:
      logging.info(
          'Karpenter node role %s not found or empty response; skipping'
          + ' instance profile cleanup',
          node_role,
      )
      profiles_json = {'InstanceProfiles': []}
    for profile in profiles_json.get('InstanceProfiles', []):
      profile_name = profile['InstanceProfileName']
      vm_util.IssueCommand([
          'aws',
          'iam',
          'remove-role-from-instance-profile',
          '--instance-profile-name',
          profile_name,
          '--role-name',
          node_role,
          '--region',
          f'{self.region}',
      ])
      vm_util.IssueCommand([
          'aws',
          'iam',
          'delete-instance-profile',
          '--instance-profile-name',
          profile_name,
          '--region',
          f'{self.region}',
      ])
    # Finish deleting the stack after deleting the role.
    vm_util.IssueCommand(delete_stack_cmd)

  def _DeleteIngresses(self):
    """Deletes all ingresses in all namespaces (to trigger ALB deletion)."""
    kubectl.RunKubectlCommand(
        [
            'delete',
            'ingress',
            '--all',
            '--all-namespaces',
            '--timeout=600s',
        ],
        timeout=660,
        raise_on_timeout=False,
        suppress_failure=lambda stdout, stderr, retcode: (
            'deleted' in stdout
            and 'timed out waiting for the condition' in stderr
        ),
    )

  def _CleanupKarpenter(self):
    """Cleanup Karpenter managed nodes before cluster deletion."""
    logging.info('Cleaning up Karpenter nodes...')
    # Delete NodePool resources - this will trigger node termination
    kubectl.RunRetryableKubectlCommand(
        [
            'delete',
            'nodepool,ec2nodeclass',
            '--all',
            '--timeout=120s',
        ],
        timeout=300,
        suppress_failure=lambda stdout, stderr, retcode: (
            'no resources found' in stderr.lower()
            or 'not found' in stderr.lower()
        ),
    )
    # Wait for all Karpenter nodes to be deleted
    kubectl.RunRetryableKubectlCommand(
        [
            'wait',
            '--for=delete',
            'node',
            '-l',
            'karpenter.sh/nodepool',
            '--timeout=120s',
        ],
        timeout=300,
        suppress_failure=lambda stdout, stderr, retcode: (
            'no matching resources found' in stderr.lower()
            or 'no resources found' in stderr.lower()
        ),
    )

    # Force terminate remaining EC2 instances
    stdout, _, _ = vm_util.IssueCommand(
        [
            'aws',
            'ec2',
            'describe-instances',
            '--region',
            self.region,
            '--filters',
            'Name=tag:karpenter.sh/nodepool,Values=*',
            f'Name=tag:kubernetes.io/cluster/{self.name},Values=owned',
            'Name=instance-state-name,Values=running,pending',
            '--query',
            'Reservations[].Instances[].InstanceId',
            '--output',
            'text',
        ],
    )
    instance_ids = stdout.strip().split() if stdout and stdout.strip() else []
    if instance_ids:
      logging.info('Terminating %d remaining instances', len(instance_ids))
      vm_util.IssueCommand(
          [
              'aws',
              'ec2',
              'terminate-instances',
              '--region',
              self.region,
              '--instance-ids',
              *instance_ids,
          ],
      )
      vm_util.IssueCommand(
          [
              'aws',
              'ec2',
              'wait',
              'instance-terminated',
              '--region',
              self.region,
              '--instance-ids',
              *instance_ids,
          ],
          timeout=180,
      )
    # Cleanup orphaned network interfaces
    stdout, _, _ = vm_util.IssueCommand(
        [
            'aws',
            'ec2',
            'describe-network-interfaces',
            '--region',
            self.region,
            '--filters',
            f'Name=tag:cluster.k8s.amazonaws.com/name,Values={self.name}',
            'Name=status,Values=available',
            '--query',
            'NetworkInterfaces[].NetworkInterfaceId',
            '--output',
            'text',
        ],
        suppress_failure=lambda stdout, stderr, retcode: (
            'not found' in stderr.lower()
        ),
    )
    eni_ids = stdout.strip().split() if stdout and stdout.strip() else []
    if eni_ids:
      logging.info('Deleting %d orphaned network interfaces', len(eni_ids))
      for eni_id in eni_ids:
        # Bind eni_id by default to avoid loop closure issues if
        # this is refactored.
        def _delete_one_eni(eni_id=eni_id) -> None:
          _, stderr, retcode = vm_util.IssueCommand(
              [
                  'aws',
                  'ec2',
                  'delete-network-interface',
                  '--region',
                  self.region,
                  '--network-interface-id',
                  eni_id,
              ],
              raise_on_failure=False,
          )
          if retcode == 0:
            return
          stderr_lower = (stderr or '').lower()
          # ENI already deleted (e.g. by another process or previous attempt).
          if 'invalidnetworkinterfaceid.notfound' in stderr_lower:
            return
          # RequestLimitExceeded (throttle): retry via vm_util.Retry.
          if 'requestlimitexceeded' in stderr_lower:
            raise errors.Resource.RetryableDeletionError(stderr or '')
          raise errors.VmUtil.IssueCommandError(
              f'DeleteNetworkInterface failed: {stderr}'
          )

        # max_retries=5 yields 6 CLI attempts (tries > 5 on 6th failure).
        vm_util.Retry(
            poll_interval=10,
            max_retries=5,
            retryable_exceptions=(errors.Resource.RetryableDeletionError,),
        )(_delete_one_eni)()

  def _IsReady(self):
    """Returns True if cluster is running. Autopilot defaults to 0 nodes."""
    stdout, _, _ = kubectl.RunKubectlCommand(['cluster-info'])
    # These two strings are printed in sequence, but with ansi color code
    # escape characters in between.
    return 'Kubernetes control plane' in stdout and 'is running at' in stdout

  def GetDefaultStorageClass(self) -> str:
    """Gets the default storage class for the provider."""
    return aws_disk.GP2

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_cluster.DEFAULT_NODEPOOL
  ):
    """Change the number of nodes in the node group."""
    raise NotImplementedError()

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Gets the node selectors section of a yaml for the provider."""
    selectors = {}
    machine_family = util.GetMachineFamily(machine_type)
    if machine_family:
      selectors['karpenter.k8s.aws/instance-family'] = machine_family
    return selectors

  def GetNodePoolNames(self) -> list[str]:
    """Gets node pool names for the cluster.

    Returns:
      All CRD NodePool names created by Karpenter.
    """
    cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'get',
        'nodepool',
        '-o',
        'json',
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode:
      logging.warning(
          'Failed to get Karpenter NodePools: %s, error: %s', stdout, stderr
      )
      return []
    nodepools = json.loads(stdout)
    return [item['metadata']['name'] for item in nodepools.get('items', [])]

  def AddNodepool(self, batch_name, pool_id):
    kubernetes_commands.ApplyManifest(
        'provision_node_pools/karpenter/nodepool.yaml.j2',
        batch_name=batch_name,
        pool_id=pool_id,
        cluster_name=self.name,
    )
