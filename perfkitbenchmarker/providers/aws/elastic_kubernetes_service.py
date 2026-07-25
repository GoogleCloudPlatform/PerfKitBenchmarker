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
import functools
import json
import time
import logging
import math
import re
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

FLAGS = flags.FLAGS
# GPU types which practically require spot to get.
_RARE_GPU_TYPES = [
    virtual_machine_spec.GPU_H100,
    virtual_machine_spec.GPU_A100,
    virtual_machine_spec.GPU_B200,
]


def RecursivelyUpdateDictionary(
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
      original[k] = RecursivelyUpdateDictionary(original.get(k, {}), v)  # pyrefly: ignore[bad-argument-type]
    else:
      original[k] = v
  return original


def ApplyInferenceS3PvAndPvc() -> None:
  """Apply the PV and PVC backed by Mountpoint for Amazon S3 CSI driver.

  Prerequisites:
    - Model weights uploaded to the S3 bucket
    (--k8s_inference_server_s3_bucket).
    - S3 CSI driver installed on the cluster (--eks_install_s3_csi_addon).
  """
  bucket = aws_flags.K8S_INFERENCE_SERVER_S3_BUCKET.value
  region = aws_flags.K8S_INFERENCE_SERVER_S3_REGION.value
  if not bucket or not region:
    raise errors.Resource.CreationError(
        'Both --k8s_inference_server_s3_bucket and '
        '--k8s_inference_server_s3_region are required to apply the S3 PVC.'
    )
  kubernetes_commands.ApplyManifest(
      'container/kubernetes_ai_inference/s3_pv_pvc.yaml.j2',
      s3_bucket=bucket,
      s3_region=region,
  )
  logging.info('Successfully applied S3 PVC.')


class BaseEksCluster(kubernetes_cluster.KubernetesCluster):
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
    self.node_to_nodepool: dict[str, container.BaseNodePoolConfig | None] = {}
    self.node_to_machine_type: dict[str, str | None] = {}

  def _ChooseSecondZone(self):
    """Choose a second zone for the control plane if only one is specified."""
    if len(self.control_plane_zones) == 1:
      # eksctl essentially requires you pass --zones if you pass --node-zones
      # and --zones must have at least 2 zones
      # https://github.com/weaveworks/eksctl/issues/4735
      self.control_plane_zones.append(
          self.region + ('b' if self.zone.endswith('a') else 'a')  # pyrefly: ignore[unsupported-operation]
      )

  def _CreateDependencies(self):
    """Set up the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _DeleteDependencies(self):
    """Delete the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _EksCtlCreate(self, create_json: dict[str, Any]):
    """Creates the EKS cluster."""
    # If multiple zones are passed use them for the control plane.
    # Otherwise EKS will auto-select control plane zones in the region.
    if self.control_plane_zones:
      create_json['availabilityZones'] = self.control_plane_zones
    # Schema for the cluster create command is here:
    # https://schema.eksctl.io/
    create_json = RecursivelyUpdateDictionary(
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
        cmd, timeout=2700, raise_on_failure=False
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
    vm_util.IssueCommand(cmd, timeout=1800)  # pyrefly: ignore[bad-argument-type]

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
      logging.info(
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
          logging.info(
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
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)  # pyrefly: ignore[bad-argument-type]
    if retcode:
      raise errors.Resource.GetError(
          f'Failed to get nodegroups: {stdout}, error: {stderr}'
      )
    nodegroups = json.loads(stdout)
    return [ng['Name'] for ng in nodegroups]


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
        'vpc': {
            'nat': {'gateway': 'Disable'},
        },
    }
    self._EksCtlCreate(create_json)

    # Above create command passes "withOidc=true", but it doesn't seem to work &
    # therefore this command is needed.
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

  @functools.lru_cache(maxsize=None)
  def _DiscoverSubnets(self) -> list[str]:
    """Returns the EKS cluster's subnet IDs (cached after first call)."""
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
    return info['cluster']['resourcesVpcConfig']['subnetIds']

  @functools.lru_cache(maxsize=None)
  def _DiscoverSubnetsPerAZ(self) -> dict[str, str]:
    """Returns a mapping of {AZ: subnet_id} for the cluster's subnets.

    Used by CreateNodePoolAsync to distribute nodegroups round-robin across
    AZs, avoiding per-AZ EC2 capacity limits when creating many pools.
    Only returns AZs that are in control_plane_zones (if specified).
    Cached after first call (via functools.lru_cache).
    """
    subnet_ids = self._DiscoverSubnets()
    if not subnet_ids:
      return {}

    # Describe subnets to get their AZ mapping
    out, _, rc = vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'ec2',
            'describe-subnets',
            '--region',
            self.region,
            '--subnet-ids',
            *subnet_ids,
            '--query',
            (
                'Subnets[*].{SubnetId:SubnetId,AZ:AvailabilityZone,'
                'Public:MapPublicIpOnLaunch}'
            ),
            '--output',
            'json',
        ],
        raise_on_failure=False,
    )
    if rc:
      logging.info(
          '[EKS] Could not describe subnets for AZ mapping —'
          ' falling back to all subnets'
      )
      return {}

    subnets = json.loads(out)

    # Do NOT filter by control_plane_zones — PKB truncates it to 2 AZs.
    # Accept all subnets the VPC has across all AZs.
    # Build AZ map — always prefer public subnets (MapPublicIpOnLaunch=True)
    # which have an internet gateway route. Private subnets lack IGW routes
    # and nodes launched there cannot reach the EKS API server to join.
    az_map: dict[str, str] = {}
    az_map_private: dict[str, str] = {}
    for s in subnets:
      az = s['AZ']
      if s.get('Public'):
        az_map[az] = s['SubnetId']
        logging.info('[EKS] AZ %s → public subnet %s', az, s['SubnetId'])
      elif az not in az_map:
        az_map_private[az] = s['SubnetId']
    for az, sid in az_map_private.items():
      if az not in az_map:
        logging.info(
            '[EKS] AZ %s has no public subnet — using private %s', az, sid
        )
        az_map[az] = sid

    logging.info(
        '[EKS] Subnet-per-AZ mapping: %s (from %d total subnets)',
        az_map,
        len(subnet_ids),
    )
    return az_map

  @functools.lru_cache(maxsize=None)
  def _ResolveReleaseVersion(self, minor: str) -> str:
    """Returns the EKS-optimized AMI release version (e.g. '1.33.10-20260124').

    Used to populate `releaseVersion` in the create-nodegroup payload so the
    benchmark can pin specific K8s minors. lru_cache is thread-safe by
    construction, so at scale, of N workers asking for the same minor, only
    the first does the SSM lookup -- the rest get the cached result.
    """
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
    return out.strip()

  @functools.lru_cache(maxsize=None)
  def _DiscoverNodeRoleArn(self) -> str:
    """Returns a node IAM role ARN by inspecting an existing nodegroup."""
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
        return role
    raise errors.Resource.CreationError(
        'No existing nodegroup found to discover node role for '
        f'cluster {self.name}.'
    )

  def CreateNodePoolAsync(
      self,
      nodepool_config: container.BaseNodePoolConfig,
      node_version: str | None = None,
  ) -> str:
    """Initiates EKS nodegroup create; returns ng_active handle."""
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
      # pkbmb (Scenario B) has no suffix — assign to us-east-1b (idx=1)
      # to avoid competing with us-east-1a which has the default nodegroup.
      idx = int(suffix) if suffix else 1
      zones = sorted(az_subnets.keys())
      assigned_az = zones[idx % len(zones)]
      subnets = [az_subnets[assigned_az]]
      logging.info(
          '[EKS] CreateNodePool %s -> AZ=%s subnet=%s (round-robin idx=%d)',
          name,
          assigned_az,
          subnets[0],
          idx,
      )
    else:
      subnets = self._DiscoverSubnets()
      logging.info(
          '[EKS] CreateNodePool %s -> using all subnets (single AZ)',
          nodepool_config.name,
      )

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
    az = (
        assigned_az if az_subnets and len(az_subnets) > 1 else f'{self.region}a'
    )
    # Only look up launch templates and capacity reservations when
    # --eks_reserve_capacity_per_az=true. Other benchmarks skip this entirely.
    if FLAGS.eks_reserve_capacity_per_az:
      lt_name = f'pkb-eks-lt-{az}'
      lt_out, _, lt_rc = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'ec2',
              'describe-launch-templates',
              '--region',
              self.region,
              '--filters',
              f'Name=launch-template-name,Values={lt_name}',
              '--query',
              'LaunchTemplates[0].LaunchTemplateId',
              '--output',
              'text',
          ],
          raise_on_failure=False,
      )
      res_id = getattr(self, '_capacity_reservation_ids', {}).get(az, '')
      if (
          res_id
          and lt_rc == 0
          and lt_out.strip()
          and lt_out.strip() not in ('None', 'null', '')
      ):
        payload['launchTemplate'] = {
            'id': lt_out.strip(),
            'version': '$Latest',
        }
        # When launch template specifies an ImageId, EKS rejects these fields:
        # - releaseVersion: conflicts with AMI
        # - instanceTypes:  must come from launch template only
        # - amiType:        conflicts with AMI
        payload.pop('releaseVersion', None)
        payload.pop('instanceTypes', None)
        payload.pop('amiType', None)
        logging.info(
            # pylint: disable-next=implicit-str-concat
            '[EKS] Nodegroup %s using launch template %s targeting reservation'
            ' %s in AZ %s',
            nodepool_config.name,
            lt_name,
            res_id,
            az,
        )
      else:
        logging.info(
            '[EKS] No reservation/template for AZ %s — using on-demand', az
        )

    if node_version:
      # EKS rejects both 'version' and 'releaseVersion' when a launch template
      # with ImageId is specified — skip both when launchTemplate is in use.
      if 'launchTemplate' not in payload:
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
    for attempt in range(5):
      _, stderr, retcode = vm_util.IssueCommand(
          cmd, timeout=300, raise_on_failure=False
      )
      if retcode == 0:
        break
      throttled = (
          'Request limit exceeded' in stderr or 'ThrottlingException' in stderr
      )
      if throttled and attempt < 4:
        logging.info(
            '[EKS] CreateNodegroup throttled — retry %d/5', attempt + 1
        )
        time.sleep(10 * (2**attempt))
        continue
      raise errors.Resource.CreationError(stderr)
    return f'ng_active:{nodepool_config.name}'

  def UpgradeNodePoolAsync(self, name: str, target_version: str) -> str:
    """Initiates EKS nodegroup upgrade; returns ng_active handle."""
    # For Custom AMI nodegroups (using launch template with ImageId),
    # EKS requires the launch template to be passed on upgrade.
    # Determine the AZ for this nodegroup to find the correct launch template.
    suffix = ''.join(c for c in name if c.isdigit())
    # pkbmb (Scenario B) has no suffix — use idx=1 (us-east-1b) to avoid
    # competing with us-east-1a which already has the default nodegroup
    idx = int(suffix) if suffix else 1
    # Only look up launch template when capacity reservations are enabled.
    # For other benchmarks, always use standard kubernetes-version upgrade.
    lt_id = ''
    lt_name = ''
    az = f'{self.region}a'
    if FLAGS.eks_reserve_capacity_per_az:
      az_subnets = self._DiscoverSubnetsPerAZ()
      if az_subnets and len(az_subnets) > 1:
        zones = sorted(az_subnets.keys())
        az = zones[idx % len(zones)]
    if FLAGS.eks_reserve_capacity_per_az:
      lt_name = f'pkb-eks-lt-{az}'
      lt_out, _, lt_rc = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'ec2',
              'describe-launch-templates',
              '--region',
              self.region,
              '--filters',
              f'Name=launch-template-name,Values={lt_name}',
              '--query',
              'LaunchTemplates[0].LaunchTemplateId',
              '--output',
              'text',
          ],
          raise_on_failure=False,
      )
      lt_id = (
          lt_out.strip()
          if lt_rc == 0 and lt_out.strip() not in ('', 'None', 'null')
          else ''
      )

    # Custom AMI nodegroups cannot use --kubernetes-version;
    # use launch template only.
    if lt_id:
      cmd = util.AWS_PREFIX + [
          'eks',
          'update-nodegroup-version',
          '--cluster-name',
          self.name,
          '--nodegroup-name',
          name,
          '--region',
          self.region,
          '--launch-template',
          f'id={lt_id},version=$Latest',
      ]
      logging.info(
          '[EKS] Upgrading %s with launch template %s in AZ %s',
          name,
          lt_name,
          az,
      )
    else:
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
    """Initiates EKS nodegroup delete; returns ng_gone handle."""
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
    log_types = [
        'api',
        'audit',
        'authenticator',
        'controllerManager',
        'scheduler',
    ]
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
    payload = json.dumps(
        {'clusterLogging': [{'types': log_types, 'enabled': not any_enabled}]}
    )
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
    # Wait for cluster ACTIVE before firing update — at 99-pool scale
    # Scenario A leaves the cluster UPDATING causing ResourceInUseException.
    logging.info('[EKS] Waiting for cluster ACTIVE before ClusterUpdate...')
    for _ in range(60):
      status_out, _, status_rc = vm_util.IssueCommand(
          util.AWS_PREFIX
          + [
              'eks',
              'describe-cluster',
              '--name',
              self.name,
              '--region',
              self.region,
              '--query',
              'cluster.status',
              '--output',
              'text',
          ],
          raise_on_failure=False,
      )
      if status_rc == 0 and status_out.strip() == 'ACTIVE':
        logging.info('[EKS] Cluster is ACTIVE — proceeding with ClusterUpdate')
        break
      logging.info(
          '[EKS] Cluster status=%s — waiting 5s...', status_out.strip()
      )
      time.sleep(5)
    stdout = ''
    for attempt in range(10):
      stdout, stderr, retcode = vm_util.IssueCommand(
          upd, timeout=300, raise_on_failure=False
      )
      if retcode == 0:
        break
      if 'ResourceInUseException' in stderr and attempt < 9:
        logging.info(
            '[EKS] UpdateClusterConfig ResourceInUseException — retry %d/10',
            attempt + 1,
        )
        time.sleep(30)
        continue
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
    target = f'{major}.{minor}'
    initial = f'{major}.{minor - 1}'
    logging.info(
        '[EKS] ResolveNodePoolVersions: cluster=%s initial=%s target=%s',
        cluster_ver,
        initial,
        target,
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
    log_types = [
        'api',
        'audit',
        'authenticator',
        'controllerManager',
        'scheduler',
    ]
    describe = util.AWS_PREFIX + [
        'eks',
        'describe-cluster',
        '--name',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(describe)
    info = json.loads(stdout)
    current = info['cluster'].get('logging', {}).get('clusterLogging', [])
    any_enabled = any(entry.get('enabled', False) for entry in current)
    new_enabled = not any_enabled
    logging_payload = json.dumps(
        {'clusterLogging': [{'types': log_types, 'enabled': new_enabled}]}
    )
    update = util.AWS_PREFIX + [
        'eks',
        'update-cluster-config',
        '--name',
        self.name,
        '--region',
        self.region,
        '--logging',
        logging_payload,
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
          'eks',
          'describe-cluster',
          '--name',
          self.name,
          '--region',
          self.region,
          '--query',
          'cluster.status',
          '--output',
          'text',
      ]
      out, _, _ = vm_util.IssueCommand(query)
      status = out.strip()
      if status != 'ACTIVE':
        raise errors.Resource.RetryableCreationError(f'cluster status={status}')

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
    is_rare_gpu = self.gpu_type in _RARE_GPU_TYPES
    self.use_spot: bool = aws_flags.USE_AWS_SPOT_INSTANCES.value or is_rare_gpu

  def _Create(self):
    """Creates the control plane and worker nodes."""
    self._EksCtlCreate({
        'autoModeConfig': {
            'enabled': True,
            'nodePools': ['general-purpose', 'system'],
        }
    })

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
    if aws_flags.EKS_INSTALL_S3_CSI_ADDON.value:
      self._InstallS3CsiAddon()
    if aws_flags.EKS_INSTALL_NEURON_DEVICE_PLUGIN.value:
      self._InstallNeuronDevicePlugin()

  # AWS-stable identifiers for the Mountpoint for S3 CSI driver:
  # https://docs.aws.amazon.com/eks/latest/userguide/s3-csi.html
  _S3_CSI_ADDON_NAME = 'aws-mountpoint-s3-csi-driver'
  _S3_CSI_SERVICE_ACCOUNT_NAME = 's3-csi-driver-sa'
  _S3_CSI_SERVICE_ACCOUNT_NAMESPACE = 'kube-system'
  # PKB-managed IAM resources reused across runs.
  _S3_CSI_ROLE_NAME = 'PkbS3CsiDriverRole'
  _S3_CSI_POLICY_NAME_PREFIX = 'PkbS3CsiDriverReadOnly'

  def _InstallS3CsiAddon(self):
    """Installs the S3 CSI Driver and the IAM glue (Role/Policy + PIA)."""
    bucket = aws_flags.K8S_INFERENCE_SERVER_S3_BUCKET.value
    if not bucket:
      raise errors.Config.InvalidValue(
          '--k8s_inference_server_s3_bucket is required when '
          '--eks_install_s3_csi_addon is enabled.'
      )
    policy_arn = self._EnsureS3CsiPolicy(bucket)
    role_arn = self._EnsureS3CsiRole(policy_arn)

    vm_util.IssueRetryableCommand(
        util.AWS_PREFIX
        + [
            'eks',
            'create-addon',
            '--region',
            self.region,
            '--cluster-name',
            self.name,
            '--addon-name',
            self._S3_CSI_ADDON_NAME,
            '--resolve-conflicts',
            'OVERWRITE',
        ]
    )
    vm_util.IssueRetryableCommand(
        util.AWS_PREFIX
        + [
            'eks',
            'wait',
            'addon-active',
            '--region',
            self.region,
            '--cluster-name',
            self.name,
            '--addon-name',
            self._S3_CSI_ADDON_NAME,
        ],
        timeout=900,
    )
    self._EnsureS3CsiPodIdentityAssociation(role_arn)
    logging.info('Successfully installed Mountpoint for S3 CSI Driver.')

  def _EnsureS3CsiPolicy(self, bucket: str) -> str:
    """Idempotently creates the read-only S3 policy; returns its ARN."""
    name = f'{self._S3_CSI_POLICY_NAME_PREFIX}-{bucket}'
    doc = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Action': ['s3:ListBucket'],
                'Resource': [f'arn:aws:s3:::{bucket}'],
            },
            {
                'Effect': 'Allow',
                'Action': ['s3:GetObject'],
                'Resource': [f'arn:aws:s3:::{bucket}/*'],
            },
        ],
    }
    vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'iam',
            'create-policy',
            '--policy-name',
            name,
            '--policy-document',
            json.dumps(doc),
        ],
        suppress_failure=lambda stdout, stderr, retcode: (
            'EntityAlreadyExists' in stderr
        ),
    )
    return f'arn:aws:iam::{self.account}:policy/{name}'

  def _EnsureS3CsiRole(self, policy_arn: str) -> str:
    """Idempotently creates the S3 CSI driver role; returns its ARN."""
    trust = {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'Service': 'pods.eks.amazonaws.com'},
            'Action': ['sts:AssumeRole', 'sts:TagSession'],
        }],
    }
    vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'iam',
            'create-role',
            '--role-name',
            self._S3_CSI_ROLE_NAME,
            '--assume-role-policy-document',
            json.dumps(trust),
        ],
        suppress_failure=lambda stdout, stderr, retcode: (
            'EntityAlreadyExists' in stderr
        ),
    )
    # attach-role-policy is idempotent at the AWS API level.
    vm_util.IssueCommand(
        util.AWS_PREFIX
        + [
            'iam',
            'attach-role-policy',
            '--role-name',
            self._S3_CSI_ROLE_NAME,
            '--policy-arn',
            policy_arn,
        ]
    )
    return f'arn:aws:iam::{self.account}:role/{self._S3_CSI_ROLE_NAME}'

  def _EnsureS3CsiPodIdentityAssociation(self, role_arn: str) -> None:
    """Idempotently binds the role to s3-csi-driver-sa via Pod Identity."""
    vm_util.IssueCommand(
        util.AWS_PREFIX  # pyrefly: ignore[bad-argument-type]
        + [
            'eks',
            'create-pod-identity-association',
            '--region',
            self.region,
            '--cluster-name',
            self.name,
            '--namespace',
            self._S3_CSI_SERVICE_ACCOUNT_NAMESPACE,
            '--service-account',
            self._S3_CSI_SERVICE_ACCOUNT_NAME,
            '--role-arn',
            role_arn,
        ],
        suppress_failure=lambda stdout, stderr, retcode: (
            'already exists' in stderr.lower()
        ),
    )

  def _InstallNeuronDevicePlugin(self):
    """Applies the AWS Neuron Device Plugin DaemonSet to the cluster."""
    # PKB only renders .j2 when ApplyManifest kwargs is non-empty
    # (vm_util.ReadAndRenderJinja2Template). With no kwargs the literal
    # "{{ neuron_device_plugin_image }}" would be sent to kubectl.
    default_image = 'public.ecr.aws/neuron/neuron-device-plugin:2.22.4.0'
    kubernetes_commands.ApplyManifest(
        'container/aws/neuron-device-plugin.yaml.j2',
        neuron_device_plugin_image=default_image,
    )
    logging.info('Successfully applied Neuron Device Plugin DaemonSet.')

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
    vm_util.IssueCommand(cmd, timeout=1800)  # pyrefly: ignore[bad-argument-type]

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
    # Theoretically needed in mixed mode, but deployments fail without it:
    # https://docs.aws.amazon.com/eks/latest/userguide/associate-workload.html#_require_a_workload_is_deployed_to_eks_auto_mode_nodes
    selectors = {'eks.amazonaws.com/compute-type': 'auto'}
    if self.use_spot:
      selectors['karpenter.sh/capacity-type'] = 'spot'
    if self.gpu_type:
      selectors['eks.amazonaws.com/instance-gpu-name'] = self.gpu_type
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
    vm_util.IssueCommand([
        'curl',
        '-fsSL',
        f'https://raw.githubusercontent.com/aws/karpenter-provider-aws/v{_KARPENTER_VERSION}/website/content/en/preview/getting-started/getting-started-with-karpenter/cloudformation.yaml',
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
                    f'arn:aws:iam::{self.account}:policy/KarpenterControllerPolicy-{self.name}'
                ],
            }],
        },
        'iamIdentityMappings': [{
            'arn': (
                f'arn:aws:iam::{self.account}:role/KarpenterNodeRole-{self.name}'
            ),
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
        vm_util.IssueCommand([
            'curl',
            '-sSL',
            '-o',
            tf.name,
            (
                'https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/'
                'v2.13.4/docs/install/iam_policy.json'
            ),
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
        [  # pyrefly: ignore[bad-argument-type]
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
    kubectl.RunKubectlCommand(
        [
            'apply',
            '-f',
            'https://raw.githubusercontent.com/aws/eks-charts/master/stable/aws-load-balancer-controller/crds/crds.yaml',
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
    """Fixs ALB -> nodes connectivity to prevent 504 errors from unhealthy targets."""

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
        util.AWS_PREFIX  # pyrefly: ignore[bad-argument-type]
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
          util.AWS_PREFIX  # pyrefly: ignore[bad-argument-type]
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
              '--timeout=%ds' % vm_util.DEFAULT_TIMEOUT,
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
        # Not always needed but enable the feature.
        '--set',
        'settings.featureGates.staticCapacity=true',
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
    image_id, _, _ = vm_util.IssueCommand([  # pyrefly: ignore[bad-argument-type]
        'aws',
        'ssm',
        'get-parameter',
        '--name',
        f'/aws/service/eks/optimized-ami/{self.cluster_version}/amazon-linux-2023/x86_64/standard/recommended/image_id',
        '--region',
        self.region,
        '--query',
        'Parameter.Value',
    ])
    image_id = image_id.strip().strip('"')
    full_version, _, _ = vm_util.IssueCommand([  # pyrefly: ignore[bad-argument-type]
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
      # Not using autoscaling; set static replica count & don't consolidate.
      del yaml_nodepool[0]['spec']['disruption']
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
          ' instance profile cleanup',
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
        [  # pyrefly: ignore[bad-argument-type]
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
          [  # pyrefly: ignore[bad-argument-type]
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
          [  # pyrefly: ignore[bad-argument-type]
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
        [  # pyrefly: ignore[bad-argument-type]
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
        def _DeleteOneEni(eni_id=eni_id) -> None:
          _, stderr, retcode = vm_util.IssueCommand(
              [  # pyrefly: ignore[bad-argument-type]
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
        )(_DeleteOneEni)()

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
