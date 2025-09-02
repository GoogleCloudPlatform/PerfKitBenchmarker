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
import json
import logging
import re
from typing import Any
import time
from urllib.parse import urlparse

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
      original[k] = RecursivelyUpdateDictionary(original.get(k, {}), v)
    else:
      original[k] = v
  return original


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
    self.node_to_nodepool: dict[
        str, container_service.BaseNodePoolConfig | None
    ] = {}

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
        cmd, timeout=1800, raise_on_failure=False
    )
    if retcode:
      if 'The maximum number of VPCs has been reached' in stdout:
        raise errors.Benchmarks.QuotaFailure(stdout)
      else:
        raise errors.Resource.CreationError(stdout)

  def _RenderNodeGroupJson(
      self, nodepool: container_service.BaseNodePoolConfig
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
    if (
        nodepool.name == self.default_nodepool.name
        and self.min_nodes != self.max_nodes
    ):
      # Min / max config only apply to the default nodepool.
      group_json['minSize'] = self.min_nodes
      group_json['maxSize'] = self.max_nodes
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
  ) -> container_service.BaseNodePoolConfig | None:
    """Get the nodepool from the node name.

    This method assumes that the nodepool name is embedded in the node name.
    Better would be a lookup from the cloud provider.

    Args:
      node_name: The name of the node.

    Returns:
      The associated nodepool, or None if not found.
    """
    if node_name in self.node_to_nodepool:
      return self.node_to_nodepool[node_name]
    nodepool_name, err, code = container_service.RunKubectlCommand(
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

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    return aws_disk.GP2

  def DeployIngress(self, name: str, namespace: str, port: int) -> str:
    """Deploys an Ingress resource to the cluster."""
    self.ApplyManifest(
        'container/ingress.yaml.j2',
        name=name,
        namespace=namespace,
        port=port,
    )
    self.WaitForResource(
        'ingress',
        container_service.INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
        extra_args=[name],
    )
    stdout, _, _ = container_service.RunKubectlCommand([
        'get',
        'ingress',
        name,
        '-n',
        namespace,
        '-o',
        f'jsonpath={container_service.INGRESS_JSONPATH}',
    ])
    return self._GetAddressFromIngress(stdout)


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
      self, nodepool: container_service.BaseNodePoolConfig
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

  def GetNodeSelectors(self) -> list[str]:
    """Get the node selectors section of a yaml for the provider."""
    # Theoretically needed in mixed mode, but deployments fail without it:
    # https://docs.aws.amazon.com/eks/latest/userguide/associate-workload.html#_require_a_workload_is_deployed_to_eks_auto_mode_nodes
    return ['eks.amazonaws.com/compute-type: auto']


_KARPENTER_NAMESPACE = 'kube-system'
_KARPENTER_VERSION = '1.5.0'
_DEAULT_K8S_VERSION = '1.32'


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
    self.cluster_version: str = self.cluster_version or _DEAULT_K8S_VERSION

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    pass

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
    vm_util.IssueCommand([
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
    ])
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
        'managedNodeGroups': [self._RenderNodeGroupJson(self.default_nodepool)],
    }
    self._EksCtlCreate(create_json)

  def _InstallAwsLoadBalancerController(self) -> None:
    """Install AWS Load Balancer Controller for the cluster.

      - Associates the OIDC provider (IRSA).
      - Reuses a customer-managed IAM policy if present; otherwise creates it
        from the official policy JSON (pinned version).
      - Ensures the ServiceAccount with the attached policy exists.
      - Applies CRDs and installs/updates the Helm chart with a correct IngressClass.
      - Waits for the controller rollout.
    """
    # 1) Ensure OIDC provider (IRSA)
    vm_util.IssueCommand([
        FLAGS.eksctl, 'utils', 'associate-iam-oidc-provider',
        f'--region={self.region}',
        f'--cluster={self.name}',
        '--approve',
    ], raise_on_failure=False)

    # 2) Ensure the IAM policy exists (reuse by name or create)
    list_cmd = util.AWS_PREFIX + [
        'iam', 'list-policies', '--scope', 'Local',
        '--query', "Policies[?PolicyName=='AWSLoadBalancerControllerIAMPolicy'].Arn | [0]",
        '--output', 'text',
    ]
    stdout, _, _ = vm_util.IssueCommand(list_cmd, raise_on_failure=False)
    policy_arn = (stdout or '').strip()
    if not policy_arn or policy_arn == 'None':
        # Download the official policy JSON (pinned to a specific controller version).
        with vm_util.NamedTemporaryFile(dir=vm_util.GetTempDir(), mode='w') as tf:
            vm_util.IssueCommand([
                'curl', '-sSL', '-o', tf.name,
                'https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/'
                'v2.13.4/docs/install/iam_policy.json',
            ], raise_on_failure=True)
            create_cmd = util.AWS_PREFIX + [
                'iam', 'create-policy',
                '--policy-name', 'AWSLoadBalancerControllerIAMPolicy',
                '--policy-document', f'file://{tf.name}',
                '--query', 'Policy.Arn', '--output', 'text',
            ]
            stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=True)
            policy_arn = (stdout or '').strip()


    # 3) Ensure ServiceAccount with the attached IAM policy (IRSA)
    vm_util.IssueCommand([
        FLAGS.eksctl, 'create', 'iamserviceaccount',
        '--cluster', self.name,
        '--namespace', 'kube-system',
        '--name', 'aws-load-balancer-controller',
        '--attach-policy-arn', policy_arn,
        '--approve',
        '--override-existing-serviceaccounts',
    ], raise_on_failure=False)

    # 4) Ensure CRDs (safe to re-apply)
    container_service.RunKubectlCommand([
        'apply', '-k',
        'github.com/aws/eks-charts/stable/aws-load-balancer-controller/crds?ref=v2.13.4',
    ], raise_on_failure=False)

    # 5) Install/upgrade the Helm chart (creates a correct IngressClass 'alb')
    vm_util.IssueCommand(['helm', 'repo', 'add', 'eks', 'https://aws.github.io/eks-charts'],
                          raise_on_failure=False)
    vm_util.IssueCommand(['helm', 'repo', 'update', 'eks'], raise_on_failure=False)
    vm_util.IssueCommand([
        'helm', 'upgrade', '--install',
        'aws-load-balancer-controller', 'eks/aws-load-balancer-controller',
        '-n', 'kube-system',
        '--kubeconfig', FLAGS.kubeconfig,
        '--set', f'clusterName={self.name}',
        '--set', 'serviceAccount.create=false',
        '--set', 'serviceAccount.name=aws-load-balancer-controller',
        '--set', f'region={self.region}',
        '--set', 'createIngressClassResource=true',
        '--set', 'ingressClass=alb',
    ], raise_on_failure=True)

    # 6) Wait for the controller to be ready (best-effort)
    container_service.RunKubectlCommand([
        'rollout', 'status', 'deployment/aws-load-balancer-controller',
        '-n', 'kube-system', '--timeout=180s',
    ], raise_on_failure=False)

  def DeployIngress(self, name: str, namespace: str, port: int) -> str:
    """Deploys only Service + Ingress (without IngressClass) for AWS Load Balancer Controller."""
    # Apply the custom manifest template (service + ingress with annotations).
    self.ApplyManifest(
        'container/kubernetes_hpa/ingress_alb.yaml.j2',
        name=name,
        namespace=namespace,
        port=port,
    )
    # Wait until the ingress resource gets an address (hostname or IP).
    self.WaitForResource(
        'ingress',
        container_service.INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
        extra_args=[name],
    )
    # Retrieve the ingress address to return it back.
    stdout, _, _ = container_service.RunKubectlCommand([
        'get', 'ingress', name,
        '-n', namespace,
        '-o', f'jsonpath={container_service.INGRESS_JSONPATH}',
    ])
    address = self._GetAddressFromIngress(stdout)

    # Networking fixups for ALB/NODE security groups.
    self._PostIngressNetworkingFixups(namespace=namespace, name=name, port=port, address=address)

    return address

  def _PostIngressNetworkingFixups(self, namespace: str, name: str, port: int, address: str) -> None:
    """Best-effort ALB/SG fixups once the Ingress has an address.

    Steps (idempotent):
      - Resolve the ALB security group from the Ingress hostname.
      - Open port 80 on the ALB SG (public).
      - Find node security groups for running instances in this cluster.
      - Ensure the cluster tag on node SGs.
      - Allow traffic from ALB SG to node SGs on the application port.
    """
    # 1) Find the ALB security group by Ingress DNS name.
    # Ensure 80/tcp is open on the ALB security group
    raw = (address or '').strip()
    host = urlparse(raw).hostname if raw.startswith('http') else raw
    normalized = (host or '').replace('dualstack.', '')
    alb_sg = ''
    lb_query = util.AWS_PREFIX + [
        'elbv2', 'describe-load-balancers',
        '--region', self.region,
        '--query', f"LoadBalancers[?contains(DNSName, '{normalized}')].SecurityGroups[0]",
        '--output', 'text',
    ]
    # Resolve ALB Security Group with retries (up to 120s total).
    try:
        out, _, _ = vm_util.IssueRetryableCommand(
            lb_query,
            should_retry=lambda retcode, stdout, stderr: (stdout or '').strip() in ('', 'None'),
            timeout=120,   # total wait time
            delay=5        # seconds between retries
        )
        alb_sg = (out or '').strip()
    except Exception:
        logging.warning('[PKB][EKS] Could not resolve ALB SG for %s; skipping fixups', normalized)
        return
    if not alb_sg or alb_sg == 'None':
        logging.warning('[PKB][EKS] Could not resolve ALB SG for %s; skipping fixups', normalized)
        return
    alb_sg = (out or '').strip()
    if alb_sg and alb_sg != 'None':
      vm_util.IssueCommand(util.AWS_PREFIX + [
          'ec2', 'authorize-security-group-ingress',
          '--region', self.region,
          '--group-id', alb_sg,
          '--protocol', 'tcp',
          '--port', '80',
          '--cidr', '0.0.0.0/0',
      ], raise_on_failure=False)  # ok if rule already exists
      logging.info('[PKB][EKS] Opened 80/tcp on ALB SG %s', alb_sg)
    else:
      logging.warning('[PKB][EKS] Could not resolve ALB SG for %s', address)

    # 2) Collect node security groups for the actual running nodes (robust way).
    #    Read instance IDs from Kubernetes node objects (spec.providerID),
    #    then get SGs from those exact instances.
    ids_out, _, _ = container_service.RunKubectlCommand([
        'get', 'nodes',
        '-o', 'jsonpath={.items[*].spec.providerID}',
    ], raise_on_failure=False)

    provider_ids = (ids_out or '').split()
    instance_ids = []
    for pid in provider_ids:
        # Expected format: aws:///us-east-1a/i-0123456789abcdef0  -> take the last token
        if '/' in pid:
            instance_ids.append(pid.split('/')[-1])

    instance_ids = sorted(set(i for i in instance_ids if i.startswith('i-')))
    if not instance_ids:
        return  # No nodes yet; Karpenter may scale later.

    # Describe those instances and collect unique SG IDs.
    desc_cmd = util.AWS_PREFIX + [
        'ec2', 'describe-instances',
        '--region', self.region,
        '--instance-ids', *instance_ids,
        '--query', 'Reservations[].Instances[].SecurityGroups[].GroupId',
        '--output', 'text',
    ]
    out, _, _ = vm_util.IssueCommand(desc_cmd, raise_on_failure=False)
    node_sgs = set((out or '').split())
    if not node_sgs:
        return

    # 3) Ensure cluster tag and allow ALB->nodes on the app port.
    for sg in sorted(node_sgs):
        # Tag: kubernetes.io/cluster/<name>=owned (safe to repeat).
        vm_util.IssueCommand(util.AWS_PREFIX + [
            'ec2', 'create-tags',
            '--region', self.region,
            '--resources', sg,
            '--tags', f'Key=kubernetes.io/cluster/{self.name},Value=owned',
        ], raise_on_failure=False)

        # Allow traffic from ALB SG to nodes on the application port.
        vm_util.IssueCommand(util.AWS_PREFIX + [
            'ec2', 'authorize-security-group-ingress',
            '--region', self.region,
            '--group-id', sg,
            '--protocol', 'tcp',
            '--port', str(port),
            '--source-group', alb_sg,
        ], raise_on_failure=False)

  def _PostCreate(self):
    """Performs post-creation steps for the cluster."""
    super()._PostCreate()
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
        'controller.resources.requests.cpu=1',
        '--set',
        'controller.resources.requests.memory=1Gi',
        '--set',
        'controller.resources.limits.cpu=1',
        '--set',
        'controller.resources.limits.memory=1Gi',
        '--set',
        'logLevel=debug',
        '--wait',
    ])
    # Ensure ALB ingress support: install AWS Load Balancer Controller.
    self._InstallAwsLoadBalancerController()
    # Get the AMI version for current kubernetes version.
    # See e.g. https://karpenter.sh/docs/tasks/managing-amis/ for not using
    # @latest.
    image_id, _, _ = vm_util.IssueCommand([
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
    alias_version = (
        'v'
        + full_version.strip().strip('"').split(f'{self.cluster_version}-v')[1]
    )
    self.ApplyManifest(
        'container/karpenter/nodepool.yaml.j2',
        CLUSTER_NAME=self.name,
        ALIAS_VERSION=alias_version,
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
    vm_util.IssueCommand([
        'aws',
        'cloudformation',
        'delete-stack',
        '--stack-name',
        self.stack_name,
        '--region',
        f'{self.region}',
    ])

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
    raise NotImplementedError()

  def GetNodeSelectors(self) -> list[str]:
    """Get the node selectors section of a yaml for the provider."""
    return []
