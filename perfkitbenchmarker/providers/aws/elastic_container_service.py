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

"""Contains classes/functions related to EKS (Elastic Container Service).

This requires that the eksServiceRole IAM role has already been created and
requires that the heptio-authenticator-aws binary has been installed.
See https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html for
instructions.
"""

import json
import re
import uuid
from perfkitbenchmarker import container_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

_AWS_KUBECONFIG = """
apiVersion: v1
clusters:
- cluster:
    server: {endpoint}
    certificate-authority-data: {ca_cert}
  name: kubernetes
contexts:
- context:
    cluster: kubernetes
    user: aws
  name: aws
current-context: aws
kind: Config
preferences: {{}}
users:
- name: aws
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1alpha1
      command: heptio-authenticator-aws
      args:
        - "token"
        - "-i"
        - "{name}"
"""
_CONFIG_MAP = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: aws-auth
  namespace: kube-system
data:
  mapRoles: |
    - rolearn: {node_instance_role}
      username: system:node:{{{{EC2PrivateDNSName}}}}
      groups:
        - system:bootstrappers
        - system:nodes
"""
_EKS_WORKERS_TEMPLATE = ('https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/'
                         '2018-06-05/amazon-eks-nodegroup.yaml')


class _EksControlPlane(resource.BaseResource):
  """Class representing the control plane portion of an EKS cluster."""

  def __init__(self, name, region, networks):
    super(_EksControlPlane, self).__init__()
    self.name = name
    self.region = region
    self.request_token = str(uuid.uuid4())
    self.networks = networks
    self.cluster_version = FLAGS.container_cluster_version
    self.endpoint = None
    self.ca_cert = None

  def _GetVpcConfig(self):
    """Returns the formatted vpc config for the cluster."""
    subnet_ids = [net.subnet.id for net in self.networks]
    security_group_id = (self.networks[0].regional_network.vpc.
                         default_security_group_id)
    vpc_config = {
        'subnetIds': subnet_ids,
        'securityGroupIds': [security_group_id]
    }
    return json.dumps(vpc_config)

  def _Create(self):
    """Creates the EKS control plane."""
    role_arn = 'arn:aws:iam::%s:role/eksServiceRole' % util.GetAccount()
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'eks', 'create-cluster',
        '--name', self.name,
        '--role-arn', role_arn,
        '--resources-vpc-config', self._GetVpcConfig(),
        '--client-request-token', self.request_token,
    ]
    if self.cluster_version:
      create_cmd.extend(['--kubernetes-version', self.cluster_version])
    if not FLAGS.eks_verify_ssl:
      create_cmd.append('--no-verify-ssl')
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the EKS control plane."""
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'eks', 'delete-cluster',
        '--name', self.name,
    ]
    if not FLAGS.eks_verify_ssl:
      delete_cmd.append('--no-verify-ssl')
    vm_util.IssueCommand(delete_cmd)

  def _IsReady(self):
    """Returns True if the control plane is ready, else False."""
    describe_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'eks', 'describe-cluster',
        '--name', self.name,
    ]
    if not FLAGS.eks_verify_ssl:
      describe_cmd.append('--no-verify-ssl')
    stdout, _, _ = vm_util.IssueCommand(describe_cmd)
    response = json.loads(stdout)
    ready = response['cluster']['status'] != 'CREATING'
    if ready:
      self.endpoint = response['cluster']['endpoint']
      self.ca_cert = response['cluster']['certificateAuthority']['data']
    return ready

  def _PostCreate(self):
    """Sets up the kubeconfig for the cluster."""
    kubeconfig = _AWS_KUBECONFIG.format(
        endpoint=self.endpoint, ca_cert=self.ca_cert, name=self.name)
    with open(FLAGS.kubeconfig, 'w') as f:
      f.write(kubeconfig)


class _EksWorkers(resource.BaseResource):
  """Class representing the nodes in an EKS cluster."""

  def __init__(self, name, region):
    super(_EksWorkers, self).__init__()
    self.create_token = str(uuid.uuid4())
    self.delete_token = str(uuid.uuid4())
    self.name = name
    self.region = region
    self.node_instance_role = None
    self.parameters = {}

  def _Create(self):
    """Creates the worker nodes."""
    parameter_list = [{'ParameterKey': k, 'ParameterValue': str(v)}
                      for k, v in self.parameters.items()]
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'cloudformation', 'create-stack',
        '--stack-name', self.name,
        '--template-url', _EKS_WORKERS_TEMPLATE,
        '--client-request-token', self.create_token,
        '--parameters', json.dumps(parameter_list),
        '--capabilities', 'CAPABILITY_IAM',
    ]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the worker nodes."""
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'cloudformation', 'delete-stack',
        '--stack-name', self.name,
        '--client-request-token', self.delete_token,
    ]
    vm_util.IssueCommand(delete_cmd)

  def _IsReady(self):
    """Returns True if the stack has finished creating, else False."""
    wait_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'cloudformation', 'wait', 'stack-create-complete',
        '--stack-name', self.name,
    ]
    _, _, retcode = vm_util.IssueCommand(wait_cmd)
    return retcode == 0

  def _IsDeleting(self):
    """Returns False when the workers are done deleting."""
    wait_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'cloudformation', 'wait', 'stack-delete-complete',
        '--stack-name', self.name,
    ]
    vm_util.IssueCommand(wait_cmd)
    return False

  def _PostCreate(self):
    """Gets the node instance role from the stack."""
    describe_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'cloudformation', 'describe-stacks',
        '--stack-name', self.name,
    ]
    stdout, _, _ = vm_util.IssueCommand(describe_cmd)
    response = json.loads(stdout)
    output = response['Stacks'][0]['Outputs'][0]
    assert output['OutputKey'] == 'NodeInstanceRole', 'Unexpected output key.'
    self.node_instance_role = output['OutputValue']


class EksCluster(container_service.KubernetesCluster):
  """Class representing an Elastic Container Service cluster."""

  CLOUD = providers.AWS

  def __init__(self, spec):
    super(EksCluster, self).__init__(spec)
    self.zones = sorted(FLAGS.eks_zones)
    self.zone = ','.join(self.zones)
    self.region = util.GetRegionFromZone(self.zones[0])

    self.networks = [
        aws_network.AwsNetwork.GetNetworkFromNetworkSpec(
            network.BaseNetworkSpec(zone))
        for zone in self.zones
    ]
    self.eks_control_plane = _EksControlPlane(
        self.name, self.region, self.networks)
    self.eks_workers = _EksWorkers(
        '%s-worker-nodes' % self.name, self.region)

  def _CreateDependencies(self):
    """Set up the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _DeleteDependencies(self):
    """Delete the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _Create(self):
    """Creates the control plane and worker nodes."""
    self.eks_control_plane.Create()
    vpc = self.networks[0].regional_network.vpc
    self.eks_workers.parameters = {
        'ClusterName': self.name,
        'ClusterControlPlaneSecurityGroup': vpc.default_security_group_id,
        'NodeGroupName': 'eks',
        'NodeAutoScalingGroupMinSize': self.min_nodes,
        'NodeAutoScalingGroupMaxSize': self.max_nodes,
        'NodeInstanceType': self.machine_type,
        'NodeImageId': self._GetImageId(),
        'KeyName': aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun(),
        'VpcId': vpc.id,
        'Subnets': self.networks[0].subnet.id,
    }
    self.eks_workers.Create()
    self._AuthorizeNodes()

  def _Delete(self):
    """Deletes the control plane and worker nodes."""
    self.eks_control_plane.Delete()
    self.eks_workers.Delete()

  def _GetImageId(self):
    """Gets the current EKS worker image for the region."""
    describe_cmd = util.AWS_PREFIX + [
        '--region=%s' % self.region,
        'ec2',
        'describe-images',
        '--query', 'Images[*].{Name:Name,ImageId:ImageId}',
        '--filters',
        'Name=name,Values=eks-worker-v*',
    ]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    return max(json.loads(stdout), key=lambda image: image['Name'])['ImageId']

  def _AuthorizeNodes(self):
    """Allow the nodes to be added to the cluster."""
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(_CONFIG_MAP.format(
          node_instance_role=self.eks_workers.node_instance_role))
      tf.close()
      apply_cmd = [
          FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
          'apply', '-f', tf.name,
      ]
      vm_util.IssueCommand(apply_cmd)

  def _IsReady(self):
    """Returns True if the workers are ready, else False."""
    get_cmd = [
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
        'get', 'nodes',
    ]
    stdout, _, _ = vm_util.IssueCommand(get_cmd)
    ready_nodes = len(re.findall('Ready', stdout))
    return ready_nodes >= self.min_nodes
