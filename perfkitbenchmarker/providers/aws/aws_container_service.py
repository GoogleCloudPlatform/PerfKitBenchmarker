# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains classes/functions related to AWS container clusters."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import json
import os
import uuid
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_load_balancer
from perfkitbenchmarker.providers.aws import aws_logs
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util
import requests
import six
import yaml

FLAGS = flags.FLAGS
_ECS_NOT_READY = frozenset(['PROVISIONING', 'PENDING'])


class EcrRepository(resource.BaseResource):
  """Class representing an Elastic Container Registry image repository."""

  def __init__(self, name, region):
    super(EcrRepository, self).__init__()
    self.name = name
    self.region = region

  def _Create(self):
    """Creates the image repository."""
    if self._Exists():
      self.user_managed = True
      return
    create_cmd = util.AWS_PREFIX + [
        'ecr', 'create-repository',
        '--region', self.region,
        '--repository-name', self.name
    ]
    vm_util.IssueCommand(create_cmd)

  def _Exists(self):
    """Returns True if the repository exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ecr', 'describe-repositories',
        '--region', self.region,
        '--repository-names', self.name
    ]
    stdout, _, _ = vm_util.IssueCommand(
        describe_cmd, suppress_warning=True, raise_on_failure=False)
    if not stdout or not json.loads(stdout)['repositories']:
      return False
    return True

  def _Delete(self):
    """Deletes the repository."""
    delete_cmd = util.AWS_PREFIX + [
        'ecr', 'delete-repository',
        '--region', self.region,
        '--repository-name', self.name,
        '--force'
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)


class ElasticContainerRegistry(container_service.BaseContainerRegistry):
  """Class for building and storing container images on AWS."""

  CLOUD = providers.AWS

  def __init__(self, registry_spec):
    super(ElasticContainerRegistry, self).__init__(registry_spec)
    self.account = self.project or util.GetAccount()
    self.region = util.GetRegionFromZone(self.zone.split(',')[0])
    self.repositories = []

  def _Delete(self):
    """Deletes the repositories."""
    for repository in self.repositories:
      repository.Delete()

  def Push(self, image):
    """Push a locally built image to the registry."""
    repository_name = '{namespace}/{name}'.format(
        namespace=self.name, name=image.name)
    repository = EcrRepository(repository_name, self.region)
    self.repositories.append(repository)
    repository.Create()
    super(ElasticContainerRegistry, self).Push(image)

  def GetFullRegistryTag(self, image):
    """Gets the full tag of the image."""
    tag = '{account}.dkr.ecr.{region}.amazonaws.com/{namespace}/{name}'.format(
        account=self.account, region=self.region, namespace=self.name,
        name=image)
    return tag

  def Login(self):
    """Logs in to the registry."""
    get_login_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecr', 'get-login', '--no-include-email'
    ]
    stdout, _, _ = vm_util.IssueCommand(get_login_cmd)
    login_cmd = stdout.split()
    vm_util.IssueCommand(login_cmd)

  def RemoteBuild(self, image):
    """Build the image remotely."""
    # TODO(ehankland) use AWS codebuild to build the image.
    raise NotImplementedError()


class TaskDefinition(resource.BaseResource):
  """Class representing an AWS task definition."""

  def __init__(self, name, container_spec, cluster):
    super(TaskDefinition, self).__init__()
    self.name = name
    self.cpus = container_spec.cpus
    self.memory = container_spec.memory
    self.image = container_spec.image
    self.container_port = container_spec.container_port
    self.region = cluster.region
    self.arn = None
    self.log_group = aws_logs.LogGroup(self.region, 'pkb')

  def _CreateDependencies(self):
    """Create the log group if it doesn't exist."""
    if not self.log_group.Exists():
      self.log_group.Create()

  def _Create(self):
    """Create the task definition."""
    register_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'register-task-definition',
        '--family', self.name,
        '--execution-role-arn', 'ecsTaskExecutionRole',
        '--network-mode', 'awsvpc',
        '--requires-compatibilities=FARGATE',
        '--cpu', str(int(1024 * self.cpus)),
        '--memory', str(self.memory),
        '--container-definitions', self._GetContainerDefinitions()
    ]
    stdout, _, _ = vm_util.IssueCommand(register_cmd)
    response = json.loads(stdout)
    self.arn = response['taskDefinition']['taskDefinitionArn']

  def _Delete(self):
    """Deregister the task definition."""
    if self.arn is None:
      return
    deregister_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'deregister-task-definition',
        '--task-definition', self.arn
    ]
    vm_util.IssueCommand(deregister_cmd)

  def _GetContainerDefinitions(self):
    """Returns a JSON representation of the container definitions."""
    definitions = [{
        'name': self.name,
        'image': self.image,
        'essential': True,
        'portMappings': [
            {
                'containerPort': self.container_port,
                'protocol': 'TCP'
            }
        ],
        'logConfiguration': {
            'logDriver': 'awslogs',
            'options': {
                'awslogs-group': 'pkb',
                'awslogs-region': self.region,
                'awslogs-stream-prefix': 'pkb'
            }
        }
    }]
    return json.dumps(definitions)


class EcsTask(container_service.BaseContainer):
  """Class representing an ECS/Fargate task."""

  def __init__(self, name, container_spec, cluster):
    super(EcsTask, self).__init__(container_spec)
    self.name = name
    self.task_def = cluster.task_defs[name]
    self.arn = None
    self.region = cluster.region
    self.cluster_name = cluster.name
    self.subnet_id = cluster.network.subnet.id
    self.ip_address = None
    self.security_group_id = (
        cluster.network.regional_network.vpc.default_security_group_id)

  def _GetNetworkConfig(self):
    network_config = {
        'awsvpcConfiguration': {
            'subnets': [self.subnet_id],
            'securityGroups': [self.security_group_id],
            'assignPublicIp': 'ENABLED',
        }
    }
    return json.dumps(network_config)

  def _GetOverrides(self):
    """Returns a JSON representaion of task overrides.

    While the container level resources can be overridden, they have no
    effect on task level resources for Fargate tasks. This means
    that modifying a container spec will only affect the command of any
    new containers launched from it and not cpu/memory.
    """
    overrides = {
        'containerOverrides': [
            {
                'name': self.name,
            }
        ]
    }
    if self.command:
      overrides['containerOverrides'][0]['command'] = self.command
    return json.dumps(overrides)

  def _Create(self):
    """Creates the task."""
    run_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'run-task',
        '--cluster', self.cluster_name,
        '--task-definition', self.task_def.arn,
        '--launch-type', 'FARGATE',
        '--network-configuration', self._GetNetworkConfig(),
        '--overrides', self._GetOverrides()
    ]
    stdout, _, _ = vm_util.IssueCommand(run_cmd)
    response = json.loads(stdout)
    self.arn = response['tasks'][0]['taskArn']

  def _PostCreate(self):
    """Gets the tasks IP address."""
    container = self._GetTask()['containers'][0]
    self.ip_address = container['networkInterfaces'][0]['privateIpv4Address']

  def _DeleteDependencies(self):
    """Delete the task def."""
    self.task_def.Delete()

  def _Delete(self):
    """Deletes the task."""
    if self.arn is None:
      return
    stop_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'stop-task',
        '--cluster', self.cluster_name,
        '--task', self.arn
    ]
    vm_util.IssueCommand(stop_cmd)

  def _GetTask(self):
    """Returns a dictionary representation of the task."""
    describe_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'describe-tasks',
        '--cluster', self.cluster_name,
        '--tasks', self.arn
    ]
    stdout, _, _ = vm_util.IssueCommand(describe_cmd)
    response = json.loads(stdout)
    return response['tasks'][0]

  def _IsReady(self):
    """Returns true if the task has stopped pending."""
    return self._GetTask()['lastStatus'] not in _ECS_NOT_READY

  def WaitForExit(self, timeout=None):
    """Waits until the task has finished running."""
    @vm_util.Retry(timeout=timeout)
    def _WaitForExit():
      status = self._GetTask()['lastStatus']
      if status != 'STOPPED':
        raise Exception('Task is not STOPPED.')
    _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    task_id = self.arn.split('/')[-1]
    log_stream = 'pkb/{name}/{task_id}'.format(name=self.name, task_id=task_id)
    return six.text_type(
        aws_logs.GetLogStreamAsString(self.region, log_stream, 'pkb'))


class EcsService(container_service.BaseContainerService):
  """Class representing an ECS/Fargate service."""

  def __init__(self, name, container_spec, cluster):
    super(EcsService, self).__init__(container_spec)
    self.client_token = str(uuid.uuid4())[:32]
    self.name = name
    self.task_def = cluster.task_defs[name]
    self.arn = None
    self.region = cluster.region
    self.cluster_name = cluster.name
    self.subnet_id = cluster.network.subnet.id
    self.security_group_id = (
        cluster.network.regional_network.vpc.default_security_group_id)
    self.load_balancer = aws_load_balancer.LoadBalancer([
        cluster.network.subnet])
    self.target_group = aws_load_balancer.TargetGroup(
        cluster.network.regional_network.vpc, self.container_port)
    self.port = 80

  def _CreateDependencies(self):
    """Creates the load balancer for the service."""
    self.load_balancer.Create()
    self.target_group.Create()
    listener = aws_load_balancer.Listener(
        self.load_balancer, self.target_group, self.port)
    listener.Create()
    self.ip_address = self.load_balancer.dns_name

  def _DeleteDependencies(self):
    """Deletes the service's load balancer."""
    self.task_def.Delete()
    self.load_balancer.Delete()
    self.target_group.Delete()

  # TODO(ferneyhough): Consider supporting the flag container_cluster_version.
  def _Create(self):
    """Creates the service."""
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'create-service',
        '--desired-count', '1',
        '--client-token', self.client_token,
        '--cluster', self.cluster_name,
        '--service-name', self.name,
        '--task-definition', self.task_def.arn,
        '--launch-type', 'FARGATE',
        '--network-configuration', self._GetNetworkConfig(),
        '--load-balancers', self._GetLoadBalancerConfig(),
    ]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the service."""
    update_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'update-service',
        '--cluster', self.cluster_name,
        '--service', self.name,
        '--desired-count', '0'
    ]
    vm_util.IssueCommand(update_cmd)
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'delete-service',
        '--cluster', self.cluster_name,
        '--service', self.name
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _GetNetworkConfig(self):
    network_config = {
        'awsvpcConfiguration': {
            'subnets': [self.subnet_id],
            'securityGroups': [self.security_group_id],
            'assignPublicIp': 'ENABLED',
        }
    }
    return json.dumps(network_config)

  def _GetLoadBalancerConfig(self):
    """Returns the JSON representation of the service load balancers."""
    load_balancer_config = [{
        'targetGroupArn': self.target_group.arn,
        'containerName': self.name,
        'containerPort': self.container_port,
    }]
    return json.dumps(load_balancer_config)

  def _IsReady(self):
    """Returns True if the Service is ready."""
    url = 'http://%s' % self.ip_address
    try:
      r = requests.get(url)
    except requests.ConnectionError:
      return False
    if r.status_code == 200:
      return True
    return False


class FargateCluster(container_service.BaseContainerCluster):
  """Class representing an AWS Fargate cluster."""

  CLOUD = providers.AWS
  CLUSTER_TYPE = 'Fargate'

  def __init__(self, cluster_spec):
    super(FargateCluster, self).__init__(cluster_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.network = aws_network.AwsNetwork.GetNetwork(self)
    self.firewall = aws_network.AwsFirewall.GetFirewall()
    self.name = 'pkb-%s' % FLAGS.run_uri
    self.task_defs = {}
    self.arn = None

  def _Create(self):
    """Creates the cluster."""
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'create-cluster',
        '--cluster-name', self.name
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.arn = response['cluster']['clusterArn']

  def _Exists(self):
    """Returns True if the cluster exists."""
    if not self.arn:
      return False
    describe_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'describe-clusters',
        '--clusters', self.arn
    ]
    stdout, _, _ = vm_util.IssueCommand(describe_cmd)
    response = json.loads(stdout)
    clusters = response['clusters']
    if not clusters or clusters[0]['status'] == 'INACTIVE':
      return False
    return True

  def _Delete(self):
    """Deletes the cluster."""
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'ecs', 'delete-cluster',
        '--cluster', self.name
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def DeployContainer(self, name, container_spec):
    """Deploys the container according to the spec."""
    if name not in self.task_defs:
      task_def = TaskDefinition(name, container_spec, self)
      self.task_defs[name] = task_def
      task_def.Create()
    task = EcsTask(name, container_spec, self)
    self.containers[name].append(task)
    task.Create()

  def DeployContainerService(self, name, container_spec):
    """Deploys the container service according to the spec."""
    if name not in self.task_defs:
      task_def = TaskDefinition(name, container_spec, self)
      self.task_defs[name] = task_def
      task_def.Create()
    service = EcsService(name, container_spec, self)
    self.services[name] = service
    self.firewall.AllowPortInSecurityGroup(
        service.region, service.security_group_id, service.container_port)
    service.Create()


class AwsKopsCluster(container_service.KubernetesCluster):
  """Class representing a kops based Kubernetes cluster."""

  CLOUD = providers.AWS
  CLUSTER_TYPE = 'kops'

  def __init__(self, spec):
    super(AwsKopsCluster, self).__init__(spec)
    self.name += '.k8s.local'
    self.config_bucket = 'kops-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4()))
    self.region = util.GetRegionFromZone(self.zone)
    self.s3_service = s3.S3Service()
    self.s3_service.PrepareService(self.region)

  def _CreateDependencies(self):
    """Create the bucket to store cluster config."""
    self.s3_service.MakeBucket(self.config_bucket)

  def _DeleteDependencies(self):
    """Delete the bucket that stores cluster config."""
    self.s3_service.DeleteBucket(self.config_bucket)

  def _Create(self):
    """Creates the cluster."""
    # Create the cluster spec but don't provision any resources.
    create_cmd = [
        FLAGS.kops, 'create', 'cluster',
        '--name=%s' % self.name,
        '--zones=%s' % self.zone,
        '--node-count=%s' % self.num_nodes,
        '--node-size=%s' % self.machine_type
    ]
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    env['KOPS_STATE_STORE'] = 's3://%s' % self.config_bucket
    vm_util.IssueCommand(create_cmd, env=env)

    # Download the cluster spec and modify it.
    get_cmd = [
        FLAGS.kops, 'get', 'cluster', self.name, '--output=yaml'
    ]
    stdout, _, _ = vm_util.IssueCommand(get_cmd, env=env)
    spec = yaml.load(stdout)
    spec['metadata']['creationTimestamp'] = None
    spec['spec']['api']['loadBalancer']['idleTimeoutSeconds'] = 3600
    benchmark_spec = context.GetThreadBenchmarkSpec()
    spec['spec']['cloudLabels'] = {
        'owner': FLAGS.owner,
        'perfkitbenchmarker-run': FLAGS.run_uri,
        'benchmark': benchmark_spec.name,
        'perfkit_uuid': benchmark_spec.uuid,
        'benchmark_uid': benchmark_spec.uid
    }

    # Replace the cluster spec.
    with vm_util.NamedTemporaryFile() as tf:
      yaml.dump(spec, tf)
      tf.close()
      replace_cmd = [
          FLAGS.kops, 'replace', '--filename=%s' % tf.name
      ]
      vm_util.IssueCommand(replace_cmd, env=env)

    # Create the actual cluster.
    update_cmd = [
        FLAGS.kops, 'update', 'cluster', self.name, '--yes'
    ]
    vm_util.IssueCommand(update_cmd, env=env)

  def _Delete(self):
    """Deletes the cluster."""
    delete_cmd = [
        FLAGS.kops, 'delete', 'cluster',
        '--name=%s' % self.name,
        '--state=s3://%s' % self.config_bucket,
        '--yes'
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _IsReady(self):
    """Returns True if the cluster is ready, else False."""
    validate_cmd = [
        FLAGS.kops, 'validate', 'cluster',
        '--name=%s' % self.name,
        '--state=s3://%s' % self.config_bucket
    ]
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    _, _, retcode = vm_util.IssueCommand(validate_cmd, env=env,
                                         suppress_warning=True,
                                         raise_on_failure=False)
    return not retcode
