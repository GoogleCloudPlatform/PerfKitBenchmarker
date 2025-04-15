"""Class to represent a AWS cluster."""

import json
import os

from absl import flags as absl_flags
import jinja2
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import cluster
from perfkitbenchmarker import data
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import flags
from perfkitbenchmarker.providers.aws import util


FLAGS = absl_flags.FLAGS


class AWSClusterSpec(cluster.BaseClusterSpec):
  """Class to represent a AWS cluster."""

  CLOUD = provider_info.AWS


class AWSCluster(cluster.BaseCluster):
  """Class to represent a AWS cluster."""

  CLOUD = provider_info.AWS
  DEFAULT_TEMPLATE = 'cluster/parallel_cluster.yaml.j2'

  def __init__(self, cluster_spec: AWSClusterSpec):
    super().__init__(cluster_spec)
    self._pub_key, _, _ = vm_util.IssueCommand(
        ['cat', vm_util.GetPublicKeyPath()]
    )
    self._key_manager = aws_virtual_machine.AwsKeyFileManager()
    # Headnode needs a public IP
    self._headnode_subnet_id = None
    self._worker_subnet_id = None
    self.region = util.GetRegionFromZone(self.zone)
    self._config_path = os.path.join(vm_util.GetTempDir(), self.name + '.yaml')
    self._network_stack = None
    self._vpc: aws_network.AwsVpc = None
    self.nfs_path = '/opt/apps'

  def _CreateDependencies(self):
    """Create dependencies for parallel cluster.

    Dependencies includes key files, vpcs and cloudformation template.
    """
    # Create keypair, VPC & Subnet
    self._key_manager.ImportKeyfile(self.region)
    # Use pcluster to create and configure vpc & subnet
    # Headnode needs a public IP to be accessible and requires a public subnet.
    # Workers having more than 1 network interface needs to sit inside
    # private subnet and connect to public subnet with NAT.
    # TODO(yuyanting): Consider replicating with low level ec2 commands.
    # subnet_config_path points to a generic configuration that is only used
    # to create dependencies such as VPC and subnets.
    subnet_config_path = os.path.join(vm_util.GetTempDir(), 'subnet')
    stdout, _, _ = vm_util.IssueCommand([
        'bash',
        '-c',
        f'printf "{self.region}\n{self._key_manager.GetKeyNameForRun()}\n'
        # The follownig does not matter as this step only creates the network.
        'slurm\nalinux2\nt2.micro\n1\nqueue1\n1\nt2.micro\n1\ny\n'
        f'{self.zone}\n1\n" '
        f'| pcluster configure --config {subnet_config_path}',
    ])
    # Parse stack name: aws cloudformation delete-stack help
    self._network_stack = regex_util.ExtractAllMatches(
        r'Status: (parallelclusternetworking-pubpriv-\d+) ', stdout
    )[0]
    suffix = self._network_stack.split('-')[-1]
    stdout, _, _ = vm_util.IssueCommand([
        'aws',
        'ec2',
        'describe-vpcs',
        '--filters',
        f'Name=tag:Name,Values=ParallelClusterVPC-{suffix}',
        '--region',
        self.region,
    ])
    resources_to_tag = []
    vpc_id = json.loads(stdout)['Vpcs'][0]['VpcId']
    self._vpc = aws_network.AwsVpc(self.region, vpc_id)
    resources_to_tag.append(vpc_id)
    stdout, _, _ = vm_util.IssueCommand(['cat', subnet_config_path])
    subnet_ids = regex_util.ExtractAllMatches(r'(subnet-\w+)', stdout)
    self._headnode_subnet_id = subnet_ids[0]
    self._worker_subnet_id = subnet_ids[1]
    resources_to_tag.extend(subnet_ids)
    resources_to_tag.append(
        json.loads(
            vm_util.IssueCommand([
                'aws',
                'ec2',
                'describe-internet-gateways',
                '--filters',
                f'Name=attachment.vpc-id,Values={vpc_id}',
                '--region',
                self.region,
            ])[0]
        )['InternetGateways'][0]['InternetGatewayId']
    )
    route_tables = json.loads(
        vm_util.IssueCommand([
            'aws',
            'ec2',
            'describe-route-tables',
            '--filters',
            f'Name=vpc-id,Values={vpc_id}',
            '--region',
            self.region,
        ])[0]
    )['RouteTables']
    for table in route_tables:
      if not table['Associations'][0]['Main']:
        resources_to_tag.append(table['RouteTableId'])
    nat = json.loads(
        vm_util.IssueCommand([
            'aws',
            'ec2',
            'describe-nat-gateways',
            '--filter',
            f'Name=vpc-id,Values={vpc_id}',
            '--region',
            self.region,
        ])[0]
    )['NatGateways'][0]
    resources_to_tag.append(nat['NatGatewayId'])
    resources_to_tag.append(nat['NatGatewayAddresses'][0]['NetworkInterfaceId'])
    for resource_to_tag in resources_to_tag:
      util.AddDefaultTags(resource_to_tag, self.region)
    self._RenderClusterConfig()

  def _DeleteDependencies(self):
    """Deletes vpc and cloudformation template."""
    self._key_manager.DeleteKeyfile(self.region)
    vm_util.IssueCommand([
        'aws',
        'cloudformation',
        'delete-stack',
        '--stack-name',
        self._network_stack,
        '--region',
        self.region,
    ])
    # VPC is created outside of PKB scope as part of pcluster intiailization.
    # But the tool does not take care of vpc cleanup.
    self._vpc._Delete()  # pylint: disable=protected-access

  def _RenderClusterConfig(self):
    """Render the config file that will be used to create the cluster."""
    tags = util.MakeDefaultTags(FLAGS.timeout_minutes)
    with open(data.ResourcePath(self.template)) as content:
      template = jinja2.Template(
          content.read(), undefined=jinja2.StrictUndefined
      )
      self._config = template.render(
          name=self.name,
          os_type=self.os_type.replace('amazonlinux', 'alinux'),
          region=self.region,
          num_workers=self.num_workers,
          worker_machine_type=self.worker_machine_type,
          headnode_machine_type=self.headnode_machine_type,
          headnode_subnet_id=self._headnode_subnet_id,
          worker_subnet_id=self._worker_subnet_id,
          ssh_key=self._key_manager.GetKeyNameForRun(),
          tags=tags,
          # boot disk of headnode is also mounted as NFS
          nfs_size=self.headnode_spec.boot_disk_size,
          efa_enabled=FLAGS.aws_efa,
          enable_spot_vm=FLAGS.aws_spot_instances,
          # Expose enable_smt flag for consistency across clouds.
          enable_smt=not FLAGS.disable_smt,
      )

  def _Create(self):
    with open(self._config_path, 'w') as config_file:
      config_file.write(self._config)
    vm_util.IssueCommand([
        flags.PCLUSTER_PATH.value,
        'create-cluster',
        '--cluster-configuration',
        # Create actual cluster with rendered configuration.
        self._config_path,
        '--cluster-name',
        self.name,
    ])

  @vm_util.Retry(
      poll_interval=1,
      log_errors=False,
      retryable_exceptions=(aws_virtual_machine.AwsUnknownStatusError,),
  )
  def _WaitUntilRunning(self):
    """Waits until the cluster is (or was) running."""
    stdout, _, _ = vm_util.IssueCommand([
        flags.PCLUSTER_PATH.value,
        'describe-cluster',
        '--cluster-name',
        self.name,
        '--region',
        self.region,
    ])
    status = json.loads(stdout)['clusterStatus']
    if status != 'CREATE_COMPLETE':
      raise aws_virtual_machine.AwsUnknownStatusError(
          f'Unknown status: {status}; retrying describe-instances command'
      )

  def _PostCreate(self):
    """Backfill VM object after cluster creation."""
    self.worker_vms = []
    stdout, _, _ = vm_util.IssueCommand([
        flags.PCLUSTER_PATH.value,
        'describe-cluster-instances',
        '--cluster-name',
        self.name,
        '--region',
        self.region,
    ])

    def _PopulateVM(vm, instance):
      """Extract VM id/ips."""
      vm.id = instance.get('instanceId')
      vm.created = True
      vm.ip_address = instance.get('publicIpAddress', None)
      vm.internal_ips = [instance.get('privateIpAddress')]
      vm.internal_ip = instance.get('privateIpAddress')

    # AWS VM class does a lot more than just parsing describe output
    # and backfill ip addresses.
    # E.g.
    # - installing & configuring EFA (not needed if created with pcluster)
    # - wait & configure public IP (not available for workers)

    # Create headnode object to avoid race condition
    for instance in json.loads(stdout)['instances']:
      if instance['nodeType'] == 'HeadNode':
        self.headnode_vm = self.InstantiateVm(self.headnode_spec)
        _PopulateVM(self.headnode_vm, instance)

    for instance in json.loads(stdout)['instances']:
      if instance['nodeType'] == 'HeadNode':
        continue
      vm = self.InstantiateVm(self.workers_spec)
      self.worker_vms.append(vm)
      _PopulateVM(vm, instance)
      vm.ip_address = vm.internal_ips[0]
      vm.proxy_jump = self.headnode_vm.name
    self.vms = [self.headnode_vm] + self.worker_vms
    self.headnode_vm.network.regional_network._reference_count += 1

    vm_util.GenerateSSHConfig(
        self.vms, {'headnode': [self.headnode_vm], 'worker': self.worker_vms}
    )

    def _SetupEnvironment(vm):
      vm.WaitForBootCompletion()
      vm.RemoteCommand(
          f'sudo ln -s {self.nfs_path} {linux_packages.INSTALL_DIR}'
      )
      vm.PrepareVMEnvironment()

    # Since worker uses headnode as proxy, setup headnode to avoid potential
    # race condition.
    _SetupEnvironment(self.headnode_vm)
    background_tasks.RunThreaded(_SetupEnvironment, self.worker_vms)

  def _Delete(self):
    vm_util.IssueCommand([
        flags.PCLUSTER_PATH.value,
        'delete-cluster',
        '--cluster-name',
        self.name,
        '--region',
        self.region,
    ])

  def AuthenticateVM(self):
    """Authenticate a remote machine to access all vms."""
    # Already taken care of by pcluster
    pass
