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
"""Contains classes/functions related to GKE (Google Kubernetes Engine)."""

import json
import logging
import math
import os
import re
from typing import Any

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

_CONTAINER_REMOTE_BUILD_CONFIG = flags.DEFINE_string(
    'container_remote_build_config',
    None,
    'The YAML or JSON file to use as the build configuration file.',
)

NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT = 'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded.yaml'
NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET = (
    'nvidia_unrestricted_permissions_daemonset.yml'
)
SERVICE_ACCOUNT_PATTERN = r'.*((?<!iam)|{project}.iam).gserviceaccount.com'
RELEASE_CHANNELS = ['rapid', 'regular', 'stable']
ONE_HOUR = 60 * 60


def _CalculateCidrSize(nodes: int) -> int:
  # Defaults are used for pod and services CIDR ranges:
  # https://cloud.google.com/kubernetes-engine/docs/concepts/alias-ips#cluster_sizing_secondary_range_svcs)
  # Each node requires a /24 CIDR range for pods
  # The cluster requires a /20 CIDR range for services
  # So 2^(32 - nodes) - 2^(32 - 20) >= 2^(32 - 24) * CIDR
  # OR CIDR <= 32 - log2(2^8 * nodes + 2^12)
  cidr_size = int(32 - math.log2((nodes << 8) + (1 << 12)))
  # /19 is narrowest CIDR range GKE supports
  return min(cidr_size, 19)


class GoogleArtifactRegistry(container_service.BaseContainerRegistry):
  """Class for building/storing container images on GCP w/ Artifact Registry."""

  CLOUD = provider_info.GCP

  def __init__(self, registry_spec):
    super().__init__(registry_spec)
    self.project = self.project or util.GetDefaultProject()
    self.region = util.GetRegionFromZone(self.zone)
    # Remove from gcloud commands
    self.zone = None
    self.endpoint = f'{self.region}-docker.pkg.dev'

  def GetFullRegistryTag(self, image: str) -> str:
    """Gets the full tag of the image."""
    project = self.project.replace(':', '/')
    full_tag = f'{self.endpoint}/{project}/{self.name}/{image}'
    return full_tag

  def Login(self):
    """Configures docker to be able to push to remote repo."""
    util.GcloudCommand(self, 'auth', 'configure-docker', self.endpoint).Issue()

  def _Create(self):
    util.GcloudCommand(
        self,
        'artifacts',
        'repositories',
        'create',
        self.name,
        '--repository-format=docker',
        f'--location={self.region}',
    ).Issue()

  def _Delete(self):
    util.GcloudCommand(
        self,
        'artifacts',
        'repositories',
        'delete',
        self.name,
        f'--location={self.region}',
    ).Issue()

  def RemoteBuild(self, image: container_service.ContainerImage):
    """Builds the image remotely."""
    if not _CONTAINER_REMOTE_BUILD_CONFIG.value:
      full_tag = self.GetFullRegistryTag(image.name)
    else:
      full_tag = _CONTAINER_REMOTE_BUILD_CONFIG.value
    build_cmd = util.GcloudCommand(
        self, 'builds', 'submit', '--tag', full_tag, image.directory
    )
    build_cmd.Issue(timeout=None)


class BaseGkeCluster(container_service.KubernetesCluster):
  """Base class for regular & Autopilot GKE clusters."""

  def __init__(self, spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(spec)
    self.project: str = spec.vm_spec.project or FLAGS.project
    self.cluster_version: str = FLAGS.container_cluster_version
    self.use_application_default_credentials: bool = True
    self.region: str

  def _GcloudCommand(self, *args, **kwargs) -> util.GcloudCommand:
    """Creates a gcloud command."""
    return util.GcloudCommand(self, *args, **kwargs)

  def _RunClusterCreateCommand(self, cmd: util.GcloudCommand):
    """Adds flags to the cluster create command and runs it."""
    if self.cluster_version:
      if self.cluster_version in RELEASE_CHANNELS:
        if FLAGS.gke_enable_alpha:
          raise errors.Config.InvalidValue(
              'Kubernetes Alpha is not compatible with release channels'
          )
        cmd.flags['release-channel'] = self.cluster_version
      else:
        cmd.flags['cluster-version'] = self.cluster_version
    if FLAGS.gke_enable_alpha:
      cmd.args.append('--enable-kubernetes-alpha')
      cmd.args.append('--no-enable-autorepair')

    user = util.GetDefaultUser()
    if FLAGS.gcp_service_account:
      cmd.flags['service-account'] = FLAGS.gcp_service_account
    # Matches service accounts that either definitely belongs to this project or
    # are a GCP managed service account like the GCE default service account,
    # which we can't tell to which project they belong.
    elif re.match(SERVICE_ACCOUNT_PATTERN, user):
      logging.info(
          'Re-using configured service-account for GKE Cluster: %s', user
      )
      cmd.flags['service-account'] = user
      self.use_application_default_credentials = False
    else:
      logging.info('Using default GCE service account for GKE cluster')
      cmd.flags['scopes'] = 'cloud-platform'

    self._IssueResourceCreationCommand(cmd)

  def _IssueResourceCreationCommand(self, cmd: util.GcloudCommand):
    """Issues a command to gcloud to create resources."""

    # This command needs a long timeout due to the many minutes it
    # can take to provision a large GPU-accelerated GKE cluster.
    _, stderr, retcode = cmd.Issue(timeout=ONE_HOUR, raise_on_failure=False)
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  def _PostCreate(self):
    """Acquires cluster authentication."""
    super()._PostCreate()
    cmd = self._GcloudCommand(
        'container', 'clusters', 'get-credentials', self.name
    )
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    cmd.IssueRetryable(env=env)

    # GKE does not wait for kube-dns by default
    logging.info('Waiting for kube-dns')
    self.WaitForResource(
        'deployment/kube-dns',
        condition_name='Available',
        namespace='kube-system',
    )

  def _IsDeleting(self):
    cmd = self._GcloudCommand('container', 'clusters', 'describe', self.name)
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    return True if stdout else False

  def _Delete(self):
    """Deletes the cluster."""
    super()._Delete()
    cmd = self._GcloudCommand('container', 'clusters', 'delete', self.name)
    cmd.args.append('--async')
    _, err, _ = cmd.Issue(raise_on_failure=False)
    if 'Please wait and try again' in err:
      raise errors.Resource.RetryableDeletionError(err)

  def _Exists(self):
    """Returns True if the cluster exits."""
    cmd = self._GcloudCommand('container', 'clusters', 'describe', self.name)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return retcode == 0

  def GetDefaultStorageClass(self) -> str:
    """Gets the default storage class for the provider."""
    # https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/gce-pd-csi-driver
    # PD-SSD
    return 'premium-rwo'


class GkeCluster(BaseGkeCluster):
  """Class representing a Google Kubernetes Engine cluster."""

  CLOUD = provider_info.GCP

  def __init__(self, spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(spec)
    self.zones = (
        self.default_nodepool.zone and self.default_nodepool.zone.split(',')
    )
    if not self.zones:
      raise errors.Config.MissingOption(
          'container_cluster.vm_spec.GCP.zone is required.'
      )
    elif len(self.zones) == 1 and util.IsRegion(self.default_nodepool.zone):
      self.region = self.default_nodepool.zone
      self.zones = []
      logging.info(
          "Interpreting zone '%s' as a region", self.default_nodepool.zone
      )
    else:
      self.region = util.GetRegionFromZone(self.zones[0])
    # Update the environment for gcloud commands:
    if gcp_flags.GKE_API_OVERRIDE.value:
      os.environ['CLOUDSDK_API_ENDPOINT_OVERRIDES_CONTAINER'] = (
          gcp_flags.GKE_API_OVERRIDE.value
      )

    self.enable_nccl_fast_socket = False
    if gcp_flags.GKE_NCCL_FAST_SOCKET.value:
      if self.nodepools:
        self.enable_nccl_fast_socket = True
      else:
        raise errors.Config.InvalidValue(
            'NCCL fast socket is only supported on secondary node pools.'
        )

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    nodepool_config.disk_type = vm_config.boot_disk.boot_disk_type
    nodepool_config.disk_size = vm_config.boot_disk.boot_disk_size
    nodepool_config.max_local_disks = vm_config.max_local_disks
    nodepool_config.gpu_type = vm_config.gpu_type
    nodepool_config.gpu_count = vm_config.gpu_count
    nodepool_config.threads_per_core = vm_config.threads_per_core
    nodepool_config.gce_tags = vm_config.gce_tags
    nodepool_config.min_cpu_platform = vm_config.min_cpu_platform
    nodepool_config.network = vm_config.network
    nodepool_config.cpus: int = vm_config.cpus
    nodepool_config.memory_mib: int = vm_config.memory_mib

  def _GcloudCommand(self, *args, **kwargs) -> util.GcloudCommand:
    """Fix zone and region."""
    cmd = super()._GcloudCommand(*args, **kwargs)
    if len(self.zones) != 1:
      del cmd.flags['zone']
      cmd.flags['region'] = self.region
    return cmd

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super().GetResourceMetadata()
    result['project'] = self.project
    if self.cluster_version in RELEASE_CHANNELS:
      result['gke_release_channel'] = self.cluster_version

    result['boot_disk_type'] = self.default_nodepool.disk_type
    result['boot_disk_size'] = self.default_nodepool.disk_size
    if self.default_nodepool.max_local_disks:
      result['gce_local_ssd_count'] = self.default_nodepool.max_local_disks
      # TODO(pclay): support NVME when it leaves alpha
      # Also consider moving FLAGS.gce_ssd_interface into the vm_spec.
      result['gce_local_ssd_interface'] = gce_virtual_machine.SCSI
    result['gke_nccl_fast_socket'] = self.enable_nccl_fast_socket
    if 'nccl' in self.nodepools:
      result['gpu_type'] = self.nodepools['nccl'].gpu_type
      result['gpu_count'] = self.nodepools['nccl'].gpu_count
    return result

  def _Create(self):
    """Creates the cluster."""
    cmd = self._GcloudCommand('container', 'clusters', 'create', self.name)
    if self.default_nodepool.network:
      cmd.flags['network'] = self.default_nodepool.network.network_resource.name

    cmd.args.append('--no-enable-shielded-nodes')
    self._AddNodeParamsToCmd(
        self.default_nodepool,
        cmd,
    )
    if (
        self.min_nodes != self.default_nodepool.num_nodes
        or self.max_nodes != self.default_nodepool.num_nodes
    ):
      cmd.args.append('--enable-autoscaling')
      cmd.flags['max-nodes'] = self.max_nodes
      cmd.flags['min-nodes'] = self.min_nodes
    cmd.flags['cluster-ipv4-cidr'] = f'/{_CalculateCidrSize(self.max_nodes)}'
    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()

    self._RunClusterCreateCommand(cmd)
    self._CreateNodePools()

  def _CreateNodePools(self):
    """Creates additional nodepools for the cluster, if applicable."""
    for name, nodepool in self.nodepools.items():
      cmd = self._GcloudCommand(
          'container', 'node-pools', 'create', name, '--cluster', self.name
      )
      self._AddNodeParamsToCmd(
          nodepool,
          cmd,
      )
      self._IssueResourceCreationCommand(cmd)

  def _AddNodeParamsToCmd(
      self,
      nodepool_config: container_service.BaseNodePoolConfig,
      cmd: util.GcloudCommand,
  ):
    """Modifies cmd to include node specific command arguments."""
    # Apply labels to all nodepools.
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    # Allow a long timeout due to the many minutes it can take to provision a
    # large GPU-accelerated GKE cluster.
    # Parameter is not documented well but is available in CLI.
    cmd.flags['timeout'] = ONE_HOUR

    if nodepool_config.gpu_count:
      if 'a2-' not in nodepool_config.machine_type:
        cmd.flags['accelerator'] = (
            gce_virtual_machine.GenerateAcceleratorSpecString(
                nodepool_config.gpu_type, nodepool_config.gpu_count
            )
        )

    gce_tags = FLAGS.gce_tags
    if nodepool_config.gce_tags:
      gce_tags = nodepool_config.gce_tags
    if gce_tags:
      cmd.flags['tags'] = ','.join(gce_tags)
    if nodepool_config.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = nodepool_config.min_cpu_platform

    if nodepool_config.threads_per_core:
      # TODO(user): Remove when threads-per-core is available in GA
      cmd.use_alpha_gcloud = True
      cmd.flags['threads-per-core'] = nodepool_config.threads_per_core

    if nodepool_config.disk_size:
      cmd.flags['disk-size'] = nodepool_config.disk_size
    if nodepool_config.disk_type:
      cmd.flags['disk-type'] = nodepool_config.disk_type
    if nodepool_config.max_local_disks:
      # TODO(pclay): Switch to local-ssd-volumes which support NVME when it
      # leaves alpha. See
      # https://cloud.google.com/sdk/gcloud/reference/alpha/container/clusters/create
      cmd.flags['local-ssd-count'] = nodepool_config.max_local_disks

    cmd.flags['num-nodes'] = nodepool_config.num_nodes
    # zone may be split a comma separated list
    if nodepool_config.zone:
      cmd.flags['node-locations'] = nodepool_config.zone

    if nodepool_config.machine_type is None:
      cmd.flags['machine-type'] = 'custom-{}-{}'.format(
          nodepool_config.cpus, nodepool_config.memory_mib
      )
    else:
      cmd.flags['machine-type'] = nodepool_config.machine_type

    if FLAGS.gke_enable_gvnic:
      cmd.args.append('--enable-gvnic')
    else:
      cmd.args.append('--no-enable-gvnic')
    if (
        self.enable_nccl_fast_socket
        and nodepool_config.name != container_service.DEFAULT_NODEPOOL
    ):
      cmd.args.append('--enable-fast-socket')

    if FLAGS.gke_node_system_config is not None:
      cmd.flags['system-config-from-file'] = FLAGS.gke_node_system_config

    if nodepool_config.sandbox_config is not None:
      cmd.flags['sandbox'] = nodepool_config.sandbox_config.ToSandboxFlag()

    # If using a fixed version (or the default) do not enable upgrades.
    if self.cluster_version not in RELEASE_CHANNELS:
      cmd.args.append('--no-enable-autoupgrade')

    cmd.flags['node-labels'] = f'pkb_nodepool={nodepool_config.name}'

  def _PostCreate(self):
    """Installs nvidia drivers if needed."""
    super()._PostCreate()

    should_install_nvidia_drivers = self.default_nodepool.gpu_count or any(
        pool.gpu_count for pool in self.nodepools.values()
    )
    if should_install_nvidia_drivers:
      kubernetes_helper.CreateFromFile(NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT)
      kubernetes_helper.CreateFromFile(
          data.ResourcePath(NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET)
      )

  def _GetInstanceGroups(self):
    cmd = self._GcloudCommand('container', 'node-pools', 'list')
    cmd.flags['cluster'] = self.name
    stdout, _, _ = cmd.Issue()
    json_output = json.loads(stdout)
    instance_groups = []
    for node_pool in json_output:
      for group_url in node_pool['instanceGroupUrls']:
        instance_groups.append(group_url.split('/')[-1])  # last url part
    return instance_groups

  def LabelDisks(self):
    """Sets common labels on PVCs.

    GKE does this in the background every hour. Do it immediately in case the
    cluster is deleted within that hour.
    https://cloud.google.com/kubernetes-engine/docs/how-to/creating-managing-labels#label_propagation
    """
    pvcs = self._GetPvcs()
    for pvc in pvcs:
      gce_disk.AddLabels(self, pvc['spec']['volumeName'])

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_service.DEFAULT_NODEPOOL
  ):
    """Changes the number of nodes in the node pool."""
    cmd = self._GcloudCommand('container', 'clusters', 'resize', self.name)
    cmd.flags['num-nodes'] = new_size
    # updates default node pool by default
    if node_pool != container_service.DEFAULT_NODEPOOL:
      cmd.flags['node-pool'] = node_pool
    cmd.Issue()


class GkeAutopilotCluster(BaseGkeCluster):
  """Class representing an Autopilot GKE cluster, which has no nodepools."""

  CLOUD = provider_info.GCP
  CLUSTER_TYPE = 'Autopilot'

  def __init__(self, spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(spec)
    self.region = util.GetRegionFromZone(self.zones[0])
    # Nodepools are not supported for Autopilot clusters, but default vm_spec
    # still used for pod spec input.
    self.nodepools = []

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    self.zones = [vm_config.zone]
    nodepool_config.network = vm_config.network
    return nodepool_config

  @property
  def zone(self) -> str:
    if not self.zones:
      raise errors.Resource.CreationInternalError(
          'Cluster has not been initialized yet.'
      )
    return self.zones[0] or 'us-central1-a'

  def _GcloudCommand(self, *args, **kwargs) -> util.GcloudCommand:
    """Creates a gcloud command."""
    cmd = super()._GcloudCommand(*args, **kwargs)
    if 'zone' in cmd.flags:
      del cmd.flags['zone']
    cmd.flags['region'] = self.region
    return cmd

  def _Create(self):
    """Creates the cluster."""
    cmd = self._GcloudCommand(
        'container',
        'clusters',
        'create-auto',
        self.name,
        '--no-autoprovisioning-enable-insecure-kubelet-readonly-port',
    )
    if self.default_nodepool.network:
      cmd.flags['network'] = self.default_nodepool.network.network_resource.name
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()

    self._RunClusterCreateCommand(cmd)

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = {
        'cloud': self.CLOUD,
        'cluster_type': 'KubernetesAutopilot',
        'zone': self.zone,
        'region': self.region,
    }
    return metadata

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_service.DEFAULT_NODEPOOL
  ):
    raise NotImplementedError('Autopilot clusters do not support resizing.')
