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
import time
import typing
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import container
from perfkitbenchmarker.resources.container_service import container_cluster
from perfkitbenchmarker.resources.container_service import container_registry
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

SERVICE_ACCOUNT_PATTERN = r'.*((?<!iam)|{project}.iam).gserviceaccount.com'
ONE_HOUR = 60 * 60

TPU_MACHINE_TO_ACCELERATOR_MAP: dict[str, str] = {
    'tpu7x-standard-4t': 'tpu7x',
    'ct6e-standard-1t': 'tpu-v6e-slice',
    'ct6e-standard-4t': 'tpu-v6e-slice',
    'ct6e-standard-8t': 'tpu-v6e-slice',
    'ct5p-hightpu-4t': 'tpu-v5p-slice',
    'ct5lp-hightpu-1t': 'tpu-v5-lite-podslice',
    'ct5lp-hightpu-4t': 'tpu-v5-lite-podslice',
    'ct5lp-hightpu-8t': 'tpu-v5-lite-podslice',
    'ct4p-hightpu-4t': 'tpu-v4-podslice',
    'ct3-hightpu-4t': 'tpu-v3-device',
    'ct3p-hightpu-4t': 'tpu-v3-slice',
}


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


class GoogleArtifactRegistry(container_registry.BaseContainerRegistry):
  """Class for building/storing container images on GCP w/ Artifact Registry."""

  CLOUD = provider_info.GCP

  def __init__(self, registry_spec):
    super().__init__(registry_spec)
    self.project = self.project or util.GetDefaultProject()
    self.region = util.GetRegionFromZone(self.zone)
    # Remove from gcloud commands
    self.zone = None  # pyrefly: ignore[bad-assignment]
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
    self.Login()
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

  def RemoteBuild(self, image: container.ContainerImage):
    """Builds the image remotely."""
    if not gcp_flags.CONTAINER_REMOTE_BUILD_CONFIG.value:
      full_tag = self.GetFullRegistryTag(image.name)
    else:
      full_tag = gcp_flags.CONTAINER_REMOTE_BUILD_CONFIG.value
    build_cmd = util.GcloudCommand(
        self, 'builds', 'submit', '--tag', full_tag, image.directory
    )
    build_cmd.Issue(timeout=None)


class BaseGkeCluster(kubernetes_cluster.KubernetesCluster):
  """Base class for regular & Autopilot GKE clusters."""

  def __init__(self, spec: container_spec_lib.ContainerClusterSpec):
    self.tpu_topology: str | None = None
    self.tpu_count: int | None = None
    self.tpu_type: str | None = None
    super().__init__(spec)
    self.project: str = spec.vm_spec.GetProject()  # pyrefly: ignore[missing-attribute]
    self.cluster_version: str = FLAGS.container_cluster_version
    self.release_channel: str | None = gcp_flags.CONTAINER_RELEASE_CHANNEL.value
    self.use_application_default_credentials: bool = True
    self.zones = (
        self.default_nodepool.zone and self.default_nodepool.zone.split(',')
    )
    if not self.zones:
      raise errors.Config.MissingOption(
          'container_cluster.vm_spec.GCP.zone is required.'
      )
    elif len(self.zones) == 1 and util.IsRegion(self.default_nodepool.zone):
      self.region: str = self.default_nodepool.zone
      self.zones = []
      logging.info(
          "Interpreting zone '%s' as a region", self.default_nodepool.zone
      )
    else:
      self.region: str = util.GetRegionFromZone(self.zones[0])

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine_spec.BaseVmSpec,
      nodepool_config: container.BaseNodePoolConfig,
  ):
    super().InitializeNodePoolForCloud(vm_config, nodepool_config)
    vm_config = typing.cast(gce_virtual_machine.GceVmSpec, vm_config)
    nodepool_config.network = gce_network.GceNetwork.GetNetwork(vm_config)  # pyrefly: ignore[missing-attribute]
    if nodepool_config.tpu_topology:
      if self.tpu_topology:
        raise errors.Config.InvalidValue(
            f'Attempted to set TPU topology to {nodepool_config.tpu_topology}'
            f', but it was already set to {self.tpu_topology}. Multiple'
            ' nodepools with different TPU topologies are not currently'
            ' supported.'
        )
      if nodepool_config.tpu_topology and not nodepool_config.tpu_count:
        raise errors.Config.InvalidValue(
            'tpu_count must be set if tpu_topology is set.'
        )
      if nodepool_config.machine_type not in TPU_MACHINE_TO_ACCELERATOR_MAP:
        raise errors.Config.InvalidValue(
            f'Unsupported TPU machine type: {nodepool_config.machine_type}'
        )
      self.tpu_type = TPU_MACHINE_TO_ACCELERATOR_MAP[
          nodepool_config.machine_type
      ]
      self.tpu_topology = nodepool_config.tpu_topology
      self.tpu_count = nodepool_config.tpu_count

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    metadata = super().GetResourceMetadata()
    metadata['project'] = self.project
    if self.release_channel:
      metadata['release_channel'] = self.release_channel
    return metadata

  def _GcloudCommand(self, *args, **kwargs) -> util.GcloudCommand:
    """Creates a gcloud command."""
    return util.GcloudCommand(self, *args, **kwargs)

  def _RunClusterCreateCommand(self, cmd: util.GcloudCommand):
    """Adds flags to the cluster create command and runs it."""
    # All combinations of cluster-version and release-channel are supported.
    # Specifying one uses default for the other. Specifying both can be needed
    # as some versions are only supported in some release channels.
    if self.cluster_version:
      cmd.flags['cluster-version'] = self.cluster_version
    if self.release_channel:
      if FLAGS.gke_enable_alpha:
        raise errors.Config.InvalidValue(
            'Kubernetes Alpha is not compatible with release channels'
        )
      cmd.flags['release-channel'] = self.release_channel

    if FLAGS.gke_enable_alpha:
      cmd.args.append('--enable-kubernetes-alpha')
      cmd.args.append('--no-enable-autorepair')
    cmd.flags['monitoring'] = 'SYSTEM,API_SERVER,SCHEDULER,CONTROLLER_MANAGER'

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

    if self.enable_vpa:
      cmd.flags['enable-vertical-pod-autoscaling'] = True

    self._IssueResourceCreationCommand(cmd)

  def _IssueResourceCreationCommand(self, cmd: util.GcloudCommand):
    """Issues a command to gcloud to create resources."""

    # This command needs a long timeout due to the many minutes it
    # can take to provision a large GPU-accelerated GKE cluster.
    _, stderr, retcode = cmd.Issue(timeout=ONE_HOUR, raise_on_failure=False)
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  def _GetKubeconfig(self):
    """Returns the kubeconfig for the cluster."""
    cmd = self._GcloudCommand(
        'container', 'clusters', 'get-credentials', self.name
    )
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    cmd.IssueRetryable(env=env)

  def _IsDeleting(self):
    cmd = self._GcloudCommand('container', 'clusters', 'describe', self.name)
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    return True if stdout else False

  def _Delete(self):
    """Deletes the cluster."""
    super()._Delete()
    cmd = self._GcloudCommand('container', 'clusters', 'delete', self.name)
    cmd.args.append('--async')
    _, err, retcode = cmd.Issue(raise_on_failure=False)
    if retcode:
      # Some known retryable errors:
      # Please wait and try again
      # Cluster is running incompatible operation operation-id.
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

  def HasLocalSsd(self, nodepool_name: str = 'default') -> bool:
    """Returns true if the given nodepool has local SSDs."""
    if nodepool_name == 'default':
      nodepool = self.default_nodepool
    elif nodepool_name not in self.nodepools:
      raise errors.Config.InvalidValue(
          f'Nodepool {nodepool_name} not found in cluster.'
      )
    else:
      nodepool = self.nodepools[nodepool_name]
    return nodepool.max_local_disks is not None and nodepool.max_local_disks > 0

  def GetNodePoolNames(self) -> list[str]:
    """Get node pool names for the cluster."""
    # Command `gcloud container node-pools list` does not work for Autopilot
    # clusters - node pools are hidden and command results in 4xx.
    cmd = self._GcloudCommand('container', 'clusters', 'describe', self.name)
    cmd.flags['flatten'] = 'nodePools'
    cmd.flags['format'] = 'value(nodePools.name)'
    stdout, _, _ = cmd.Issue()
    return stdout.split()

  def GetMachineTypeFromNodeName(self, node_name: str) -> str | None:
    """Get the machine type from the node name."""
    machine_type: str | None = super().GetMachineTypeFromNodeName(node_name)
    if machine_type is not None:
      return machine_type
    out, _, _ = kubectl.RunKubectlCommand([
        'get',
        'node',
        node_name,
        '-o',
        r'jsonpath={.metadata.labels.node\.kubernetes\.io/instance-type}',
    ])
    return out.strip() or None

  def _UsesCustomComputeClass(
      self, nodepool_config: container.BaseNodePoolConfig
  ) -> bool:
    """Returns True if the nodepool config uses a custom compute class."""
    return bool(
        (nodepool_config.machine_type is None and not nodepool_config.cpus)
        or nodepool_config.machine_families
    )

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Returns node selectors for the default nodepool."""
    del machine_type
    selectors = {}
    if self.tpu_topology and self.tpu_type:
      selectors.update({
          'cloud.google.com/gke-tpu-topology': self.tpu_topology,
          'cloud.google.com/gke-tpu-accelerator': self.tpu_type,
      })
    if gcp_flags.GCE_RESERVATION_ID.value:
      selectors.update({
          'cloud.google.com/reservation-name': (
              gcp_flags.GCE_RESERVATION_ID.value
          ),
          'cloud.google.com/reservation-affinity': 'specific',
      })
    return selectors

  def _ModifyPodSpecPlacementYaml(
      self,
      pod_spec_yaml: dict[str, Any],
      name: str,
      machine_type: str | None = None,
  ) -> None:
    """Modifies the pod spec yaml with additional needed attributes."""
    super()._ModifyPodSpecPlacementYaml(pod_spec_yaml, name, machine_type)
    if self.tpu_count:
      for c in pod_spec_yaml['containers']:
        c['resources']['limits']['google.com/tpu'] = (
            str(self.tpu_count)
        )
        c['resources']['requests']['google.com/tpu'] = (
            str(self.tpu_count)
        )


class GkeCluster(BaseGkeCluster):
  """Class representing a Google Kubernetes Engine cluster."""

  CLOUD = provider_info.GCP

  def __init__(self, spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(spec)
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
    self.image_type = gcp_flags.GKE_IMAGE_TYPE.value

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine_spec.BaseVmSpec,
      nodepool_config: container.BaseNodePoolConfig,
  ):
    super().InitializeNodePoolForCloud(vm_config, nodepool_config)
    vm_config = typing.cast(gce_virtual_machine.GceVmSpec, vm_config)
    nodepool_config.disk_type = vm_config.boot_disk_type
    nodepool_config.disk_size = vm_config.boot_disk_size
    nodepool_config.max_local_disks = vm_config.max_local_disks
    nodepool_config.ssd_interface = vm_config.ssd_interface
    nodepool_config.threads_per_core = vm_config.threads_per_core
    nodepool_config.gce_tags = vm_config.gce_tags
    nodepool_config.min_cpu_platform = vm_config.min_cpu_platform
    nodepool_config.cpus: int = vm_config.cpus  # pyrefly: ignore[bad-assignment]
    nodepool_config.memory_mib: int = vm_config.memory  # pyrefly: ignore[bad-assignment]

  def _GcloudCommand(self, *args, **kwargs) -> util.GcloudCommand:
    """Fix zone and region."""
    cmd = super()._GcloudCommand(*args, **kwargs)
    if len(self.zones) != 1:
      del cmd.flags['zone']
      cmd.flags['region'] = self.region
    return cmd

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Targets the default pool ComputeClass when custom classes are enabled."""
    selectors = super().GetNodeSelectors(machine_type)
    if self._UsesCustomComputeClass(self.default_nodepool):
      return selectors | {
          'cloud.google.com/compute-class': self.default_nodepool.name
      }
    return selectors

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super().GetResourceMetadata()
    result['boot_disk_type'] = self.default_nodepool.disk_type
    result['boot_disk_size'] = self.default_nodepool.disk_size
    if self.default_nodepool.max_local_disks:
      result['gce_local_ssd_count'] = self.default_nodepool.max_local_disks
      result['gce_local_ssd_interface'] = self.default_nodepool.ssd_interface
    result['gke_nccl_fast_socket'] = self.enable_nccl_fast_socket
    if self.image_type:
      result['image_type'] = self.image_type
    if gcp_flags.MAX_CPU.value:
      result['max-cpu'] = gcp_flags.MAX_CPU.value
    if gcp_flags.MAX_MEMORY.value:
      result['max-memory'] = gcp_flags.MAX_MEMORY.value
    if gcp_flags.MAX_ACCELERATOR.value:
      result['max-accelerator'] = gcp_flags.MAX_ACCELERATOR.value
    if gcp_flags.GKE_AUTOSCALING_PROFILE.value:
      result['gke_autoscaling_profile'] = (
          gcp_flags.GKE_AUTOSCALING_PROFILE.value
      )

    return result

  def _Create(self):
    """Creates the cluster."""
    cmd = self._GcloudCommand('container', 'clusters', 'create', self.name)
    if self.default_nodepool.network:  # pyrefly: ignore[missing-attribute]
      cmd.flags['network'] = self.default_nodepool.network.network_resource.name  # pyrefly: ignore[missing-attribute]
      if getattr(self.default_nodepool.network, 'primary_subnet_name', None):
        cmd.flags['subnetwork'] = (
            self.default_nodepool.network.primary_subnet_name
        )

    if gcp_flags.GKE_ENABLE_SHIELDED_NODES.value:
      cmd.args.append('--enable-shielded-nodes')
    else:
      cmd.args.append('--no-enable-shielded-nodes')
    if gcp_flags.GKE_ADDONS.value:
      cmd.args.append(f'--addons={gcp_flags.GKE_ADDONS.value}')
    if gcp_flags.GKE_ENABLE_WORKLOAD_IDENTITY.value:
      cmd.flags['workload-pool'] = f'{self.project}.svc.id.goog'
    if not self.release_channel:
      cmd.args.append('--no-enable-autoupgrade')
    self._AddNodeParamsToCmd(
        self.default_nodepool,
        cmd,
    )
    if self._UsesCustomComputeClass(self.default_nodepool):
      cmd.args.append('--enable-default-compute-class')
    enable_autoprovisioning = False
    if gcp_flags.MAX_CPU.value:
      cmd.flags['max-cpu'] = gcp_flags.MAX_CPU.value
      enable_autoprovisioning = True
    if gcp_flags.MAX_MEMORY.value:
      cmd.flags['max-memory'] = gcp_flags.MAX_MEMORY.value
      enable_autoprovisioning = True
    if gcp_flags.MAX_ACCELERATOR.value:
      cmd.flags['max-accelerator'] = gcp_flags.MAX_ACCELERATOR.value
      enable_autoprovisioning = True
    if enable_autoprovisioning:
      cmd.args.append('--enable-autoprovisioning')

    if gcp_flags.GKE_AUTOSCALING_PROFILE.value:
      cmd.flags['autoscaling-profile'] = gcp_flags.GKE_AUTOSCALING_PROFILE.value
    cidr_size = (
        gcp_flags.GKE_CLUSTER_IPV4_CIDR_SIZE.value
        or _CalculateCidrSize(self.max_total_nodes)
    )
    cmd.flags['cluster-ipv4-cidr'] = f'/{cidr_size}'
    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()

    if self.enable_aam:
      cmd.args.append('--auto-monitoring-scope=ALL')

    self._RunClusterCreateCommand(cmd)
    self._GetKubeconfig()
    self._CreateCustomComputeClass(self.default_nodepool)
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
      self._CreateCustomComputeClass(nodepool)

  def _CreateCustomComputeClass(
      self, nodepool_config: container.BaseNodePoolConfig
  ):
    """Creates a custom compute class for the nodepool."""
    if not self._UsesCustomComputeClass(nodepool_config):
      return
    compute_manifest = {
        'apiVersion': 'cloud.google.com/v1',
        'kind': 'ComputeClass',
        'metadata': {
            'name': nodepool_config.name,
        },
    }
    priorities = []
    for machine_family in nodepool_config.machine_families:
      priorities.append({
          'machineFamily': machine_family,
      })
    is_default_class = (
        nodepool_config.name == container_cluster.DEFAULT_NODEPOOL
    )
    compute_manifest['spec'] = {'priorities': priorities}  # pyrefly: ignore[bad-assignment]
    if is_default_class:
      compute_manifest['spec']['nodePoolAutoCreation'] = {'enabled': True}  # pyrefly: ignore[bad-assignment, unsupported-operation]
    kubernetes_commands.ApplyYaml([compute_manifest])
    if is_default_class:
      return
    cmd = self._GcloudCommand(
        'container',
        'node-pools',
        'update',
        nodepool_config.name,
        '--cluster',
        self.name,
        '--node-labels',
        f'cloud.google.com/compute-class={nodepool_config.name}',
    )
    cmd.Issue()
    cmd = self._GcloudCommand(
        'container',
        'node-pools',
        'update',
        nodepool_config.name,
        '--cluster',
        self.name,
        '--node-taints',
        f'cloud.google.com/compute-class={nodepool_config.name}:NoSchedule',
    )
    cmd.Issue()

  def _AddNodeParamsToCmd(
      self,
      nodepool_config: container.BaseNodePoolConfig,
      cmd: util.GcloudCommand,
  ):
    """Modifies cmd to include node specific command arguments."""
    # Apply labels to all nodepools.
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    # Allow a long timeout due to the many minutes it can take to provision a
    # large GPU-accelerated GKE cluster.
    # Parameter is not documented well but is available in CLI.
    cmd.flags['timeout'] = ONE_HOUR

    nodepool_labels = [f'pkb_nodepool={nodepool_config.name}']
    if nodepool_config.gpu_count:
      if 'a2-' not in nodepool_config.machine_type:  # pyrefly: ignore[not-iterable]
        accelerator_spec = gce_virtual_machine.GenerateAcceleratorSpecString(
            nodepool_config.gpu_type, nodepool_config.gpu_count  # pyrefly: ignore[bad-argument-type]
        )
        if gcp_flags.GKE_GPU_DRIVER_VERSION.value:
          accelerator_spec += (
              ',gpu-driver-version=' + gcp_flags.GKE_GPU_DRIVER_VERSION.value
          )
        cmd.flags['accelerator'] = accelerator_spec
    if nodepool_config.gpu_count or nodepool_config.tpu_count:
      # Not guaranteed, but atm reservations only needed for GPUs.
      if gcp_flags.GCE_RESERVATION_ID.value:
        cmd.flags['reservation'] = gcp_flags.GCE_RESERVATION_ID.value
        cmd.flags['reservation-affinity'] = 'specific'
        nodepool_labels.extend([
            'cloud.google.com/reservation-affinity=specific',
            f'cloud.google.com/reservation-name={gcp_flags.GCE_RESERVATION_ID.value}',
        ])
    cmd.flags['node-labels'] = ','.join(nodepool_labels)

    gce_tags = FLAGS.gce_tags
    if nodepool_config.gce_tags:
      gce_tags = nodepool_config.gce_tags
    if gce_tags:
      cmd.flags['tags'] = ','.join(gce_tags)
    if nodepool_config.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = nodepool_config.min_cpu_platform

    if gcp_flags.GCE_PROVISIONING_MODEL.value == 'SPOT':
      cmd.args.append('--spot')

    if nodepool_config.threads_per_core:
      # TODO(user): Remove when threads-per-core is available in GA
      cmd.use_alpha_gcloud = True
      cmd.flags['threads-per-core'] = nodepool_config.threads_per_core

    if nodepool_config.disk_size:
      cmd.flags['disk-size'] = nodepool_config.disk_size
    if nodepool_config.disk_type:
      cmd.flags['disk-type'] = nodepool_config.disk_type
    if nodepool_config.max_local_disks:
      # https://cloud.google.com/kubernetes-engine/docs/concepts/local-ssd
      if nodepool_config.ssd_interface == gce_virtual_machine.NVME:
        if gcp_flags.GKE_USE_LSSD_AS_EPHEMERAL_STORAGE.value:
          ssd_flag = 'ephemeral-storage-local-ssd'
        else:
          ssd_flag = 'local-nvme-ssd-block'
        # Technically the count paramter is optional for gen 3+ VMs.
        # However gce_virtual_machine always passes it explitly, so be
        # consistent here.
        cmd.flags[ssd_flag] = f'count={nodepool_config.max_local_disks}'
      else:
        cmd.flags['local-ssd-count'] = nodepool_config.max_local_disks

    cmd.flags['num-nodes'] = nodepool_config.num_nodes
    # zone may be split a comma separated list. For regional clusters, zone
    # holds the region name; do not set node-locations so GKE uses default.
    if nodepool_config.zone and not util.IsRegion(nodepool_config.zone):
      cmd.flags['node-locations'] = nodepool_config.zone

    if nodepool_config.machine_type:
      cmd.flags['machine-type'] = nodepool_config.machine_type
    elif nodepool_config.cpus and nodepool_config.memory_mib:
      cmd.flags['machine-type'] = 'custom-{}-{}'.format(
          nodepool_config.cpus, nodepool_config.memory_mib
      )
    else:
      assert (
          nodepool_config.machine_families
      ), 'No machine type nor custom type nor machine family specified.'

    if FLAGS.gke_enable_gvnic:
      cmd.args.append('--enable-gvnic')
    else:
      cmd.args.append('--no-enable-gvnic')
    if (
        self.enable_nccl_fast_socket
        and nodepool_config.name != container_cluster.DEFAULT_NODEPOOL
    ):
      cmd.args.append('--enable-fast-socket')
    if gcp_flags.GKE_ENABLE_WORKLOAD_IDENTITY.value:
      cmd.flags['workload-metadata'] = 'GKE_METADATA'

    if FLAGS.gke_node_system_config is not None:
      cmd.flags['system-config-from-file'] = FLAGS.gke_node_system_config

    if nodepool_config.sandbox_config is not None:
      cmd.flags['sandbox'] = nodepool_config.sandbox_config.ToSandboxFlag()

    if self.image_type:
      cmd.flags['image-type'] = self.image_type

    if nodepool_config.min_nodes != nodepool_config.max_nodes:
      cmd.args.append('--enable-autoscaling')
      cmd.flags['min-nodes'] = nodepool_config.min_nodes
      cmd.flags['max-nodes'] = nodepool_config.max_nodes

  def _PostCreate(self):
    """Waits for kube-dns to be available."""
    super()._PostCreate()

    # GKE does not wait for kube-dns by default
    logging.info('Waiting for kube-dns')
    kubernetes_commands.WaitForResource(
        'deployment/kube-dns',
        condition_name='Available',
        namespace='kube-system',
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
    pvcs = kubernetes_commands.GetPvcs()
    for pvc in pvcs:
      gce_disk.AddLabels(self, pvc['spec']['volumeName'])

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_cluster.DEFAULT_NODEPOOL
  ):
    """Changes the number of nodes in the node pool."""
    cmd = self._GcloudCommand('container', 'clusters', 'resize', self.name)
    cmd.flags['num-nodes'] = new_size
    # updates default node pool by default
    if node_pool != container_cluster.DEFAULT_NODEPOOL:
      cmd.flags['node-pool'] = node_pool
    cmd.Issue()

  def _IssueAsync(
      self,
      cmd: util.GcloudCommand,
      fallback_op_type: str | None = None,
      fallback_target: str = '',
  ) -> str:
    """Issues a gcloud --async command and returns the operation name.

    Most async commands (node-pool create/delete) print the operation name to
    stdout. A few (`clusters upgrade --node-pool`, `clusters update`) reliably
    return success with empty stdout -- gcloud simply does not emit the name
    for those subcommands. For those, pass fallback_op_type/fallback_target
    and the operation name is recovered from `gcloud container operations
    list` via GcloudCommand.IssueAsync's get_latest_op_fn fallback.

    Args:
      cmd: the gcloud command to issue (--async and format are added by
        GcloudCommand.IssueAsync).
      fallback_op_type: GKE operationType (e.g. 'UPGRADE_NODES',
        'UPDATE_CLUSTER') to look up if stdout is empty. None disables the
        fallback (create/delete, which always print the name).
      fallback_target: targetLink substring for the fallback lookup (node-pool
        or cluster name).

    Returns:
      The operation name.
    """
    # Recorded before issuing so the fallback's startTime>= guard can include
    # fast ops that may already be DONE before the operations-list query runs.
    op_start_time = time.time()
    get_latest_op_fn = None
    if fallback_op_type is not None:
      get_latest_op_fn = lambda: self._GetLatestOperationName(
          operation_type=fallback_op_type,
          target_name=fallback_target,
          op_start_time=op_start_time,
      )
    return cmd.IssueAsync(get_latest_op_fn=get_latest_op_fn, timeout=600)

  def _GetLatestOperationName(
      self,
      operation_type: str = 'UPGRADE_NODES',
      target_name: str = '',
      max_attempts: int = 5,
      retry_delay: int = 3,
      op_start_time: float = 0.0,
  ) -> str:
    """Returns the name of the most recent matching operation for this cluster.

    Used to recover an operation name for async commands that don't print one
    (upgrade/update). The async gcloud command may return before the control
    plane has transitioned the operation out of PENDING, and fast operations
    (e.g. label updates) may already be DONE by the time this runs — so the
    status filter always includes RUNNING/PENDING/DONE, with a startTime guard
    (op_start_time minus a 30s clock-skew buffer) to avoid matching older
    completed operations.

    Args:
        operation_type: GKE operationType to filter on, e.g. 'UPGRADE_NODES' for
          node pool upgrades or 'UPDATE_CLUSTER' for cluster-level updates.
        target_name: Substring to match against targetLink (node-pool name for
          UPGRADE_NODES, cluster name for UPDATE_CLUSTER). If empty, falls back
          to self.name.
        max_attempts: Number of query attempts before giving up.
        retry_delay: Seconds to wait between attempts.
        op_start_time: Unix timestamp recorded just before the async command was
          issued; used for the startTime>= guard. Defaults to now minus the
          buffer if not supplied.

    Returns:
        Operation name string.
    """
    link_target = target_name or self.name
    # 30-second buffer absorbs clock skew between client and control plane.
    from_time = time.strftime(
        '%Y-%m-%dT%H:%M:%SZ', time.gmtime((op_start_time or time.time()) - 30)
    )
    filter_str = (
        f'operationType={operation_type} AND '
        '(status=RUNNING OR status=PENDING OR status=DONE) AND '
        f'targetLink ~ {link_target} AND '
        f'startTime>="{from_time}"'
    )

    @vm_util.Retry(
        poll_interval=retry_delay,
        max_retries=max_attempts,
        retryable_exceptions=(errors.Resource.GetError,),
        log_errors=False,
    )
    def _QueryOnce() -> str:
      list_cmd = self._GcloudCommand('container', 'operations', 'list')
      list_cmd.flags['filter'] = filter_str
      list_cmd.flags['sort-by'] = '~startTime'
      list_cmd.flags['limit'] = 1
      list_cmd.flags['format'] = 'value(name)'
      stdout, stderr, _ = list_cmd.Issue(raise_on_failure=False)
      op_name = stdout.strip()
      if not op_name:
        raise errors.Resource.GetError(
            f'_GetLatestOperationName: no {operation_type} op found '
            f'for target={link_target}. stderr={stderr}'
        )
      logging.info(
          'GetLatestOp: found %s type=%s target=%s',
          op_name,
          operation_type,
          link_target,
      )
      return op_name

    return _QueryOnce()

  def CreateNodePoolAsync(
      self,
      nodepool_config: container.BaseNodePoolConfig,
      node_version: str | None = None,
  ) -> str:
    """Initiates node pool create; returns op handle. Does NOT wait."""
    cmd = self._GcloudCommand(
        'container',
        'node-pools',
        'create',
        nodepool_config.name,
        '--cluster',
        self.name,
    )
    self._AddNodeParamsToCmd(nodepool_config, cmd)
    if node_version:
      cmd.flags['node-version'] = node_version
    # --async is incompatible with the long --timeout flag in some gcloud
    # builds; remove it so the CLI just hands back the op name immediately.
    cmd.flags.pop('timeout', None)
    return self._IssueAsync(cmd)

  def UpgradeNodePoolAsync(self, name: str, target_version: str) -> str:
    """Initiates node pool upgrade; returns op handle. Does NOT wait.

    `clusters upgrade --node-pool --async` returns success with empty stdout
    (gcloud doesn't print the op name for this subcommand), so the operation
    name is recovered from the operations list via _IssueAsync's fallback.

    Returns:
        Operation name string.
    """
    cmd = self._GcloudCommand(
        'container',
        'clusters',
        'upgrade',
        self.name,
        '--node-pool',
        name,
        '--cluster-version',
        target_version,
    )
    return self._IssueAsync(
        cmd, fallback_op_type='UPGRADE_NODES', fallback_target=name
    )

  def DeleteNodePoolAsync(self, name: str) -> str:
    cmd = self._GcloudCommand(
        'container',
        'node-pools',
        'delete',
        name,
        '--cluster',
        self.name,
    )
    cmd.args.append('--quiet')
    return self._IssueAsync(cmd)

  def UpdateClusterAsync(self) -> str:
    """Initiates cluster update; returns op handle. Does NOT wait.

    Toggles a label for a non-destructive cluster update. Like
    `clusters upgrade`, `clusters update --async` returns success with empty
    stdout, so the op name is recovered via _IssueAsync's fallback. The
    label-update completes in seconds, so the fallback may find it already
    DONE — handled by _GetLatestOperationName's startTime-guarded filter.
    """
    cmd = self._GcloudCommand('container', 'clusters', 'update', self.name)
    cmd.flags['update-labels'] = f'k8s-mgmt-ts={int(time.time())}'
    # GcloudCommand sets --quiet by default; the label update is
    # non-interactive so it's safe to drop (matches how this was validated).
    cmd.flags.pop('quiet', None)
    return self._IssueAsync(
        cmd, fallback_op_type='UPDATE_CLUSTER', fallback_target=self.name
    )

  def ResolveNodePoolVersions(self) -> tuple[str, str]:
    """Returns (initial, target) GKE node versions: initial=N-1, target=N.

    GKE requires fully-qualified node versions (e.g. '1.34.4-gke.1234'),
    so we query `gcloud container get-server-config` and pick the newest
    valid version per minor.
    """
    cmd = self._GcloudCommand('container', 'get-server-config')
    cmd.flags['format'] = 'json'
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode:
      raise errors.Resource.GetError(
          f'gcloud get-server-config failed: {stderr}'
      )
    config = json.loads(stdout)
    valid = list(config.get('validNodeVersions', []))
    if not valid:
      raise errors.Resource.GetError(
          'GKE get-server-config returned no validNodeVersions'
      )

    def _version_tuple(v):
      return tuple(int(x) for x in v.split('-', 1)[0].split('.'))

    valid.sort(key=_version_tuple, reverse=True)
    target = valid[0]
    target_parts = target.split('-', 1)[0].split('.')
    initial_minor = f'{target_parts[0]}.{int(target_parts[1]) - 1}'
    for v in valid:
      v_bare = '.'.join(v.split('-', 1)[0].split('.')[:2])
      if v_bare == initial_minor:
        return v, target
    raise errors.Resource.GetError(
        f'No GKE node version found for minor {initial_minor!r}; '
        f'available top 5: {valid[:5]}'
    )

  def WaitForOperation(self, op_handle: str) -> None:
    """Polls a GKE operation until terminal; raises on failure."""

    @vm_util.Retry(
        poll_interval=5,
        fuzz=0,
        timeout=ONE_HOUR,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _poll():
      describe = self._GcloudCommand(
          'container',
          'operations',
          'describe',
          op_handle,
      )
      describe.flags['format'] = 'json'
      out, err, rc = describe.Issue(raise_on_failure=False)
      if rc:
        raise errors.Resource.RetryableCreationError(
            f'describe op failed: {err}'
        )
      try:
        status = json.loads(out).get('status')
      except (json.JSONDecodeError, ValueError):
        status = out.strip()
      if status == 'DONE':
        return
      if status in ('ABORTING', 'ABORTED'):
        raise errors.Resource.CreationError(f'op {op_handle} aborted')
      raise errors.Resource.RetryableCreationError(
          f'op {op_handle} status={status}'
      )

    _poll()


class GkeAutopilotCluster(BaseGkeCluster):
  """Class representing an Autopilot GKE cluster, which has no nodepools."""

  CLOUD = provider_info.GCP
  CLUSTER_TYPE = 'Auto'

  def __init__(self, spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(spec)
    # Nodepools are not supported for Autopilot clusters, but default vm_spec
    # still used for pod spec input.
    self.nodepools = {}

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
    if self.default_nodepool.network:  # pyrefly: ignore[missing-attribute]
      cmd.flags['network'] = self.default_nodepool.network.network_resource.name  # pyrefly: ignore[missing-attribute]
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()

    if self.enable_aam:
      cmd.args.append('--auto-monitoring-scope=ALL')

    self._RunClusterCreateCommand(cmd)
    self._GetKubeconfig()

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata['zone'] = self.zone
    metadata['region'] = self.region
    # Override node specific metadata set in parent.
    metadata['machine_type'] = (
        util.GetMachineFamily(self.default_nodepool.machine_type)
        or self.CLUSTER_TYPE
    )
    metadata['size'] = self.CLUSTER_TYPE
    metadata['nodepools'] = self.CLUSTER_TYPE
    return metadata

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Node selectors for instance capabilites in AutoPilot clusters."""
    selectors = super().GetNodeSelectors(machine_type)
    compute_class = None
    machine_family: str | None = util.GetMachineFamily(machine_type)
    if machine_family:
      selectors['cloud.google.com/machine-family'] = machine_family
      # Mandate one pod per node, which also handles packing small pods into
      # bigger nodes.
      compute_class = 'Performance'
    # https://cloud.google.com/kubernetes-engine/docs/how-to/autopilot-gpus#request-gpus
    if self.gpu_type:
      gpu_count = self.gpu_count or 1
      gpu_type = self.gpu_type
      suffix = ''
      if gpu_type in gce_virtual_machine.GPU_TYPE_TO_SUFFIX:
        suffix = gce_virtual_machine.GPU_TYPE_TO_SUFFIX[gpu_type]
      gpu_type = f'nvidia-{gpu_type}{suffix}'
      gpu_driver_version = gcp_flags.GKE_GPU_DRIVER_VERSION.value
      selectors.update({
          'cloud.google.com/gke-accelerator': gpu_type,
          # Quote to avoid YAML parsing as int.
          'cloud.google.com/gke-accelerator-count': str(gpu_count),
          'cloud.google.com/gke-gpu-driver-version': str(gpu_driver_version),
      })
      # Override earlier compute class, as only one can be set & Accelerator
      # (or nothing) is required for GPUs.
      compute_class = 'Accelerator'
    if compute_class:
      selectors['cloud.google.com/compute-class'] = compute_class
    return selectors

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_cluster.DEFAULT_NODEPOOL
  ):
    raise NotImplementedError('Autopilot clusters do not support resizing.')
