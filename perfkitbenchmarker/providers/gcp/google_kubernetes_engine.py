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

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
import six

FLAGS = flags.FLAGS

NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT = 'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded.yaml'
NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET = 'nvidia_unrestricted_permissions_daemonset.yml'
DEFAULT_CONTAINER_VERSION = 'latest'
SERVICE_ACCOUNT_PATTERN = r'.*((?<!iam)|{project}.iam).gserviceaccount.com'


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


class GoogleContainerRegistry(container_service.BaseContainerRegistry):
  """Class for building and storing container images on GCP."""

  CLOUD = providers.GCP

  def __init__(self, registry_spec):
    super(GoogleContainerRegistry, self).__init__(registry_spec)
    self.project = self.project or util.GetDefaultProject()

  def GetFullRegistryTag(self, image):
    """Gets the full tag of the image."""
    region = util.GetMultiRegionFromRegion(util.GetRegionFromZone(self.zone))
    hostname = '{region}.gcr.io'.format(region=region)
    full_tag = '{hostname}/{project}/{name}'.format(
        hostname=hostname, project=self.project, name=image)
    return full_tag

  def Login(self):
    """Configure docker to be able to push to remote repo."""
    # TODO(pclay): Don't edit user's docker config. It is idempotent.
    cmd = util.GcloudCommand(self, 'auth', 'configure-docker')
    del cmd.flags['zone']
    cmd.Issue()

  def RemoteBuild(self, image):
    """Build the image remotely."""
    full_tag = self.GetFullRegistryTag(image.name)
    build_cmd = util.GcloudCommand(self, 'builds', 'submit', '--tag', full_tag,
                                   image.directory)
    del build_cmd.flags['zone']
    build_cmd.Issue()


class GkeCluster(container_service.KubernetesCluster):
  """Class representing a Google Kubernetes Engine cluster."""

  CLOUD = providers.GCP

  def __init__(self, spec):
    super(GkeCluster, self).__init__(spec)
    self.project = spec.vm_spec.project
    self.cluster_version = (
        FLAGS.container_cluster_version or DEFAULT_CONTAINER_VERSION)
    self.use_application_default_credentials = True

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super(GkeCluster, self).GetResourceMetadata()
    result['project'] = self.project
    result['container_cluster_version'] = self.cluster_version
    result['boot_disk_type'] = self.vm_config.boot_disk_type
    result['boot_disk_size'] = self.vm_config.boot_disk_size
    if self.vm_config.max_local_disks:
      result['gce_local_ssd_count'] = self.vm_config.max_local_disks
      # TODO(pclay): support NVME when it leaves alpha
      # Also consider moving FLAGS.gce_ssd_interface into the vm_spec.
      result['gce_local_ssd_interface'] = gce_virtual_machine.SCSI
    return result

  def _Create(self):
    """Creates the cluster."""
    cmd = util.GcloudCommand(self, 'container', 'clusters', 'create', self.name)

    self._AddNodeParamsToCmd(self.vm_config, self.num_nodes,
                             container_service.DEFAULT_NODEPOOL, cmd)

    cmd.flags['cluster-version'] = self.cluster_version
    if FLAGS.gke_enable_alpha:
      cmd.args.append('--enable-kubernetes-alpha')
      cmd.args.append('--no-enable-autorepair')
      cmd.args.append('--no-enable-autoupgrade')

    user = util.GetDefaultUser()
    if FLAGS.gcp_service_account:
      cmd.flags['service-account'] = FLAGS.gcp_service_account
    # Matches service accounts that either definitely belongs to this project or
    # are a GCP managed service account like the GCE default service account,
    # which we can't tell to which project they belong.
    elif re.match(SERVICE_ACCOUNT_PATTERN, user):
      logging.info('Re-using configured service-account for GKE Cluster: %s',
                   user)
      cmd.flags['service-account'] = user
      self.use_application_default_credentials = False
    else:
      logging.info('Using default GCE service account for GKE cluster')
      cmd.flags['scopes'] = 'cloud-platform'

    if self.min_nodes != self.num_nodes or self.max_nodes != self.num_nodes:
      cmd.args.append('--enable-autoscaling')
      cmd.flags['max-nodes'] = self.max_nodes
      cmd.flags['min-nodes'] = self.min_nodes

    cmd.flags['cluster-ipv4-cidr'] = f'/{_CalculateCidrSize(self.max_nodes)}'

    if self.vm_config.network:
      cmd.flags['network'] = self.vm_config.network.network_resource.name

    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    self._IssueResourceCreationCommand(cmd)

    self._CreateNodePools()

  def _CreateNodePools(self):
    """Creates additional nodepools for the cluster, if applicable."""
    for name, nodepool in six.iteritems(self.nodepools):
      cmd = util.GcloudCommand(self, 'container', 'node-pools', 'create', name,
                               '--cluster', self.name)

      self._AddNodeParamsToCmd(nodepool.vm_config, nodepool.vm_count, name, cmd)
      self._IssueResourceCreationCommand(cmd)

  def _IssueResourceCreationCommand(self, cmd):
    """Issues a command to gcloud to create resources."""

    # This command needs a long timeout due to the many minutes it
    # can take to provision a large GPU-accelerated GKE cluster.
    _, stderr, retcode = cmd.Issue(timeout=1200, raise_on_failure=False)
    if retcode:
      # Log specific type of failure, if known.
      if 'ZONE_RESOURCE_POOL_EXHAUSTED' in stderr:
        logging.exception('Container resources exhausted: %s', stderr)
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(
            'Container resources exhausted in zone %s: %s' %
            (self.zone, stderr))
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  def _AddNodeParamsToCmd(self, vm_config, num_nodes, name, cmd):
    """Modifies cmd to include node specific command arguments."""

    if vm_config.gpu_count:
      cmd.flags['accelerator'] = (
          gce_virtual_machine.GenerateAcceleratorSpecString(
              vm_config.gpu_type,
              vm_config.gpu_count))
    if vm_config.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = vm_config.min_cpu_platform

    if vm_config.threads_per_core:
      # TODO(user): Remove when threads-per-core is available in GA
      cmd.use_alpha_gcloud = True
      cmd.flags['threads-per-core'] = vm_config.threads_per_core

    if vm_config.boot_disk_size:
      cmd.flags['disk-size'] = vm_config.boot_disk_size
    if vm_config.boot_disk_type:
      cmd.flags['disk-type'] = vm_config.boot_disk_type
    if vm_config.max_local_disks:
      # TODO(pclay): Switch to local-ssd-volumes which support NVME when it
      # leaves alpha. See
      # https://cloud.google.com/sdk/gcloud/reference/alpha/container/clusters/create
      cmd.flags['local-ssd-count'] = vm_config.max_local_disks

    cmd.flags['num-nodes'] = num_nodes

    if vm_config.machine_type is None:
      cmd.flags['machine-type'] = 'custom-{0}-{1}'.format(
          vm_config.cpus,
          vm_config.memory_mib)
    else:
      cmd.flags['machine-type'] = vm_config.machine_type

    cmd.flags['node-labels'] = f'pkb_nodepool={name}'

  def _PostCreate(self):
    """Acquire cluster authentication."""
    super(GkeCluster, self)._PostCreate()
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'get-credentials', self.name)
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    cmd.IssueRetryable(env=env)

    if self.vm_config.gpu_count:
      kubernetes_helper.CreateFromFile(NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT)
      kubernetes_helper.CreateFromFile(
          data.ResourcePath(NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET))

    # GKE does not wait for kube-dns by default
    logging.info('Waiting for kube-dns')
    self.WaitForResource(
        'deployment/kube-dns',
        condition_name='Available',
        namespace='kube-system')

  def _GetInstanceGroups(self):
    cmd = util.GcloudCommand(self, 'container', 'node-pools', 'list')
    cmd.flags['cluster'] = self.name
    stdout, _, _ = cmd.Issue()
    json_output = json.loads(stdout)
    instance_groups = []
    for node_pool in json_output:
      for group_url in node_pool['instanceGroupUrls']:
        instance_groups.append(group_url.split('/')[-1])  # last url part
    return instance_groups

  def _GetInstancesFromInstanceGroup(self, instance_group_name):
    cmd = util.GcloudCommand(self, 'compute', 'instance-groups',
                             'list-instances', instance_group_name)
    stdout, _, _ = cmd.Issue()
    json_output = json.loads(stdout)
    instances = []
    for instance in json_output:
      instances.append(instance['instance'].split('/')[-1])
    return instances

  def _IsDeleting(self):
    cmd = util.GcloudCommand(self, 'container', 'clusters', 'describe',
                             self.name)
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    return True if stdout else False

  def _Delete(self):
    """Deletes the cluster."""
    super()._Delete()
    cmd = util.GcloudCommand(self, 'container', 'clusters', 'delete', self.name)
    cmd.args.append('--async')
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the cluster exits."""
    cmd = util.GcloudCommand(self, 'container', 'clusters', 'describe',
                             self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return retcode == 0
