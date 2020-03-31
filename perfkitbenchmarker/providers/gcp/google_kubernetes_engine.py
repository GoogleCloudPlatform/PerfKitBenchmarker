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
import os
import re

from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
FLAGS.kubernetes_anti_affinity = False

NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT = 'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/k8s-1.9/nvidia-driver-installer/cos/daemonset-preloaded.yaml'
NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET = 'nvidia_unrestricted_permissions_daemonset.yml'
DEFAULT_CONTAINER_VERSION = 'latest'
SERVICE_ACCOUNT_PATTERN = r'.*((?<!iam)|{project}.iam).gserviceaccount.com'


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
    """No-op because Push() handles its own auth."""
    pass

  def Push(self, image):
    """Push a locally built image to the registry."""
    full_tag = self.GetFullRegistryTag(image.name)
    tag_cmd = ['docker', 'tag', image.name, full_tag]
    vm_util.IssueCommand(tag_cmd)
    # vm_util.IssueCommand() is used here instead of util.GcloudCommand()
    # because gcloud flags cannot be appended to the command since they
    # are interpreted as docker args instead.
    push_cmd = [
        FLAGS.gcloud_path, '--project', self.project,
        'docker', '--', 'push', full_tag
    ]
    vm_util.IssueCommand(push_cmd)

  def RemoteBuild(self, image):
    """Build the image remotely."""
    full_tag = self.GetFullRegistryTag(image.name)
    build_cmd = util.GcloudCommand(self, 'builds', 'submit',
                                   '--tag', full_tag, image.directory)
    del build_cmd.flags['zone']
    build_cmd.Issue()


class GkeCluster(container_service.KubernetesCluster):
  """Class representing a Google Kubernetes Engine cluster."""

  CLOUD = providers.GCP

  def __init__(self, spec):
    super(GkeCluster, self).__init__(spec)
    self.project = spec.vm_spec.project
    self.min_cpu_platform = spec.vm_spec.min_cpu_platform
    self.gce_accelerator_type_override = FLAGS.gce_accelerator_type_override
    self.cluster_version = (FLAGS.container_cluster_version or
                            DEFAULT_CONTAINER_VERSION)
    self.use_application_default_credentials = True

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super(GkeCluster, self).GetResourceMetadata()
    if self.gce_accelerator_type_override:
      result['accelerator_type_override'] = self.gce_accelerator_type_override
    result['container_cluster_version'] = self.cluster_version
    return result

  def _Create(self):
    """Creates the cluster."""
    if self.min_cpu_platform or self.gpu_count:
      cmd = util.GcloudCommand(
          self, 'beta', 'container', 'clusters', 'create', self.name)
    else:
      cmd = util.GcloudCommand(
          self, 'container', 'clusters', 'create', self.name)

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
      logging.info(
          'Re-using configured service-account for GKE Cluster: %s', user)
      cmd.flags['service-account'] = user
      self.use_application_default_credentials = False
    else:
      logging.info('Using default GCE service account for GKE cluster')
      cmd.flags['scopes'] = 'cloud-platform'

    if self.gpu_count:
      cmd.flags['accelerator'] = (gce_virtual_machine.
                                  GenerateAcceleratorSpecString(self.gpu_type,
                                                                self.gpu_count))
    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform

    if self.min_nodes != self.num_nodes or self.max_nodes != self.num_nodes:
      cmd.args.append('--enable-autoscaling')
      cmd.flags['max-nodes'] = self.max_nodes
      cmd.flags['min-nodes'] = self.min_nodes

    cmd.flags['num-nodes'] = self.num_nodes

    if self.machine_type is None:
      cmd.flags['machine-type'] = 'custom-{0}-{1}'.format(
          self.cpus, self.memory)
    else:
      cmd.flags['machine-type'] = self.machine_type

    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()

    # This command needs a long timeout due to the many minutes it
    # can take to provision a large GPU-accelerated GKE cluster.
    _, stderr, retcode = cmd.Issue(
        timeout=1200, raise_on_failure=False)
    if retcode != 0:
      # Log specific type of failure, if known.
      if 'ZONE_RESOURCE_POOL_EXHAUSTED' in stderr:
        logging.exception('Container resources exhausted: %s', stderr)
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(
            'Container resources exhausted in zone %s: %s' %
            (self.zone, stderr))
      raise errors.Resource.CreationError(stderr)

  def _PostCreate(self):
    """Acquire cluster authentication."""
    super(GkeCluster, self)._PostCreate()
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'get-credentials', self.name)
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    cmd.IssueRetryable(env=env)

    self._AddTags()

    if self.gpu_count:
      kubernetes_helper.CreateFromFile(NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT)
      kubernetes_helper.CreateFromFile(
          data.ResourcePath(NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET))

  def _AddTags(self):
    """Tags all VMs in the cluster."""
    vms_in_cluster = []
    for instance_group in self._GetInstanceGroups():
      vms_in_cluster.extend(self._GetInstancesFromInstanceGroup(instance_group))

    for vm_name in vms_in_cluster:
      cmd = util.GcloudCommand(self, 'compute', 'instances', 'add-metadata',
                               vm_name)
      cmd.flags['metadata'] = util.MakeFormattedDefaultTags()
      cmd.Issue()

      cmd = util.GcloudCommand(self, 'compute', 'disks', 'add-labels', vm_name)
      cmd.flags['labels'] = util.MakeFormattedDefaultTags()
      cmd.Issue()

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
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'describe', self.name)
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    return True if stdout else False

  def _Delete(self):
    """Deletes the cluster."""
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'delete', self.name)
    cmd.args.append('--async')
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the cluster exits."""
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return retcode == 0
