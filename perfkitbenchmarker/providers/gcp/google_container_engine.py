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

"""Contains classes/functions related to GKE (Google Container Engine)."""

import os

from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT = 'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/k8s-1.9/nvidia-driver-installer/cos/daemonset-preloaded.yaml'
NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET = 'nvidia_unrestricted_permissions_daemonset.yml'


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
    build_cmd = util.GcloudCommand(self, 'container', 'builds', 'submit',
                                   '--tag', full_tag, image.directory)
    del build_cmd.flags['zone']
    build_cmd.Issue()


class GkeCluster(container_service.KubernetesCluster):

  CLOUD = providers.GCP

  @staticmethod
  def _GetRequiredGkeEnv():
    env = os.environ.copy()
    env['CLOUDSDK_CONTAINER_USE_APPLICATION_DEFAULT_CREDENTIALS'] = 'true'
    return env

  def __init__(self, spec):
    super(GkeCluster, self).__init__(spec)
    self.project = spec.vm_spec.project
    self.min_cpu_platform = spec.vm_spec.min_cpu_platform
    self.gce_accelerator_type_override = FLAGS.gce_accelerator_type_override
    self.cluster_version = '1.9.6-gke.0'

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super(GkeCluster, self).GetResourceMetadata()
    if self.gce_accelerator_type_override:
      result['accelerator_type_override'] = self.gce_accelerator_type_override
    return result

  def _Create(self):
    """Creates the cluster."""
    if self.gpu_count:
      # TODO(ferneyhough): Make cluster version a flag, and allow it
      # to be specified in the spec (this will require a new spec class
      # for google_container_engine however).
      cmd = util.GcloudCommand(
          self, 'beta', 'container', 'clusters', 'create', self.name)

      cmd.flags['accelerator'] = (gce_virtual_machine.
                                  GenerateAcceleratorSpecString(self.gpu_type,
                                                                self.gpu_count))
    else:
      cmd = util.GcloudCommand(
          self, 'container', 'clusters', 'create', self.name)

    cmd.flags['cluster-version'] = self.cluster_version
    cmd.flags['scopes'] = 'cloud-platform'

    if self.enable_autoscaling:
      cmd.args.append('--enable-autoscaling')
      cmd.flags['max-nodes'] = self.max_nodes
      cmd.flags['min-nodes'] = self.min_nodes

    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform

    cmd.flags['num-nodes'] = self.num_nodes
    cmd.flags['machine-type'] = self.machine_type

    # This command needs a long timeout due to the many minutes it
    # can take to provision a large GPU-accelerated GKE cluster.
    cmd.Issue(timeout=900, env=self._GetRequiredGkeEnv())

  def _PostCreate(self):
    """Acquire cluster authentication."""
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'get-credentials', self.name)
    env = self._GetRequiredGkeEnv()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    cmd.IssueRetryable(env=env)

    if self.gpu_count:
      kubernetes_helper.CreateFromFile(NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT)
      kubernetes_helper.CreateFromFile(
          data.ResourcePath(NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET))

  def _Delete(self):
    """Deletes the cluster."""
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'delete', self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns True if the cluster exits."""
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return retcode == 0
