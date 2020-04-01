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

"""Contains code related to lifecycle management of Kubernetes Pods."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import posixpath

from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.kubernetes import kubernetes_disk
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT
import six

FLAGS = flags.FLAGS

SELECTOR_PREFIX = 'pkb'


class KubernetesVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Kubernetes POD."""
  CLOUD = providers.KUBERNETES
  DEFAULT_IMAGE = None
  CONTAINER_COMMAND = None
  HOME_DIR = '/root'
  IS_REBOOTABLE = False

  def __init__(self, vm_spec):
    """Initialize a Kubernetes virtual machine.

    Args:
      vm_spec: KubernetesPodSpec object of the vm.
    """
    super(KubernetesVirtualMachine, self).__init__(vm_spec)
    self.num_scratch_disks = 0
    self.name = self.name.replace('_', '-')
    self.user_name = FLAGS.username
    self.image = self.image or self.DEFAULT_IMAGE
    self.resource_limits = vm_spec.resource_limits
    self.resource_requests = vm_spec.resource_requests

  def GetResourceMetadata(self):
    metadata = super(KubernetesVirtualMachine, self).GetResourceMetadata()
    if self.resource_limits:
      metadata.update({
          'pod_cpu_limit': self.resource_limits.cpus,
          'pod_memory_limit_mb': self.resource_limits.memory,
      })
    if self.resource_requests:
      metadata.update({
          'pod_cpu_request': self.resource_requests.cpus,
          'pod_memory_request_mb': self.resource_requests.memory,
      })
    return metadata

  def _CreateDependencies(self):
    self._CheckPrerequisites()
    self._CreateVolumes()

  def _DeleteDependencies(self):
    self._DeleteVolumes()

  def _Create(self):
    self._CreatePod()
    self._WaitForPodBootCompletion()

  @vm_util.Retry()
  def _PostCreate(self):
    self._GetInternalIp()
    self._ConfigureProxy()
    self._SetupDevicesPaths()

  def _Delete(self):
    self._DeletePod()

  def _CheckPrerequisites(self):
    """Exits if any of the prerequisites is not met."""
    if not FLAGS.kubectl:
      raise Exception('Please provide path to kubectl tool using --kubectl '
                      'flag. Exiting.')
    if not FLAGS.kubeconfig:
      raise Exception('Please provide path to kubeconfig using --kubeconfig '
                      'flag. Exiting.')
    if self.disk_specs and self.disk_specs[0].disk_type == disk.STANDARD:
      if not FLAGS.ceph_monitors:
        raise Exception('Please provide a list of Ceph Monitors using '
                        '--ceph_monitors flag.')

  def _CreatePod(self):
    """Creates a POD (Docker container with optional volumes)."""
    create_rc_body = self._BuildPodBody()
    logging.info('About to create a pod with the following configuration:')
    logging.info(create_rc_body)
    kubernetes_helper.CreateResource(create_rc_body)

  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=False)
  def _WaitForPodBootCompletion(self):
    """
    Need to wait for the PODs to get up - PODs are created with a little delay.
    """
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'pod', '-o=json', self.name]
    logging.info('Waiting for POD %s', self.name)
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True,
                                          raise_on_failure=False)
    if pod_info:
      pod_info = json.loads(pod_info)
      containers = pod_info['spec']['containers']
      if len(containers) == 1:
        pod_status = pod_info['status']['phase']
        if (containers[0]['name'].startswith(self.name)
            and pod_status == 'Running'):
          logging.info('POD is up and running.')
          return
    raise Exception('POD %s is not running. Retrying to check status.' %
                    self.name)

  def _DeletePod(self):
    """Deletes a POD."""
    delete_pod = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                  'delete', 'pod', self.name]
    output = vm_util.IssueCommand(delete_pod, raise_on_failure=False)
    logging.info(output[STDOUT].rstrip())

  @vm_util.Retry(poll_interval=10, max_retries=20)
  def _Exists(self):
    """POD should have been already created but this is a double check."""
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'pod', '-o=json', self.name]
    pod_info, _, _ = vm_util.IssueCommand(
        exists_cmd, suppress_warning=True, raise_on_failure=False)
    if pod_info:
      return True
    return False

  def _CreateVolumes(self):
    """
    Creates volumes for scratch disks. These volumes have to be created
    BEFORE containers creation because Kubernetes doesn't allow to attach
    volume to currently running containers.
    """
    self.scratch_disks = kubernetes_disk.CreateDisks(self.disk_specs, self.name)

  @vm_util.Retry(poll_interval=10, max_retries=20, log_errors=False)
  def _DeleteVolumes(self):
    """Deletes volumes."""
    for scratch_disk in self.scratch_disks[:]:
      scratch_disk.Delete()
      self.scratch_disks.remove(scratch_disk)

  def DeleteScratchDisks(self):
    pass

  def _GetInternalIp(self):
    """Gets the POD's internal ip address."""
    pod_ip = kubernetes_helper.Get(
        'pods', self.name, '', '.status.podIP')

    if not pod_ip:
      raise Exception('Internal POD IP address not found. Retrying.')

    self.internal_ip = pod_ip
    self.ip_address = pod_ip

  def _ConfigureProxy(self):
    """
    In Docker containers environment variables from /etc/environment
    are not sourced - this results in connection problems when running
    behind proxy. Prepending proxy environment variables to bashrc
    solves the problem. Note: APPENDING to bashrc will not work because
    the script exits when it is NOT executed in interactive shell.
    """

    if FLAGS.http_proxy:
      http_proxy = 'sed -i \'1i export http_proxy=%s\' /etc/bash.bashrc'
      self.RemoteCommand(http_proxy % FLAGS.http_proxy)
    if FLAGS.https_proxy:
      https_proxy = 'sed -i \'1i export https_proxy=%s\' /etc/bash.bashrc'
      self.RemoteCommand(https_proxy % FLAGS.http_proxy)
    if FLAGS.ftp_proxy:
      ftp_proxy = 'sed -i \'1i export ftp_proxy=%s\' /etc/bash.bashrc'
      self.RemoteCommand(ftp_proxy % FLAGS.ftp_proxy)

  def _SetupDevicesPaths(self):
    """Sets the path to each scratch disk device."""
    for scratch_disk in self.scratch_disks:
      scratch_disk.SetDevicePath(self)

  def _BuildPodBody(self):
    """
    Builds a JSON which will be passed as a body of POST request
    to Kuberneres API in order to create a POD.
    """

    container = self._BuildContainerBody()
    volumes = self._BuildVolumesBody()

    template = {
        'kind': 'Pod',
        'apiVersion': 'v1',
        'metadata': {
            'name': self.name,
            'labels': {
                SELECTOR_PREFIX: self.name
            }
        },
        'spec': {
            'volumes': volumes,
            'containers': [container],
            'dnsPolicy': 'ClusterFirst',
        }
    }
    if FLAGS.kubernetes_anti_affinity:
      template['spec']['affinity'] = {
          'podAntiAffinity': {
              'requiredDuringSchedulingIgnoredDuringExecution': [{
                  'labelSelector': {
                      'matchExpressions': [{
                          'key': 'pkb_anti_affinity',
                          'operator': 'In',
                          'values': [''],
                      }],
                  },
                  'topologyKey': 'kubernetes.io/hostname',
              }],
          },
      }
      template['metadata']['labels']['pkb_anti_affinity'] = ''

    return json.dumps(template)

  def _BuildVolumesBody(self):
    """Constructs volumes-related part of POST request to create POD."""
    volumes = []

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeInfo(volumes)

    return volumes

  def _BuildContainerBody(self):
    """Constructs containers-related part of POST request to create POD."""
    registry = getattr(context.GetThreadBenchmarkSpec(), 'registry', None)
    if (not FLAGS.static_container_image and
        registry is not None):
      image = registry.GetFullRegistryTag(self.image)
    else:
      image = self.image
    container = {
        'image': image,
        'name': self.name,
        'workingDir': self.HOME_DIR,
        'securityContext': {
            'privileged': FLAGS.docker_in_privileged_mode
        },
        'volumeMounts': [
        ]
    }

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeMountInfo(container['volumeMounts'])

    resource_body = self._BuildResourceBody()
    if resource_body:
      container['resources'] = resource_body

    if self.CONTAINER_COMMAND:
      container['command'] = self.CONTAINER_COMMAND

    return container

  def _BuildResourceBody(self):
    """Constructs a dictionary that specifies resource limits and requests.

    The syntax for including GPUs is specific to GKE and is likely to
    change in the future.
    See https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus

    Returns:
      kubernetes pod resource body containing pod limits and requests.
    """
    resources = {
        'limits': {},
        'requests': {},
    }

    if self.resource_requests:
      resources['requests'].update({
          'cpu': str(self.resource_requests.cpus),
          'memory': '{0}Mi'.format(self.resource_requests.memory),
      })

    if self.resource_limits:
      resources['limits'].update({
          'cpu': str(self.resource_limits.cpus),
          'memory': '{0}Mi'.format(self.resource_limits.memory),
      })

    if self.gpu_count:
      gpu_dict = {
          'nvidia.com/gpu': str(self.gpu_count)
      }
      resources['limits'].update(gpu_dict)
      resources['requests'].update(gpu_dict)

    result_with_empty_values_removed = ({
        k: v for k, v in six.iteritems(resources) if v
    })
    return result_with_empty_values_removed


class DebianBasedKubernetesVirtualMachine(
    KubernetesVirtualMachine, linux_virtual_machine.BaseDebianMixin):
  """Base class for Debian based containers running inside k8s."""

  def RemoteHostCommandWithReturnCode(self, command,
                                      should_log=False, retries=None,
                                      ignore_failure=False, login_shell=False,
                                      suppress_warning=False, timeout=None):
    """Runs a command in the Kubernetes container."""
    cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'exec', '-i',
           self.name, '--', '/bin/bash', '-c', command]
    stdout, stderr, retcode = vm_util.IssueCommand(
        cmd, force_info_log=should_log,
        suppress_warning=suppress_warning, timeout=timeout,
        raise_on_failure=False)
    if not ignore_failure and retcode:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'Full command: %s\nSTDOUT: %sSTDERR: %s' %
                    (retcode, command, ' '.join(cmd),
                     stdout, stderr))
      raise errors.VirtualMachine.RemoteCommandError(error_text)
    return stdout, stderr, retcode

  def MoveHostFile(self, target, source_path, remote_path=''):
    """Copies a file from one VM to a target VM.

    Args:
      target: The target BaseVirtualMachine object.
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the TARGET machine, default
          is the home directory.
    """
    file_name = vm_util.PrependTempDir(posixpath.basename(source_path))
    self.RemoteHostCopy(file_name, source_path, copy_to=False)
    target.RemoteHostCopy(file_name, remote_path)

  def RemoteHostCopy(self, file_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    if copy_to:
      file_name = posixpath.basename(file_path)
      src_spec, dest_spec = file_path, '%s:%s' % (self.name, file_name)
    else:
      remote_path, _ = self.RemoteCommand('readlink -f %s' % remote_path)
      remote_path = remote_path.strip()
      src_spec, dest_spec = '%s:%s' % (self.name, remote_path), file_path
    cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
           'cp', src_spec, dest_spec]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, ' '.join(cmd), stdout, stderr))
      raise errors.VirtualMachine.RemoteCommandError(error_text)
    if copy_to:
      file_name = posixpath.basename(file_path)
      remote_path = remote_path or file_name
      self.RemoteCommand('mv %s %s; chmod 777 %s' %
                         (file_name, remote_path, remote_path))

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def PrepareVMEnvironment(self):
    super(DebianBasedKubernetesVirtualMachine, self).PrepareVMEnvironment()
    # Don't rely on SSH being installed in Kubernetes containers,
    # so install it and restart the service so that it is ready to go.
    # Although ssh is not required to connect to the container, MPI
    # benchmarks require it.
    self.InstallPackages('ssh')
    self.RemoteCommand('sudo /etc/init.d/ssh restart', ignore_failure=True)
    self.RemoteCommand('mkdir -p ~/.ssh')
    with open(self.ssh_public_key) as f:
      key = f.read()
      self.RemoteCommand('echo "%s" >> ~/.ssh/authorized_keys' % key)
    self.Install('python')

    # Needed for the MKL math library.
    self.InstallPackages('cpio')

    # Don't assume the relevant CLI is installed in the Kubernetes environment.
    if FLAGS.container_cluster_cloud == 'GCP':
      self.InstallGcloudCli()
    elif FLAGS.container_cluster_cloud == 'AWS':
      self.InstallAwsCli()
    elif FLAGS.container_cluster_cloud == 'Azure':
      self.InstallAzureCli()

  def InstallAwsCli(self):
    """Installs the AWS CLI; used for downloading preprovisioned data."""
    self.Install('aws_credentials')
    self.Install('awscli')

  def InstallAzureCli(self):
    """Installs the Azure CLI; used for downloading preprovisioned data."""
    self.Install('azure_cli')
    self.Install('azure_credentials')

  # TODO(ferneyhough): Consider making this a package.
  def InstallGcloudCli(self):
    """Installs the Gcloud CLI; used for downloading preprovisioned data."""
    self.InstallPackages('curl')
    # The driver /usr/lib/apt/methods/https is sometimes needed for apt-get.
    self.InstallPackages('apt-transport-https')
    self.RemoteCommand('echo "deb https://packages.cloud.google.com/apt '
                       'cloud-sdk-$(lsb_release -c -s) main" | sudo tee -a '
                       '/etc/apt/sources.list.d/google-cloud-sdk.list')
    self.RemoteCommand('curl https://packages.cloud.google.com/apt/doc/'
                       'apt-key.gpg | sudo apt-key add -')
    self.RemoteCommand('sudo apt-get update && sudo apt-get install '
                       '-y google-cloud-sdk')

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
    """Downloads a preprovisioned data file.

    This function works by looking up the VirtualMachine class which matches
    the cloud we are running on (defined by FLAGS.container_cluster_cloud).

    Then we look for a module-level function defined in the same module as
    the VirtualMachine class which generates a string used to download
    preprovisioned data for the given cloud.

    Note that this implementation is specific to Debian OS types.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.

    Raises:
      NotImplementedError: if this method does not support the specified cloud.
      AttributeError: if the VirtualMachine class does not implement
        GenerateDownloadPreprovisionedDataCommand.
    """
    cloud = FLAGS.container_cluster_cloud
    if cloud == 'GCP':
      download_function = (gce_virtual_machine.
                           GenerateDownloadPreprovisionedDataCommand)
    elif cloud == 'AWS':
      download_function = (aws_virtual_machine.
                           GenerateDownloadPreprovisionedDataCommand)
    elif cloud == 'Azure':
      download_function = (azure_virtual_machine.
                           GenerateDownloadPreprovisionedDataCommand)
    else:
      raise NotImplementedError(
          'Cloud {0} does not support downloading preprovisioned '
          'data on Kubernetes VMs.'.format(cloud))

    self.RemoteCommand(
        download_function(install_path, module_name, filename))

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    cloud = FLAGS.container_cluster_cloud
    if cloud == 'GCP' and FLAGS.gcp_preprovisioned_data_bucket:
      stat_function = (gce_virtual_machine.
                       GenerateStatPreprovisionedDataCommand)
    elif cloud == 'AWS' and FLAGS.aws_preprovisioned_data_bucket:
      stat_function = (aws_virtual_machine.
                       GenerateStatPreprovisionedDataCommand)
    elif cloud == 'Azure' and FLAGS.azure_preprovisioned_data_bucket:
      stat_function = (azure_virtual_machine.
                       GenerateStatPreprovisionedDataCommand)
    else:
      return False
    return self.TryRemoteCommand(stat_function(module_name, filename))


def _install_sudo_command():
  """Return a bash command that installs sudo and runs tail indefinitely.

  This is useful for some docker images that don't have sudo installed.

  Returns:
    a sequence of arguments that use bash to install sudo and never run
    tail indefinitely.
  """
  # The canonical ubuntu images as well as the nvidia/cuda
  # image do not have sudo installed so install it and configure
  # the sudoers file such that the root user's environment is
  # preserved when running as sudo. Then run tail indefinitely so that
  # the container does not exit.
  container_command = ' && '.join([
      'apt-get update',
      'apt-get install -y sudo',
      'sed -i \'/env_reset/d\' /etc/sudoers',
      'sed -i \'/secure_path/d\' /etc/sudoers',
      'sudo ldconfig',
      'tail -f /dev/null',
  ])
  return ['bash', '-c', container_command]

# All Ubuntu images below are from https://hub.docker.com/_/ubuntu/
# Note that they do not include all packages that are typically
# included with Ubuntu. For example, sudo is not installed.
# KubernetesVirtualMachine takes care of this by installing
# sudo in the container startup script.


class Ubuntu1604BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin):
  DEFAULT_IMAGE = 'ubuntu:16.04'
  CONTAINER_COMMAND = _install_sudo_command()


class Ubuntu1710BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine, linux_virtual_machine.Ubuntu1710Mixin):
  DEFAULT_IMAGE = 'ubuntu:17.10'
  CONTAINER_COMMAND = _install_sudo_command()


class Ubuntu1604Cuda9BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine,
    linux_virtual_machine.Ubuntu1604Cuda9Mixin):
  # Image is from https://hub.docker.com/r/nvidia/cuda/
  DEFAULT_IMAGE = 'nvidia/cuda:9.0-devel-ubuntu16.04'
  CONTAINER_COMMAND = _install_sudo_command()
