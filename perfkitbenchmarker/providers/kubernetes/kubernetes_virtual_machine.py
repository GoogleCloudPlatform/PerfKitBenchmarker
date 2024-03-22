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

import json
import logging
import os
import posixpath
import stat
from typing import Any, Optional, Union

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import google_cloud_sdk
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.kubernetes import flags as k8s_flags
from perfkitbenchmarker.providers.kubernetes import kubernetes_disk
from perfkitbenchmarker.providers.kubernetes import kubernetes_pod_spec
from perfkitbenchmarker.providers.kubernetes import kubernetes_resources_spec

FLAGS = flags.FLAGS
# Using root logger removes one function call logging.info adds.
logger = logging.getLogger()

SELECTOR_PREFIX = 'pkb'


def _IsKubectlErrorEphemeral(retcode: int, stderr: str) -> bool:
  """Determine if kubectl error is retriable."""
  return retcode == 1 and (
      'error dialing backend:' in stderr
      or 'connect: connection timed out' in stderr
  )


class KubernetesVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Kubernetes POD.

  Attributes:
    name: Name of the resource.
    user_name: Name of the user for ssh commands.
    image: Name of the image.
    resource_limits: Object with cpu & memory representing max limits.
    resource_requests: Object with cpu & memory representing requested size.
    cloud: Represents the regular Cloud (AWS, GCP, Azure) used.
    k8s_sriov_network: Name of a network with sriov enabled.
  """

  CLOUD: Union[list[str], str] = [
      provider_info.AWS,
      provider_info.AZURE,
      provider_info.GCP,
  ]
  PLATFORM: str = provider_info.KUBERNETES
  DEFAULT_IMAGE = None
  HOME_DIR = '/root'
  IS_REBOOTABLE = False

  def __init__(self, vm_spec: kubernetes_pod_spec.KubernetesPodSpec):
    """Initialize a Kubernetes virtual machine.

    Args:
      vm_spec: KubernetesPodSpec object of the vm.
    """
    super().__init__(vm_spec)
    self.name: str = self.name.replace('_', '-')
    self.user_name: str = FLAGS.username
    self.image: str = self.image or self.DEFAULT_IMAGE
    self.resource_limits: Optional[
        kubernetes_resources_spec.KubernetesResourcesSpec
    ] = vm_spec.resource_limits
    self.resource_requests: Optional[
        kubernetes_resources_spec.KubernetesResourcesSpec
    ] = vm_spec.resource_requests
    self.cloud: str = (
        self.CLOUD  # pytype: disable=annotation-type-mismatch
        if isinstance(self.CLOUD, str) else FLAGS.cloud
    )
    self.sriov_network: Optional[str] = FLAGS.k8s_sriov_network or None

  def GetResourceMetadata(self):
    metadata = super().GetResourceMetadata()
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
    if self.sriov_network:
      metadata.update({'annotations': {'sriov_network': self.sriov_network}})
    return metadata

  def _CreateDependencies(self):
    self._CheckPrerequisites()
    self._CreateVolumes()

  def _DeleteDependencies(self):
    self._DeleteVolumes()

  @vm_util.Retry()
  def _PostCreate(self):
    self._GetInternalIp()
    self._ConfigureProxy()
    self._SetupDevicesPaths()

  # Kubernetes VMs do not implement _Start or _Stop
  def _Start(self):
    """Starts the VM."""
    raise NotImplementedError()

  def _Stop(self):
    """Stops the VM."""
    raise NotImplementedError()

  def _CheckPrerequisites(self):
    """Exits if any of the prerequisites is not met."""
    if not FLAGS.kubectl:
      raise errors.Setup.InvalidFlagConfigurationError(
          'Please provide path to kubectl tool using --kubectl flag. Exiting.'
      )
    if not FLAGS.kubeconfig:
      raise errors.Setup.InvalidFlagConfigurationError(
          'Please provide path to kubeconfig using --kubeconfig flag. Exiting.'
      )
    if self.disk_specs and self.disk_specs[0].disk_type == disk.STANDARD:
      if not FLAGS.ceph_monitors:
        raise errors.Setup.InvalidFlagConfigurationError(
            'Please provide a list of Ceph Monitors using --ceph_monitors flag.'
        )

  def _Create(self):
    """Creates a POD (Docker container with optional volumes)."""
    create_rc_body = self._BuildPodBody()
    logging.info('About to create a pod with the following configuration:')
    logging.info(create_rc_body)
    kubernetes_helper.CreateResource(create_rc_body)

  def _IsReady(self) -> bool:
    """Need to wait for the PODs to get up, they're created with a little delay."""
    exists_cmd = [
        FLAGS.kubectl,
        '--kubeconfig=%s' % FLAGS.kubeconfig,
        'get',
        'pod',
        '-o=json',
        self.name,
    ]
    logging.info('Waiting for POD %s', self.name)
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, raise_on_failure=False)
    if pod_info:
      pod_info = json.loads(pod_info)
      containers = pod_info['spec']['containers']
      if len(containers) == 1:
        pod_status = pod_info['status']['phase']
        if (
            containers[0]['name'].startswith(self.name)
            and pod_status == 'Running'
        ):
          logging.info('POD is up and running.')
          return True
    return False

  def WaitForBootCompletion(self):
    """No-op, because waiting for boot completion covered by _IsReady."""
    self.bootable_time = self.resource_ready_time

  def _Delete(self):
    """Deletes a POD."""
    delete_pod = [
        FLAGS.kubectl,
        '--kubeconfig=%s' % FLAGS.kubeconfig,
        'delete',
        'pod',
        self.name,
    ]
    stdout, _, _ = vm_util.IssueCommand(delete_pod, raise_on_failure=False)
    logging.info(stdout.rstrip())

  @vm_util.Retry(poll_interval=10, max_retries=20)
  def _Exists(self) -> bool:
    """POD should have been already created but this is a double check."""
    exists_cmd = [
        FLAGS.kubectl,
        '--kubeconfig=%s' % FLAGS.kubeconfig,
        'get',
        'pod',
        '-o=json',
        self.name,
    ]
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, raise_on_failure=False)
    if pod_info:
      return True
    return False

  def _CreateVolumes(self):
    """Creates volumes for scratch disks.

    These volumes have to be created BEFORE containers creation because
    Kubernetes doesn't allow to attach volume to currently running containers.
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
    pod_ip = kubernetes_helper.Get('pods', self.name, '', '.status.podIP')

    if not pod_ip:
      raise Exception('Internal POD IP address not found. Retrying.')

    self.internal_ip = pod_ip
    self.ip_address = pod_ip
    if self.sriov_network:
      annotations = json.loads(
          kubernetes_helper.Get('pods', self.name, '', '.metadata.annotations')
      )
      sriov_ip = json.loads(annotations['k8s.v1.cni.cncf.io/network-status'])[
          1
      ]['ips'][0]

      if not sriov_ip:
        raise Exception('SRIOV interface ip address not found. Retrying.')

      self.internal_ip = sriov_ip
      self.ip_address = sriov_ip

  def _ConfigureProxy(self):
    """Configures Proxy.

    In Docker containers environment variables from /etc/environment
    are not sourced - this results in connection problems when running
    behind proxy. Prepending proxy environment variables to bashrc
    solves the problem. Note: APPENDING to bashrc will not work because
    the script exits when it is NOT executed in interactive shell.
    """

    if FLAGS.http_proxy:
      http_proxy = "sed -i '1i export http_proxy=%s' /etc/bash.bashrc"
      self.RemoteCommand(http_proxy % FLAGS.http_proxy)
    if FLAGS.https_proxy:
      https_proxy = "sed -i '1i export https_proxy=%s' /etc/bash.bashrc"
      self.RemoteCommand(https_proxy % FLAGS.http_proxy)
    if FLAGS.ftp_proxy:
      ftp_proxy = "sed -i '1i export ftp_proxy=%s' /etc/bash.bashrc"
      self.RemoteCommand(ftp_proxy % FLAGS.ftp_proxy)

  def _SetupDevicesPaths(self):
    """Sets the path to each scratch disk device."""
    for scratch_disk in self.scratch_disks:
      scratch_disk.SetDevicePath(self)

  def _BuildPodBody(self) -> str:
    """Builds a JSON.

    This will be passed as a body of POST request Kuberneres API in order to
    create a POD.

    Returns:
      json string for POD creation.
    """

    container = self._BuildContainerBody()
    volumes = self._BuildVolumesBody()

    template = {
        'kind': 'Pod',
        'apiVersion': 'v1',
        'metadata': {'name': self.name, 'labels': {SELECTOR_PREFIX: self.name}},
        'spec': {
            'volumes': volumes,
            'containers': [container],
            'dnsPolicy': 'ClusterFirst',
            'tolerations': [{
                'key': 'kubernetes.io/arch',
                'operator': 'Exists',
                'effect': 'NoSchedule',
            }],
        },
    }

    if k8s_flags.USE_NODE_SELECTORS.value and self.vm_group:
      if self.vm_group == 'default':
        nodepool = k8s_flags.DEFAULT_VM_GROUP_NODEPOOL.value
      else:
        nodepool = self.vm_group
      template['spec']['nodeSelector'] = {
          'pkb_nodepool': container_service.NodePoolName(nodepool)
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

    if self.sriov_network:
      annotations = self._addAnnotations()
      template['metadata'].update({'annotations': annotations})
    return json.dumps(template)

  def _BuildVolumesBody(self) -> list[dict[str, Any]]:
    """Constructs volumes-related part of POST request to create POD."""
    volumes = []

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeInfo(volumes)

    return volumes

  def _BuildContainerBody(self) -> dict[str, Any]:
    """Constructs containers-related part of POST request to create POD."""
    registry = getattr(context.GetThreadBenchmarkSpec(), 'registry', None)
    if not FLAGS.static_container_image and registry is not None:
      image = registry.GetFullRegistryTag(self.image)
    else:
      image = self.image
    container = {
        'image': image,
        'name': self.name,
        'workingDir': self.HOME_DIR,
        'securityContext': {'privileged': FLAGS.docker_in_privileged_mode},
        'volumeMounts': [],
    }

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeMountInfo(container['volumeMounts'])

    resource_body = self._BuildResourceBody()
    if resource_body:
      container['resources'] = resource_body

    # Tail /dev/null as a means of keeping the container alive.
    container['command'] = ['tail', '-f', '/dev/null']

    return container

  def _addAnnotations(self) -> dict[str, str]:
    """Constructs annotations required for Kubernetes."""
    annotations = {}
    if self.sriov_network:
      annotations.update({'k8s.v1.cni.cncf.io/networks': self.sriov_network})
    return annotations

  def _BuildResourceBody(self) -> dict[str, Any]:
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
      gpu_dict = {'nvidia.com/gpu': str(self.gpu_count)}
      resources['limits'].update(gpu_dict)
      resources['requests'].update(gpu_dict)

    result_with_empty_values_removed = {k: v for k, v in resources.items() if v}
    return result_with_empty_values_removed


class DebianBasedKubernetesVirtualMachine(
    KubernetesVirtualMachine, linux_virtual_machine.BaseDebianMixin
):
  """Base class for Debian based containers running inside k8s."""

  def RemoteHostCommandWithReturnCode(
      self,
      command: str,
      retries: Optional[int] = None,
      ignore_failure: bool = False,
      login_shell: bool = False,
      timeout: Optional[float] = None,
      ip_address: Optional[str] = None,
      should_pre_log: bool = True,
      stack_level: int = 1,
  ):
    """Runs a command in the Kubernetes container using kubectl.

    Args:
      command: A bash command string.
      retries: The maximum number of times to retry SSHing 255 errors.
      ignore_failure: If true, ignores failures rather than raising an
        Exception.
      login_shell: If true, runs commands in a login shell.
      timeout: The timeout for the command.
      ip_address: Should always be None; incompatible with Kubernetes.
      should_pre_log: Whether to output a "Running command" log statement.
      stack_level: The number of stack frames to skip for an "interesting"
        callsite to be logged.

    Returns:
      A tuple of stdout, stderr, return_code from the command.

    Raises:
      RemoteCommandError If there was an error & ignore_failure is false.
    """
    if ip_address:
      raise AssertionError('Kubernetes VMs cannot use IP')
    if retries is None:
      retries = FLAGS.ssh_retries
    stack_level += 1
    if should_pre_log:
      logging.info(
          'Running on kubernetes %s cmd: %s',
          self.name,
          command,
          stacklevel=stack_level,
      )
    cmd = [
        FLAGS.kubectl,
        '--kubeconfig=%s' % FLAGS.kubeconfig,
        'exec',
        '-i',
        self.name,
        '--',
        '/bin/bash',
        '-c',
        command,
    ]
    for _ in range(retries):
      stdout, stderr, retcode = vm_util.IssueCommand(
          cmd,
          timeout=timeout,
          raise_on_failure=False,
          stack_level=stack_level,
          should_pre_log=False,
      )
      # Check for ephemeral connection issues.
      if not _IsKubectlErrorEphemeral(retcode, stderr):
        break
      logger.info(
          'Retrying ephemeral connection issue\n:%s',
          stderr,
      )
    if not ignore_failure and retcode:
      error_text = (
          'Got non-zero return code (%s) executing %s\n'
          'Full command: %s\nSTDOUT: %sSTDERR: %s'
          % (retcode, command, ' '.join(cmd), stdout, stderr)
      )
      raise errors.VirtualMachine.RemoteCommandError(error_text)
    return stdout, stderr, retcode

  def MoveHostFile(
      self,
      target: virtual_machine.BaseVirtualMachine,
      source_path: str,
      remote_path: str = '',
  ):
    """Copies a file from one VM to a target VM.

    Args:
      target: The target BaseVirtualMachine object.
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the TARGET machine, default is
        the home directory.
    """
    file_name = vm_util.PrependTempDir(posixpath.basename(source_path))
    self.RemoteHostCopy(file_name, source_path, copy_to=False)
    target.RemoteHostCopy(file_name, remote_path)

  def RemoteHostCopy(
      self,
      file_path: str,
      remote_path: str = '',
      copy_to: bool = True,
      retries: Optional[int] = None,
  ):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.
      retries: Number of attempts for the copy

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    if copy_to:
      file_name = posixpath.basename(file_path)
      src_spec, dest_spec = file_path, '%s:%s' % (self.name, file_name)
    else:
      remote_path, _ = self.RemoteCommand('readlink -f %s' % remote_path)
      remote_path = remote_path.strip()
      file_name = posixpath.basename(remote_path)
      try:
        # kubectl cannot copy into a directory. Only to a new file
        # https://github.com/kubernetes/kubernetes/pull/81782
        if stat.S_ISDIR(os.stat(file_path).st_mode):
          file_path = os.path.join(file_path, file_name)
      except FileNotFoundError:
        # file_path is already a non-existent file
        pass
      src_spec, dest_spec = '%s:%s' % (self.name, remote_path), file_path
    if retries is None:
      retries = FLAGS.ssh_retries
    for _ in range(retries):
      cmd = [
          FLAGS.kubectl,
          '--kubeconfig=%s' % FLAGS.kubeconfig,
          'cp',
          src_spec,
          dest_spec,
      ]
      stdout, stderr, retcode = vm_util.IssueCommand(
          cmd, raise_on_failure=False
      )
      if not _IsKubectlErrorEphemeral(retcode, stderr):
        break
      logging.info('Retrying ephemeral connection issue\n:%s', stderr)
    if retcode:
      error_text = (
          'Got non-zero return code (%s) executing %s\nSTDOUT: %sSTDERR: %s'
          % (retcode, ' '.join(cmd), stdout, stderr)
      )
      raise errors.VirtualMachine.RemoteCommandError(error_text)
    if copy_to:
      remote_path = remote_path or file_name
      self.RemoteCommand(
          'mv %s %s; chmod 755 %s' % (file_name, remote_path, remote_path)
      )
    # TODO(pclay): Validate directories
    if not stat.S_ISDIR(os.stat(file_path).st_mode):
      # Validate file sizes
      # Sometimes kubectl cp seems to gracefully truncate the file.
      local_size = os.path.getsize(file_path)
      stdout, _ = self.RemoteCommand(f'stat -c %s {remote_path}')
      remote_size = int(stdout)
      if local_size != remote_size:
        raise errors.VirtualMachine.RemoteCommandError(
            f'Failed to copy {file_name}. '
            f'Remote size {remote_size} != local size {local_size}'
        )

  def PrepareVMEnvironment(self):
    # Install sudo as most PrepareVMEnvironment assume it exists.
    self._InstallPrepareVmEnvironmentDependencies()
    super(DebianBasedKubernetesVirtualMachine, self).PrepareVMEnvironment()
    if k8s_flags.SETUP_SSH.value:
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

    # Ubuntu docker images are based on Minimal Ubuntu
    # https://wiki.ubuntu.com/Minimal
    # The VM images PKB uses are based on a full Ubuntu Server flavor and have a
    # bunch of useful utilities
    # Utilities packages install here so that we
    # have similar base packages. This is essentially the same as running
    # unminimize.
    # ubuntu-minimal contains iputils-ping
    # ubuntu-server contains curl, net-tools, software-properties-common
    # ubuntu-standard contains wget
    # TODO(pclay): Revisit if Debian or RHEL images are added.
    self.InstallPackages('ubuntu-minimal ubuntu-server ubuntu-standard')

  def DownloadPreprovisionedData(
      self,
      install_path: str,
      module_name: str,
      filename: str,
      timeout: int = virtual_machine.PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Downloads a preprovisioned data file.

    This function works by looking up the VirtualMachine class which matches
    the cloud we are running on. Each cloud's VM module has a function to
    generate the preprovisioned data command, which we run from here.

    Note that this implementation is specific to Debian OS types.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout for the command.

    Raises:
      NotImplementedError: if this method does not support the specified cloud.
      AttributeError: if the VirtualMachine class does not implement
        GenerateDownloadPreprovisionedDataCommand.
    """
    if self.cloud == 'GCP':
      download_function = (
          gce_virtual_machine.GenerateDownloadPreprovisionedDataCommand
      )
    elif self.cloud == 'AWS':
      download_function = (
          aws_virtual_machine.GenerateDownloadPreprovisionedDataCommand
      )
    elif self.cloud == 'Azure':
      download_function = (
          azure_virtual_machine.GenerateDownloadPreprovisionedDataCommand
      )
    else:
      raise NotImplementedError(
          'Cloud {0} does not support downloading preprovisioned '
          'data on Kubernetes VMs.'.format(self.cloud)
      )

    self.RemoteCommand(
        download_function(install_path, module_name, filename), timeout=timeout
    )

  def ShouldDownloadPreprovisionedData(self, module_name: str, filename: str):
    """Returns whether or not preprovisioned data is available."""
    if self.cloud == 'GCP' and FLAGS.gcp_preprovisioned_data_bucket:
      stat_function = gce_virtual_machine.GenerateStatPreprovisionedDataCommand
      gce_virtual_machine.GceVirtualMachine.InstallCli(self)
      # We assume that gsutil is installed to /usr/bin/gsutil on GCE VMs
      # ln -f is idempotent and can be called multiple times
      self.RemoteCommand(
          f'ln -sf {google_cloud_sdk.GSUTIL_PATH} /usr/bin/gsutil'
      )
    elif self.cloud == 'AWS' and FLAGS.aws_preprovisioned_data_bucket:
      stat_function = aws_virtual_machine.GenerateStatPreprovisionedDataCommand
      aws_virtual_machine.AwsVirtualMachine.InstallCli(self)
    elif self.cloud == 'Azure' and FLAGS.azure_preprovisioned_data_bucket:
      stat_function = (
          azure_virtual_machine.GenerateStatPreprovisionedDataCommand
      )
      azure_virtual_machine.AzureVirtualMachine.InstallCli(self)
    else:
      return False
    return self.TryRemoteCommand(stat_function(module_name, filename))

  # Retry installing sudo for ephemeral APT errors
  @vm_util.Retry(max_retries=linux_virtual_machine.UPDATE_RETRIES)
  def _InstallPrepareVmEnvironmentDependencies(self):
    """Installs sudo and other packages assumed by SetupVmEnvironment."""
    # The canonical ubuntu images as well as the nvidia/cuda
    # image do not have sudo installed so install it and configure
    # the sudoers file such that the root user's environment is
    # preserved when running as sudo.
    self.RemoteCommand(
        ' && '.join([
            # Clear existing lists to work around hash mismatches
            'rm -rf /var/lib/apt/lists/*',
            'apt-get update',
            'apt-get install -y sudo',
            "sed -i '/env_reset/d' /etc/sudoers",
            "sed -i '/secure_path/d' /etc/sudoers",
            'sudo ldconfig',
        ])
    )
    # iproute2 propvides ip
    self.InstallPackages('iproute2')


# All Ubuntu images below are from https://hub.docker.com/_/ubuntu/
# Note that they do not include all packages that are typically
# included with Ubuntu. For example, sudo is not installed.
# KubernetesVirtualMachine takes care of this by installing
# sudo in the container startup script.


class Ubuntu2204BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine, linux_virtual_machine.Ubuntu2204Mixin
):
  """Ubuntu 22 based Kubernetes VM."""

  DEFAULT_IMAGE = 'ubuntu:22.04'

  def _InstallPrepareVmEnvironmentDependencies(self):
    # fdisk is not installed. It needs to be installed after sudo, but before
    # RecordAdditionalMetadata.
    super()._InstallPrepareVmEnvironmentDependencies()
    # util-linux budled in the image no longer depends on fdisk.
    # Ubuntu 22 VMs get fdisk from ubuntu-server (which maybe we should
    # install here?).
    self.InstallPackages('fdisk')


class Ubuntu2004BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine, linux_virtual_machine.Ubuntu2004Mixin
):
  DEFAULT_IMAGE = 'ubuntu:20.04'


class Ubuntu1804BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine, linux_virtual_machine.Ubuntu1804Mixin
):
  DEFAULT_IMAGE = 'ubuntu:18.04'


class Ubuntu1604BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin
):
  DEFAULT_IMAGE = 'ubuntu:16.04'


class Ubuntu1604Cuda9BasedKubernetesVirtualMachine(
    DebianBasedKubernetesVirtualMachine,
    linux_virtual_machine.Ubuntu1604Cuda9Mixin,
):
  # Image is from https://hub.docker.com/r/nvidia/cuda/
  DEFAULT_IMAGE = 'nvidia/cuda:9.0-devel-ubuntu16.04'
