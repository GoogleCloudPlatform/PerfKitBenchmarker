# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains code related to lifecycle management of Docker Containers."""

import json
import logging
import os
import threading

from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.docker import docker_container_spec
from perfkitbenchmarker.providers.docker import docker_disk

FLAGS = flags.FLAGS

UBUNTU_IMAGE = 'ubuntu:xenial'
DEFAULT_DOCKER_IMAGE = 'pkb/ubuntu16'
DOCKERFILE_DIRECTORY = 'perfkitbenchmarker/data/docker'


class DockerContainer(virtual_machine.BaseVirtualMachine):
  """Object representing a Docker Container instance."""

  CLOUD = providers.DOCKER
  DEFAULT_IMAGE = None
  CONTAINER_COMMAND = None
  docker_build_lock = threading.Lock()

  def __init__(self, vm_spec):
    """Initialize a Docker Container."""
    super(DockerContainer, self).__init__(vm_spec)
    self.name = self.name.replace('_', '-')
    self.container_id = ''
    self.user_name = FLAGS.username
    self.image = self.image or self.DEFAULT_IMAGE
    self.cpus = vm_spec.machine_type.cpus
    self.memory_mb = vm_spec.machine_type.memory
    self.privileged = vm_spec.privileged_docker
    self.container_image = DEFAULT_DOCKER_IMAGE
    self.docker_sysctl_flags = ''
    # apply flags
    if FLAGS.docker_custom_image:
      self.container_image = FLAGS.docker_custom_image
    if FLAGS.sysctl:
      self.docker_sysctl_flags = FLAGS.sysctl

  def _CreateDependencies(self):
    self._CreateVolumes()

  def _DeleteDependencies(self):
    self._DeleteVolumes()

  def _Create(self):
    """Create a Docker instance."""

    # Locally build docker container
    with self.docker_build_lock:
      image_exists = self._LocalImageExists(self.container_image)
      if image_exists is False:
        self._BuildImageLocally()

    create_command = self._FormatCreateCommand()
    container_info, _, _ = vm_util.IssueCommand(create_command,
                                                raise_on_failure=False)
    self.container_id = container_info.encode('ascii')

  def _FormatCreateCommand(self):
    """Formats the command for Docker based on vm_spec and flags."""

    create_command = ['docker', 'run', '-d', '--name', self.name]

    # format scratch disks
    for vol in self.scratch_disks:
      vol_string = vol.volume_name + ':' + vol.mount_point
      create_command.append('-v')
      create_command.append(vol_string)
    # format cpus option
    if self.cpus > 0:
      create_command.append('--cpus')
      create_command.append(str(self.cpus))
    # format memory option
    if self.memory_mb > 0:
      create_command.append('-m')
      create_command.append(str(self.memory_mb) + 'm')
    if self.docker_sysctl_flags:
      logging.info(self.docker_sysctl_flags)
      sysctl_string = ''
      for sysctl_flag in self.docker_sysctl_flags:
        sysctl_string = sysctl_string + sysctl_flag + ' '
      logging.info(sysctl_string)
      create_command.append('--sysctl')
      create_command.append(sysctl_string)

    create_command.append(self.container_image)
    create_command.append('/usr/sbin/sshd')
    create_command.append('-D')

    return create_command

  @vm_util.Retry()
  def _PostCreate(self):
    """Prepares running container.

    Gets the IP address, copies public keys,
    and configures the proxy if one is specified
    """
    self._GetIpAddresses()

    # Copy ssh key to container to enable ssh login
    copy_ssh_command = ['docker', 'cp', self.ssh_public_key,
                        '%s:/root/.ssh/authorized_keys' % self.name]
    vm_util.IssueCommand(copy_ssh_command, raise_on_failure=False)

    # change ownership of authorized_key file to root in container
    chown_command = ['docker', 'exec', self.name, 'chown',
                     'root:root', '/root/.ssh/authorized_keys']
    vm_util.IssueCommand(chown_command, raise_on_failure=False)
    self._ConfigureProxy()

  def _Delete(self):
    """Kill and Remove Docker Container."""

    delete_command = ['docker', 'kill', self.name]
    output = vm_util.IssueCommand(delete_command, raise_on_failure=False)
    logging.info(output[vm_util.OUTPUT_STDOUT].rstrip())

    remove_command = ['docker', 'rm', self.name]
    output = vm_util.IssueCommand(remove_command, raise_on_failure=False)
    logging.info(output[vm_util.OUTPUT_STDOUT].rstrip())

    return

  @vm_util.Retry(poll_interval=10, max_retries=10)
  def _Exists(self):
    """Returns whether the container is up and running."""

    info, return_code = self._GetContainerInfo()

    logging.info('Checking if Docker Container Exists')
    if info and return_code == 0:
      status = info[0]['State']['Running']
      if status:
        logging.info('Docker Container %s is up and running.', self.name)
        return True

    return False

  def _CreateVolumes(self):
    """Creates volumes for scratch disks.

    These volumes have to be created
    BEFORE containers creation because Docker doesn't allow to attach
    volume to currently running containers.
    """
    self.scratch_disks = docker_disk.CreateDisks(self.disk_specs, self.name)

  @vm_util.Retry(poll_interval=10, max_retries=20, log_errors=False)
  def _DeleteVolumes(self):
    """Deletes volumes."""
    for scratch_disk in self.scratch_disks[:]:
      scratch_disk.Delete()
      self.scratch_disks.remove(scratch_disk)

  def DeleteScratchDisks(self):
    pass

  def _GetIpAddresses(self):
    """Sets the internal and external IP address for the Container."""
    info, return_code = self._GetContainerInfo()
    ip = False

    if info and return_code == 0:
      ip = info[0]['NetworkSettings']['IPAddress'].encode('ascii')
      logging.info('IP: %s', ip)
      self.ip_address = ip
      self.internal_ip = ip
    else:
      logging.warning('IP address information not found')

  def _RemoveIfExists(self):
    if self._Exists():
      self._Delete()

  def _GetContainerInfo(self):
    """Returns information about a container.

    Gets Container information from Docker Inspect. Returns the information,
    if there is any and a return code. 0
    """
    logging.info('Checking Container Information')
    inspect_cmd = ['docker', 'inspect', self.name]
    info, _, return_code = vm_util.IssueCommand(inspect_cmd,
                                                suppress_warning=True,
                                                raise_on_failure=False)
    info = json.loads(info)
    return info, return_code

  def _ConfigureProxy(self):
    """Configure network proxy for Docker Container.

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

  def _BuildVolumesBody(self):
    """Construct volumes-related part of create command for Docker Container."""
    volumes = []

    for scratch_disk in self.scratch_disks:
      vol_string = scratch_disk.volume_name + ':' + scratch_disk.mount_point
      volumes.append('-v')
      volumes.append(vol_string)

    return volumes

  def _LocalImageExists(self, docker_image_name):
    """Returns whether a Docker image exists locally."""
    inspect_cmd = ['docker', 'image', 'inspect', docker_image_name]
    info, _, return_code = vm_util.IssueCommand(inspect_cmd,
                                                suppress_warning=True,
                                                raise_on_failure=False)
    info = json.loads(info)
    logging.info('Checking if Docker Image Exists')
    if info and return_code == 0:
      logging.info('Image exists')
      return True
    logging.info('Image does not exist')
    return False

  def _BuildImageLocally(self):
    """Build Container Image Locally.

    Dockerfiles located at
    PerfKitBenchmarker/data/docker/pkb/<containerImage>/Dockerfile
    """
    directory = os.path.dirname(
        data.ResourcePath(os.path.join(DOCKERFILE_DIRECTORY,
                                       self.container_image,
                                       'Dockerfile')))
    build_cmd = [
        'docker', 'build', '--no-cache',
        '-t', self.container_image, directory]

    vm_util.IssueCommand(build_cmd, raise_on_failure=False)

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping string property key to value.
    """
    result = super(DockerContainer, self).GetResourceMetadata()
    logging.warn('GET RESOURCE METADATA')

    return result


class DebianBasedDockerContainer(DockerContainer,
                                 linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

  def _GetNumCpus(self):
    return self.cpus

  def ApplySysctlPersistent(self, sysctl_params):
    """Override ApplySysctlPeristent function for Docker provider.

    Parent function causes errors with Docker because it shutdowns container
    Args:
      sysctl_params: dict - the keys and values to write
    """
    logging.warn('sysctl flags are applied when container is created. '
                 'Not all sysctl flags work with Docker. It does not '
                 'support flags that modify the host system')

  def _RebootIfNecessary(self):
    """Override RebootIfNecessary for Docker Provider."""
    logging.warn('Docker Containers cannot be rebooted to apply flags')


class Ubuntu1604BasedDockerContainer(
    DebianBasedDockerContainer, linux_virtual_machine.Ubuntu1604Mixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

# Note: to add support for ubuntu 14 and ubuntu 18, we simply need to
#       create/test Dockerfiles for those distros. This should be
#       fairly simple, but may require a few changes from the
#       ubuntu16 Dockerfile.
