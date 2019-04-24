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

"""Contains code related to lifecycle management of Docker Containers."""


#TODO
#add Docker build functionality
#1) local build using a Dockerfile
#2) build using GCloud Repo
#3) build from custom dockerfile (To support different flavors)
#4) make sure container builds only once
#5) Add in resource management (specify cpu/mem etc)

import json
import logging
import posixpath
import os

from perfkitbenchmarker import data
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import container_service
from perfkitbenchmarker.providers.docker import docker_disk
from perfkitbenchmarker.providers.docker import docker_resource_spec
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT

from perfkitbenchmarker.providers.gcp import google_container_engine

FLAGS = flags.FLAGS

UBUNTU_IMAGE = 'ubuntu:xenial'
DEFAULT_DOCKER_IMAGE = 'pkb/ubuntu16_ssh'

class DockerVirtualMachine(virtual_machine.BaseVirtualMachine):
  """
  Object representing a Docker Container instance
  """
  CLOUD = providers.DOCKER
  DEFAULT_IMAGE = None
  CONTAINER_COMMAND = None

  def __init__(self, vm_spec):
    """Initialize a Docker Container.

    Args:
      vm_spec
    """
    super(DockerVirtualMachine, self).__init__(vm_spec)
    self.name = self.name.replace('_', '-')
    self.container_id = ''
    self.user_name = FLAGS.username
    self.image = self.image or self.DEFAULT_IMAGE
    self.cpus = vm_spec.docker_cpus
    self.memory_mb = vm_spec.docker_memory_mb
    self.privileged = vm_spec.privileged_docker
    self.container_image = DEFAULT_DOCKER_IMAGE

    #apply flags
    if FLAGS.custom_docker_image:
      self.container_image = FLAGS.custom_docker_image



  def _CreateDependencies(self):
    #self._CheckPrerequisites()
    self._CreateVolumes()

  def _DeleteDependencies(self):
    self._DeleteVolumes()

  def _Create(self):
    """Create a Docker instance"""

    #commands
    #Docker build dockerfile (or docker pull)
    #Transfer SSH keys to docker container
    # docker run -d --name test ubuntu_ssh:latest /usr/sbin/sshd -D
    # docker cp /home/derek/.ssh/id_rsa.pub test:/root/.ssh/authorized_keys
    # docker exec test chown root:root /root/.ssh/authorized_keys
    #Start container and start ssh server

    #NOTE for building Dockerfile, might want to use container_service
    # logging.info("google container")
    # registry_spec = 
    # gcr = google_container_engine.GoogleContainerRegistry()

    logging.info('Creating Docker Container')
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')

    #logging.info(self.ssh_public_key)
    #logging.info(public_key)
    #docker_command = "docker run -d dphanekham/ssh_server"

    ##Try to build container here
    ##create container object
    ##build local
    #containerImage = container_service._ContainerImage("ubuntu_simple")

    #TODO, replace this with removeIfExists type of command
    #self._Delete()

    #set container image
    

    image_exists = self._LocalImageExists(self.container_image)

    if image_exists == False:
      self._BuildImageLocally()

    #TODO check if container built correctly

    create_command = self._FormatCreateCommand()

    container_info, _, _ = vm_util.IssueCommand(create_command)

    self.container_id = container_info.encode("ascii")
    logging.info("Container with Disk ID: %s", self.container_id)

  def _FormatCreateCommand(self):
    """Formats the command for Docker based on vm_spec and flags"""

    create_command = ['docker', 'run', '-d', '--name', self.name]

    #format scratch disks
    for vol in self.scratch_disks:
      vol_string = vol.volume_name + ":" + vol.mount_point
      create_command.append('-v')
      create_command.append(vol_string)

    #format cpus option
    if self.cpus > 0:
      create_command.append('--cpus')
      create_command.append(self.cpus)

    #format memory option
    if self.memory_mb > 0:
      create_command.append('-m')
      create_command.append(str(self.memory_mb) + 'm')

    create_command.append(self.container_image)
    create_command.append('/usr/sbin/sshd')
    create_command.append('-D')

    logging.info("CREATE COMMAND:")
    logging.info(create_command)

    return create_command


  @vm_util.Retry()
  def _PostCreate(self):
    """
    Prepares running container. Gets the IP address, copies public keys,
    and configures the proxy if one is specified
    """

    self._GetIpAddresses()

    #Copy ssh key to container to enable ssh login
    copy_ssh_command = ['docker', 'cp', self.ssh_public_key,
                         '%s:/root/.ssh/authorized_keys' % self.name]
    vm_util.IssueCommand(copy_ssh_command)

    #change ownership of authorized_key file to root in container
    chown_command = ['docker', 'exec', self.name, 'chown',
                     'root:root', '/root/.ssh/authorized_keys']
    vm_util.IssueCommand(chown_command)

    self._ConfigureProxy()
    #self._SetupDevicesPaths()

  #TODO add checks to see if Delete fails
  def _Delete(self):
    """Kill and Remove Docker Container"""

    delete_command = ['docker', 'kill', self.name]
    output = vm_util.IssueCommand(delete_command)
    logging.info(output[STDOUT].rstrip())

    remove_command = ['docker', 'rm', self.name]
    output = vm_util.IssueCommand(remove_command)
    logging.info(output[STDOUT].rstrip())

    return


  @vm_util.Retry(poll_interval=10, max_retries=10)
  def _Exists(self):
    """
    Checks if Container has successful been created an is running
    """
    
    info, returnCode = self._GetContainerInfo()

    logging.info("Checking if Docker Container Exists")
    if len(info) > 0 and returnCode == 0:
      status = info[0]['State']['Running']

      if status == "True" or status == True:
        logging.info("Docker Container %s is up and running.", self.name)
        return True

    return False

  def _CreateVolumes(self):
    """
    Creates volumes for scratch disks. These volumes have to be created
    BEFORE containers creation because Docker doesn't allow to attach
    volume to currently running containers.
    """
    self.scratch_disks = docker_disk.CreateDisks(self.disk_specs, self.name)

  @vm_util.Retry(poll_interval=10, max_retries=20, log_errors=False)
  def _DeleteVolumes(self):
    """
    Deletes volumes.
    """
    for scratch_disk in self.scratch_disks[:]:
      scratch_disk.Delete()
      self.scratch_disks.remove(scratch_disk)
    pass


  def DeleteScratchDisks(self):
    pass

  def _GetIpAddresses(self):
    """
    Sets the internal and external IP address for the Container.
    """
    info, returnCode = self._GetContainerInfo()
    ip = False

    if len(info) > 0 and returnCode == 0:

      ip = info[0]['NetworkSettings']['IPAddress'].encode('ascii')
      logging.info("IP: " + str(ip))
      self.ip_address = ip
      self.internal_ip = ip

    else:
      logging.warning("IP address information not found")


  def _RemoveIfExists(self):
    if self._Exists():
      self._Delete()


  def _GetContainerInfo(self):
    """
    Gets Container information from Docker Inspect. Returns the information, 
    if there is any and a return code. 0  
    """
    logging.info("Finding Container Information")
    inspect_cmd = ['docker', 'inspect', self.name]
    info, _, returnCode = vm_util.IssueCommand(inspect_cmd, suppress_warning=True)

    info = json.loads(info)

    return info, returnCode


  def _ConfigureProxy(self):
    """
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

  # def _SetupDevicesPaths(self):
  #   """
  #   Sets the path to each scratch disk device.
  #   """
  #   for scratch_disk in self.scratch_disks:
  #     scratch_disk.SetDevicePath(self)


  def _BuildVolumesBody(self):
    """
    Constructs volumes-related part of create command for Docker Container
    """
    volumes = []

    for scratch_disk in self.scratch_disks:
      vol_string = scratch_disk.volume_name + ":" + scratch_disk.mount_point

      volumes.append('-v')
      volumes.append(vol_string)

    return volumes


  def _LocalImageExists(self, docker_image_name):
    """
    Checks if an image exists locally
    Returns boolean
    """
    logging.info("Finding Image Information")
    inspect_cmd = ['docker', 'image', 'inspect', docker_image_name]
    info, _, returnCode = vm_util.IssueCommand(inspect_cmd, suppress_warning=True)

    info = json.loads(info)

    logging.info("Checking if Docker Image Exists")
    if len(info) > 0 and returnCode == 0:
      logging.info("Image exists")
      return True

    logging.info("Image does not exist")
    return False


  def _BuildImageLocally(self):

    ##Try to build container here
    ##create container object
    ##build local
    #containerImage = container_service._ContainerImage("ubuntu_simple")
    
    directory = os.path.dirname(
      data.ResourcePath(os.path.join('docker', self.container_image, 'Dockerfile')))

    

    build_cmd = [
        'docker', 'build', '--no-cache',
        '-t', self.container_image, directory
    ]

    vm_util.IssueCommand(build_cmd)


class DebianBasedDockerVirtualMachine(DockerVirtualMachine,
                                          linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

# class Ubuntu1404BasedDockerVirtualMachine(
#     DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1404Mixin):
#   DEFAULT_IMAGE = 'ubuntu:14.04'

class Ubuntu1604BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

# class Ubuntu1710BasedDockerVirtualMachine(
#     DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1710Mixin):
#   DEFAULT_IMAGE = 'ubuntu:17.10'


