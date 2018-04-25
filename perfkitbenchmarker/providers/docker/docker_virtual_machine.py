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


#TODO
#REMOVE THESE
#STEPS
#1)_Create
#2)_Check if exists
#3)_PostCreate

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
from perfkitbenchmarker.providers.kubernetes import kubernetes_disk
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT

FLAGS = flags.FLAGS

UBUNTU_IMAGE = 'ubuntu:xenial'
SELECTOR_PREFIX = 'pkb'


class DockerVirtualMachine(virtual_machine.BaseVirtualMachine):
  """
  Object representing a Docker instance.
  """
  CLOUD = providers.DOCKER
  DEFAULT_IMAGE = None
  CONTAINER_COMMAND = None

  def __init__(self, vm_spec):
    """Initialize a Docker Container.

    Args:
      vm_spec: KubernetesPodSpec object of the vm.
    """
    super(DockerVirtualMachine, self).__init__(vm_spec)
    #self.num_scratch_disks = 0
    self.name = self.name.replace('_', '-')
    self.container_id = ''
    self.user_name = FLAGS.username
    self.image = self.image or self.DEFAULT_IMAGE
    #self.resource_limits = vm_spec.resource_limits
    #self.resource_requests = vm_spec.resource_requests

  # def GetResourceMetadata(self):
  #   metadata = super(DockerVirtualMachine, self).GetResourceMetadata()
  #   if self.resource_limits:
  #     metadata.update({
  #         'pod_cpu_limit': self.resource_limits.cpus,
  #         'pod_memory_limit_mb': self.resource_limits.memory,
  #     })
  #   if self.resource_requests:
  #     metadata.update({
  #         'pod_cpu_request': self.resource_requests.cpus,
  #         'pod_memory_request_mb': self.resource_requests.memory,
  #     })
  #   return metadata

  #def _CreateDependencies(self):
    #self._CheckPrerequisites()
    #self._CreateVolumes()

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

    logging.info('Creating Docker Container')
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')

    #logging.info(self.ssh_public_key)
    #logging.info(public_key)
    #docker_command = "docker run -d dphanekham/ssh_server"

    create_command = ['docker', 'run', '-d', '--name', self.name, 'ubuntu_ssh:latest', '/usr/sbin/sshd', '-D']

    container_info, _, _ = vm_util.IssueCommand(create_command)

    self.container_id = container_info.encode("ascii")
    logging.info(type(self.container_id))

    

    logging.info(container_info)

    # exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
    #               'pod', '-o=json', self.name]
    # pod_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    self._WaitForContainerBootCompletion()





  @vm_util.Retry()
  def _PostCreate(self):

    copy_ssh_command = ['docker', 'cp', self.ssh_public_key,
                         '%s:/root/.ssh/authorized_keys' % self.name]

    vm_util.IssueCommand(copy_ssh_command)

    chown_command = ['docker', 'exec', self.name, 'chown', 'root:root', '/root/.ssh/authorized_keys']

    vm_util.IssueCommand(chown_command)

    #self._ConfigureProxy()
    #self._SetupDevicesPaths()

  #TODO add checks to see if Delete fails
  def _Delete(self):
    """Delete Docker Instance"""

    delete_command = ['docker', 'kill', self.name]
    output = vm_util.IssueCommand(delete_command)
    logging.info(output[STDOUT].rstrip())

    remove_command = ['docker', 'rm', self.name]
    output = vm_util.IssueCommand(remove_command)
    logging.info(output[STDOUT].rstrip())

    return

  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=False)
  def _WaitForContainerBootCompletion(self):
    """
    Need to wait for the PODs to get up  - PODs are created with a little delay.
    """
    #exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
    #              'pod', '-o=json', self.name]
    logging.info("Waiting for Container %s" % self.name)
    #container_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    
    #TODO Get this to work with container ID
    exists_cmd = ['docker', 'inspect', self.name]
    info, _, _ = vm_util.IssueCommand(exists_cmd)
    
    #print(info)
    info = json.loads(info)
    if len(info) > 0:
      
      status = info[0]['State']['Running']
      self.internal_ip = info[0]['NetworkSettings']['IPAddress'].encode('ascii')
      self.ip_address = self.internal_ip
      print(status)
      if status == "true" or status == True:
        logging.info("Docker Container %s is up and running.", self.name)
        return
      raise Exception("Container %s is not running. Retrying to check status." %
                    self.container_id)

    else:
      logging.warning("Info not found")


  @vm_util.Retry(poll_interval=10, max_retries=20)
  def _Exists(self):
    """
    POD should have been already created but this is a double check.
    """
    logging.info("Checking if Docker Container Exists")
    exists_cmd = ['docker', 'inspect', self.name]
    info, _, _ = vm_util.IssueCommand(exists_cmd)

    info = json.loads(info)
    if len(info) > 0:
      status = info[0]['State']['Running']
      print(status)
      if status == "true" or status == True:
        logging.info("Docker Container %s is up and running.", self.name)
        return True
      else:
        return False
      raise Exception("Container %s is not running. Retrying to check status." %
                    self.container_id)

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
    """
    Deletes volumes.
    """
    for scratch_disk in self.scratch_disks[:]:
      scratch_disk.Delete()
      self.scratch_disks.remove(scratch_disk)

  def DeleteScratchDisks(self):
    pass

  def _GetInternalIp(self):
    
    """
    Gets the Internal ip address.
    """
    #pod_ip = kubernetes_helper.Get(
    #    'pods', self.name, '', '.status.podIP')


    pod_ip = False
    if not pod_ip:
      raise Exception("Internal POD IP address not found. Retrying.")

    self.internal_ip = pod_ip

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


  # def _BuildVolumesBody(self):
  #   """
  #   Constructs volumes-related part of POST request to create POD.
  #   """
  #   volumes = []

  #   for scratch_disk in self.scratch_disks:
  #     scratch_disk.AttachVolumeInfo(volumes)

  #   return volumes


class DebianBasedDockerVirtualMachine(DockerVirtualMachine,
                                          linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

class Ubuntu1404BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1404Mixin):
  DEFAULT_IMAGE = 'ubuntu:14.04'

class Ubuntu1604BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin):
  DEFAULT_IMAGE = 'ubuntu:16.04'

class Ubuntu1710BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1710Mixin):
  DEFAULT_IMAGE = 'ubuntu:17.10'
