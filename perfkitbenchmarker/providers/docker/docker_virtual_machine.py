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
    """Initialize a Kubernetes virtual machine.

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

  def GetResourceMetadata(self):
    metadata = super(DockerVirtualMachine, self).GetResourceMetadata()
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

  def _Delete(self):
    """Delete Docker Instance"""
    pass

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

    if len(info) > 0:
      info = json.loads(info)
      status = info[0]['State']['Running']
      self.internal_ip = info[0]['NetworkSettings']['IPAddress'].encode('ascii')
      self.ip_address = self.internal_ip
      print(status)
      if status == "true" or status == True:
        logging.info("DOCKER CONTAINER is up and running.")
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

    exists_cmd = ['docker', 'inspect', self.name]
    info, _, _ = vm_util.IssueCommand(exists_cmd)

    if len(info) > 0:
      info = json.loads(info)
      status = info[0]['State']['Running']
      print(status)
      if status == "true" or status == True:
        logging.info("DOCKER CONTAINER is up and running.")
        return True
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

  def _BuildContainerBody(self):
    """
    Constructs containers-related part of POST request to create POD.
    """
    registry = getattr(context.GetThreadBenchmarkSpec(), 'registry', None)
    if (not FLAGS.static_container_image and
        registry is not None):
      image = registry.GetFullRegistryTag(self.image)
    else:
      image = self.image
    container = {
        "image": image,
        "name": self.name,
        "securityContext": {
            "privileged": FLAGS.docker_in_privileged_mode
        },
        "volumeMounts": [
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

    result_with_empty_values_removed = (
        {k: v for k, v in resources.iteritems() if v})
    return result_with_empty_values_removed


class DebianBasedDockerVirtualMachine(DockerVirtualMachine,
                                          linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

  # def RemoteHostCommandWithReturnCode(self, command,
  #                                     should_log=False, retries=None,
  #                                     ignore_failure=False, login_shell=False,
  #                                     suppress_warning=False, timeout=None):
   

  #   """Runs a command in the Kubernetes container."""
  #   cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'exec', '-i',
  #          self.name, '--', '/bin/bash', '-c', command]
  #   stdout, stderr, retcode = vm_util.IssueCommand(
  #       cmd, force_info_log=should_log,
  #       suppress_warning=suppress_warning, timeout=timeout)
  #   if not ignore_failure and retcode:
  #     error_text = ('Got non-zero return code (%s) executing %s\n'
  #                   'Full command: %s\nSTDOUT: %sSTDERR: %s' %
  #                   (retcode, command, ' '.join(cmd),
  #                    stdout, stderr))
  #     raise errors.VirtualMachine.RemoteCommandError(error_text)
  #   return stdout, stderr, retcode

  def MoveHostFile(self, target, source_path, remote_path=''):
    """Copies a file from one VM to a target VM.

    Args:
      target: The target BaseVirtualMachine object.
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the TARGET machine, default
          is the home directory.
    """
    # file_name = vm_util.PrependTempDir(posixpath.basename(source_path))
    # self.RemoteHostCopy(file_name, source_path, copy_to=False)
    # target.RemoteHostCopy(file_name, remote_path)

  def RemoteHostCopy(self, file_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    # if copy_to:
    #   file_name = posixpath.basename(file_path)
    #   src_spec, dest_spec = file_path, '%s:%s' % (self.name, file_name)
    # else:
    #   remote_path, _ = self.RemoteCommand('readlink -f %s' % remote_path)
    #   remote_path = remote_path.strip()
    #   src_spec, dest_spec = '%s:%s' % (self.name, remote_path), file_path
    # cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
    #        'cp', src_spec, dest_spec]
    # stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    # if retcode:
    #   error_text = ('Got non-zero return code (%s) executing %s\n'
    #                 'STDOUT: %sSTDERR: %s' %
    #                 (retcode, ' '.join(cmd), stdout, stderr))
    #   raise errors.VirtualMachine.RemoteCommandError(error_text)
    # if copy_to:
    #   file_name = posixpath.basename(file_path)
    #   remote_path = remote_path or file_name
    #   self.RemoteCommand('mv %s %s; chmod 777 %s' %
    #                      (file_name, remote_path, remote_path))

  # @vm_util.Retry(log_errors=False, poll_interval=1)
  # def PrepareVMEnvironment(self):
  #   super(DebianBasedKubernetesVirtualMachine, self).PrepareVMEnvironment()
  #   # Don't rely on SSH being installed in Kubernetes containers,
  #   # so install it and restart the service so that it is ready to go.
  #   # Although ssh is not required to connect to the container, MPI
  #   # benchmarks require it.
  #   self.InstallPackages('ssh')
  #   self.RemoteCommand('sudo /etc/init.d/ssh restart', ignore_failure=True)
  #   self.RemoteCommand('mkdir ~/.ssh')
  #   with open(self.ssh_public_key) as f:
  #     key = f.read()
  #     self.RemoteCommand('echo "%s" >> ~/.ssh/authorized_keys' % key)


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


class Ubuntu1404BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1404Mixin):
  # All Ubuntu images below are from https://hub.docker.com/_/ubuntu/
  # Note that they do not include all packages that are typically
  # included with Ubuntu. For example, sudo is not installed.
  # KubernetesVirtualMachine takes care of this by installing
  # sudo in the container startup script.
  DEFAULT_IMAGE = 'ubuntu:14.04'
  CONTAINER_COMMAND = _install_sudo_command()


class Ubuntu1604BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin):
  DEFAULT_IMAGE = 'ubuntu:16.04'
  CONTAINER_COMMAND = _install_sudo_command()


class Ubuntu1710BasedDockerVirtualMachine(
    DebianBasedDockerVirtualMachine, linux_virtual_machine.Ubuntu1710Mixin):
  DEFAULT_IMAGE = 'ubuntu:17.10'
  CONTAINER_COMMAND = _install_sudo_command()
