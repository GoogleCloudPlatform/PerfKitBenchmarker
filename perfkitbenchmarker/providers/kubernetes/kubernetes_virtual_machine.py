# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

import json
import logging
import posixpath

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

UBUNTU_IMAGE = 'ubuntu-upstart'
SELECTOR_PREFIX = 'pkb'


class KubernetesVirtualMachine(virtual_machine.BaseVirtualMachine):
  """
  Object representing a Kubernetes POD.
  """
  CLOUD = providers.KUBERNETES

  def __init__(self, vm_spec):
    """Initialize a Kubernetes virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(KubernetesVirtualMachine, self).__init__(vm_spec)
    self.num_scratch_disks = 0
    self.name = self.name.replace('_', '-')
    self.user_name = FLAGS.username
    self.image = self.image or UBUNTU_IMAGE

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
    """
    Exits if any of the prerequisites is not met.
    """
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
    """
    Creates a POD (Docker container with optional volumes).
    """
    create_rc_body = self._BuildPodBody()
    kubernetes_helper.CreateResource(create_rc_body)

  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=False)
  def _WaitForPodBootCompletion(self):
    """
    Need to wait for the PODs to get up  - PODs are created with a little delay.
    """
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'pod', '-o=json', self.name]
    logging.info("Waiting for POD %s" % self.name)
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    if pod_info:
      pod_info = json.loads(pod_info)
      containers = pod_info['spec']['containers']
      if len(containers) == 1:
        pod_status = pod_info['status']['phase']
        if (containers[0]['name'].startswith(self.name)
            and pod_status == "Running"):
          logging.info("POD is up and running.")
          return
    raise Exception("POD %s is not running. Retrying to check status." %
                    self.name)

  def _DeletePod(self):
    """
    Deletes a POD.
    """
    delete_pod = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                  'delete', 'pod', self.name]
    output = vm_util.IssueCommand(delete_pod)
    logging.info(output[STDOUT].rstrip())

  @vm_util.Retry(poll_interval=10, max_retries=20)
  def _Exists(self):
    """
    POD should have been already created but this is a double check.
    """
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'pod', '-o=json', self.name]
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
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
    Gets the POD's internal ip address.
    """
    pod_ip = kubernetes_helper.Get(
        'pods', self.name, '', '.status.podIP')

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

  def _SetupDevicesPaths(self):
    """
    Sets the path to each scratch disk device.
    """
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
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {
            "name": self.name,
            "labels": {
                SELECTOR_PREFIX: self.name
            }
        },
        "spec": {
            "volumes": volumes,
            "containers": [container],
            "dnsPolicy": "ClusterFirst",
        }
    }
    if FLAGS.kubernetes_anti_affinity:
      template["spec"]["affinity"] = {
          "podAntiAffinity": {
              "requiredDuringSchedulingIgnoredDuringExecution": [{
                  "labelSelector": {
                      "matchExpressions": [{
                          "key": "pkb_anti_affinity",
                          "operator": "In",
                          "values": [""],
                      }],
                  },
                  "topologyKey": "kubernetes.io/hostname",
              }],
          },
      }
      template["metadata"]["labels"]["pkb_anti_affinity"] = ""

    return json.dumps(template)

  def _BuildVolumesBody(self):
    """
    Constructs volumes-related part of POST request to create POD.
    """
    volumes = []

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeInfo(volumes)

    return volumes

  def _BuildContainerBody(self):
    """
    Constructs containers-related part of POST request to create POD.
    """
    container = {
        "image": self.image,
        "name": self.name,
        "securityContext": {
            "privileged": FLAGS.docker_in_privileged_mode
        },
        "volumeMounts": [
        ]
    }

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeMountInfo(container['volumeMounts'])

    return container


class DebianBasedKubernetesVirtualMachine(KubernetesVirtualMachine,
                                          linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE

  def RemoteHostCommandWithReturnCode(self, command,
                                      should_log=False, retries=None,
                                      ignore_failure=False, login_shell=False,
                                      suppress_warning=False, timeout=None):
    """Runs a command in the Kubernetes container."""
    cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'exec', '-i',
           self.name, '--', '/bin/bash', '-c', command]
    stdout, stderr, retcode = vm_util.IssueCommand(
        cmd, force_info_log=should_log,
        suppress_warning=suppress_warning, timeout=timeout)
    if not ignore_failure and retcode:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'Full command: %s\nSTDOUT: %sSTDERR: %s' %
                    (retcode, command, ' '.join(cmd), stdout, stderr))
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
      src_spec, dest_spec = file_path, '%s:' % (self.name,)
    else:
      remote_path, _ = self.RemoteCommand('readlink -f %s' % remote_path)
      remote_path = remote_path.strip()
      src_spec, dest_spec = '%s:%s' % (self.name, remote_path), file_path
    cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
           'cp', src_spec, dest_spec]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
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

  def PrepareVMEnvironment(self):
    super(DebianBasedKubernetesVirtualMachine, self).PrepareVMEnvironment()
    self.RemoteCommand('mkdir ~/.ssh')
    with open(self.ssh_public_key) as f:
      key = f.read()
      self.RemoteCommand('echo "%s" >> ~/.ssh/authorized_keys' % key)
