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

import base64
import json
import logging
import random

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.kubernetes import kubernetes_disk
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT,\
    OUTPUT_STDERR as STDERR, OUTPUT_EXIT_CODE as EXIT_CODE

FLAGS = flags.FLAGS

DEFAULT_ZONE = 'k8s'
SECRET_PREFIX = 'public-key-'
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
    self._CreateSecret()
    self._CreateVolumes()

  def _DeleteDependencies(self):
    self._DeleteVolumes()
    self._DeleteSecret()

  def _Create(self):
    self._CreatePod()
    self._CreateService()
    self._WaitForPodBootCompletion()
    self._WaitForEndpoint()

  @vm_util.Retry()
  def _PostCreate(self):
    self._SetSshDetails()
    self._ConfigureProxy()
    self._SetupDevicesPaths()

  def _Delete(self):
    self._DeletePod()
    self._DeleteService()

  def _CheckPrerequisites(self):
    """
    Exits if any of the prerequisites is not met.
    """
    if not FLAGS.kubernetes_nodes:
      raise Exception('Kubernetes Nodes IP addresses not found. Please specify'
                      'them using --kubernetes_nodes flag. Exiting.')
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
    create_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                  'create', '-f', '-']
    create_rc_body = self._BuildPodBody()
    output = vm_util.IssueCommand(create_cmd, input=create_rc_body)
    if output[EXIT_CODE]:
      raise Exception("Creating POD failed: %s" % output[STDERR])
    logging.info(output[STDOUT].rstrip())

  def _CreateService(self):
    """
    Creates a Service (POD accessor).
    """
    # Intentionally setting validation flag to false as the kubectl binary
    # drops the request if nodePort parameter is not provided. However, it
    # is not needed - in such case nodePort will be automatically drawn
    # from the pool of ports.
    create_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                  'create', '--validate=false', '-f', '-']
    create_svc_body = self._BuildServiceBody()
    output = vm_util.IssueCommand(create_cmd, input=create_svc_body)
    if output[EXIT_CODE]:
      raise Exception("Creating Service failed: %s" % output[STDERR])
    logging.info(output[STDOUT].rstrip())

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

  @vm_util.Retry(poll_interval=10, max_retries=20, log_errors=False)
  def _WaitForEndpoint(self):
    """
    Waits till Service is matched with a POD. Only after this we will be able
    to connect to container via SSH.
    """
    logging.info("Waiting for endpoint %s" % self.name)
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'endpoints', '-o=json', self.name]

    endpoint, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    if endpoint:
      endpoint = json.loads(endpoint)
      if len(endpoint['subsets']) > 0:
        logging.info("Endpoint found. Service is successfully matched"
                     "with POD.")
        return

    raise Exception("Endpoint %s not found. Retrying." % self.name)

  def _DeletePod(self):
    """
    Deletes a POD.
    """
    delete_pod = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                  'delete', 'pod', self.name]
    output = vm_util.IssueCommand(delete_pod)
    logging.info(output[STDOUT].rstrip())

  def _DeleteService(self):
    """
    Deletes a Service.
    """
    delete_service = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                      'delete', 'service', self.name]
    output = vm_util.IssueCommand(delete_service)
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
      scratch_disk._Delete()
      self.scratch_disks.remove(scratch_disk)

  def _CreateSecret(self):
    """
    Creates a Kubernetes secret which will store public key.
    It will be used during during POD creation.
    """
    cat_cmd = ['cat', vm_util.GetPublicKeyPath()]
    key_file, _ = vm_util.IssueRetryableCommand(cat_cmd)
    encoded_public_key = base64.b64encode(key_file)
    template = {
        "kind": "Secret",
        "apiVersion": "v1",
        "metadata": {
            "name": SECRET_PREFIX + self.name
        },
        "data": {
            "authorizedkey": encoded_public_key
        }
    }
    create_secret_body = json.dumps(template)
    create_secret = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                     'create', '-f', '-']
    output = vm_util.IssueCommand(create_secret, input=create_secret_body)
    if output[EXIT_CODE]:
      raise Exception("Creating secret failed: %s" % output[STDERR])
    logging.info(output[STDOUT].rstrip())

  def _DeleteSecret(self):
    """
    Deletes a secret.
    """
    delete_secret = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                     'delete', 'secret', SECRET_PREFIX + self.name]
    result = vm_util.IssueCommand(delete_secret)
    logging.info(result[STDOUT].rstrip())

  def DeleteScratchDisks(self):
    pass

  def _SetSshDetails(self):
    """
    SSH to containers is done through one of Kubernetes Nodes:
    - SSH IP address is the address of one of the Nodes,
    - SSH port is assigned to Kubernetes Service matched with this container.
    """

    get_service_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                       'get', 'service', self.name, '-o', 'json']
    stdout, _, _ = vm_util.IssueCommand(get_service_cmd, suppress_warning=True)
    service = json.loads(stdout)
    ports = service.get('spec', {}).get('ports', [])

    if not ports:
      raise Exception("Port of service %s not found. Retrying." % self.name)
    self.ssh_port = ports[0]['nodePort']

    get_pod_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                   'get', 'pod', self.name, '-o', 'json']
    stdout, _, _ = vm_util.IssueCommand(get_pod_cmd, suppress_warning=True)
    pod = json.loads(stdout)
    pod_ip = pod.get('status', {}).get('podIP', None)

    if not pod_ip:
      raise Exception("Internal POD IP address not found. Retrying.")

    self.internal_ip = pod_ip
    self.ip_address = random.choice(FLAGS.kubernetes_nodes)

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
            "dnsPolicy": "ClusterFirst"
        }
    }
    return json.dumps(template)

  def _BuildVolumesBody(self):
    """
    Constructs volumes-related part of POST request to create POD.
    """
    volumes = []
    secret_volume = {
        "name": "ssh-details",
        "secret": {
            "secretName": SECRET_PREFIX + self.name
        }
    }
    volumes.append(secret_volume)

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeInfo(volumes)

    return volumes

  def _BuildContainerBody(self):
    """
    Constructs containers-related part of POST request to create POD.
    """

    # Kubernetes 'secret' mechanism is used to pass public key.
    # Unfortunately, Kubernetes doesn't allow to specify underline
    # character in file name. Thus 'authorizedkey' is passed which
    # is later properly moved to 'authorized_keys'.
    public_key = "/bin/mkdir /root/.ssh\n" + \
                 "mv /tmp/authorizedkey /root/.ssh/authorized_keys\n"
    container_boot_commands = public_key + "/usr/sbin/sshd -D"

    container = {
        "command": [
            "bash",
            "-c",
            container_boot_commands
        ],
        "image": self.image,
        "name": self.name,
        "securityContext": {
            "privileged": FLAGS.docker_in_privileged_mode
        },
        "volumeMounts": [
            {
                "name": "ssh-details",
                "mountPath": "/tmp"
            }
        ]
    }

    for scratch_disk in self.scratch_disks:
      scratch_disk.AttachVolumeMountInfo(container['volumeMounts'])

    return container

  def _BuildServiceBody(self):
    """
    Constructs body of a POST request to create a Service.
    """

    service = {
        "kind": "Service",
        "apiVersion": "v1",
        "metadata": {
            "name": self.name
        },
        "spec": {
            "ports": [
                {
                    "port": self.ssh_port
                }
            ],
            "selector": {
                SELECTOR_PREFIX: self.name
            },
            "type": "NodePort"
        }
    }
    return json.dumps(service)


class DebianBasedKubernetesVirtualMachine(KubernetesVirtualMachine,
                                          linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE
