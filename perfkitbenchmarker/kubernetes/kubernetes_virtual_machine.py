# Copyright 2015 Google Inc. All rights reserved.
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
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT,\
    OUTPUT_STDERR as STDERR, OUTPUT_EXIT_CODE as EXIT_CODE

FLAGS = flags.FLAGS

flags.DEFINE_string('kubeconfig', '',
                    'Path to kubeconfig to be used by kubectl')

flags.DEFINE_string('kubectl', '/opt/bin/kubectl',
                    'Path to kubectl tool')

flags.DEFINE_string('username', 'root',
                    'User name that Perfkit will attempt to use in order to '
                    'SSH into Docker instance.')

flags.DEFINE_boolean('docker_in_privileged_mode', True,
                     'If set to True, will attempt to create Docker containers '
                     'in a privileged mode. Note that some benchmarks execute '
                     'commands which are only allowed in privileged mode.')

flags.DEFINE_list('kubernetes_nodes', [],
                  'IP addresses of Kubernetes Nodes. These need to be '
                  'accessible from the machine running Perfkit '
                  'benchmarker. Example: "10.20.30.40,10.20.30.41"')

flags.DEFINE_boolean('use_ceph_volumes', True,
                     'Use Ceph volumes for scratch disks')

flags.DEFINE_list('ceph_monitors', [],
                  'IP addresses and ports of Ceph Monitors. '
                  'Must be provided when scratch disk is required. '
                  'Example: "127.0.0.1:6789,192.168.1.1:6789"')

DEFAULT_ZONE = 'k8s'
SECRET_PREFIX = 'public-key-'
UBUNTU_IMAGE = 'ubuntu-upstart'
SELECTOR_PREFIX = 'pkb'


class KubernetesVirtualMachine(virtual_machine.BaseVirtualMachine):
  """
  Object representing a Kubernetes POD.
  """

  def __init__(self, vm_spec):
    super(KubernetesVirtualMachine, self).__init__(vm_spec)
    self.num_scratch_disks = 0
    self.name = self.name.replace('_', '-')
    self.user_name = FLAGS.username

  @classmethod
  def SetVmSpecDefaults(cls, vm_spec):
    """
    Updates VM spec with cloud specific defaults.
    """
    if vm_spec.image is None:
      vm_spec.image = UBUNTU_IMAGE
    if vm_spec.zone is None:
      # Although Kubernetes doesn't have zone mechanism,
      # Perfkit requires zone to be set.
      vm_spec.zone = DEFAULT_ZONE

  def _CreateDependencies(self):
    self._CheckPrerequisites()
    self._CreateSecret()
    self._CreateCephVolumes()

  def _DeleteDependencies(self):
    self._DeleteCephVolumes()
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

  def _Delete(self):
    self._DeletePod()
    self._DeleteService()

  def _CheckPrerequisites(self):
    if self.disk_specs and not FLAGS.use_ceph_volumes:
      raise NotImplementedError('Only Ceph volumes are supported right now. '
                                'Exiting.')
    if not FLAGS.kubernetes_nodes:
      raise Exception('Kubernetes Nodes IP addresses not found. Please specify'
                      'them using --kubernetes_nodes flag. Exiting.')

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
    create_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                  'create', '-f', '-']
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
                  'pod', '-o=json', '-l', '%s=%s' % (SELECTOR_PREFIX,
                                                     self.name)]
    pod_selector = (SELECTOR_PREFIX, self.name)
    logging.info("Waiting for POD %s=%s" % pod_selector)
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    pod_info = json.loads(pod_info)

    if len(pod_info['items']) == 1:
      containers = pod_info['items'][0]['spec']['containers']
      if len(containers) == 1:
        pod_status = pod_info['items'][0]['status']['phase']
        if (containers[0]['name'].startswith(self.name)
            and pod_status == "Running"):
          logging.info("POD is up and running.")
          return
    raise Exception("POD %s=%s is not running. Retrying to check status." %
                    pod_selector)

  @vm_util.Retry(poll_interval=10, max_retries=20, log_errors=False)
  def _WaitForEndpoint(self):
    """
    Waits till Service is matched with a POD. Only after this we will be able
    to connect to container via SSH.
    """
    endpoint_selector = (SELECTOR_PREFIX, self.name)
    logging.info("Waiting for endpoint %s=%s" % endpoint_selector)
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'endpoints', '-o=json', '-l', '%s=%s' % endpoint_selector]

    endpoint, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    endpoint = json.loads(endpoint)

    if len(endpoint['items']) == 1:
      if len(endpoint['items'][0]['subsets']) > 0:
        logging.info("Endpoint found. Service is successfully matched with "
                     "POD.")
        return
    raise Exception("Endpoint %s=%s not found. Retrying." % endpoint_selector)

  def _DeletePod(self):
    """
    Deletes a POD.
    """
    delete_rc = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                 'delete', 'pod', '-l', '%s=%s' % (SELECTOR_PREFIX, self.name)]
    output = vm_util.IssueCommand(delete_rc)
    logging.info(output[STDOUT].rstrip())

  def _DeleteService(self):
    """
    Deletes a Service.
    """
    delete_service = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                      'delete', 'service', '-l', '%s=%s' % (SELECTOR_PREFIX,
                                                            self.name)]
    output = vm_util.IssueCommand(delete_service)
    logging.info(output[STDOUT].rstrip())

  @vm_util.Retry(poll_interval=10, max_retries=20)
  def _Exists(self):
    """
    POD should have been already created but this is a double check.
    """
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'pod', '-o=json', '-l', '%s=%s' % (SELECTOR_PREFIX,
                                                     self.name)]
    pod_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    pod_info = json.loads(pod_info)

    if len(pod_info['items']) == 0:
      return False
    return True

  def _CreateCephVolumes(self):
    """
    Creates Rados Block Device volumes and installs filesystem on them.

    These volumes have to be created BEFORE containers creation
    because Kubernetes doesn't allow to attach volume to currently
    running containers.
    """
    for disk_num, disk_spec in enumerate(self.disk_specs):
      image_name = 'rbd_%s_%s' % (self.name, disk_num)
      disk_spec.image_name = image_name

      cmd = ['rbd', 'create', image_name, '--size',
             str(1024 * disk_spec.disk_size)]
      output = vm_util.IssueCommand(cmd)
      if output[EXIT_CODE] == 0:
        raise Exception("Creating RBD image failed: %s" % output[STDERR])

      cmd = ['rbd', 'map', image_name]
      output = vm_util.IssueCommand(cmd)
      if output[EXIT_CODE] == 0:
        raise Exception("Mapping RBD image failed: %s" % output[STDERR])
      rbd_device = output[STDOUT].rstrip()
      if '/dev/rbd' not in rbd_device:
        # Sometimes 'rbd map' command doesn't return any output.
        # Trying to find device location another way.
        cmd = ['rbd', 'showmapped']
        output = vm_util.IssueCommand(cmd)
        for image_device in output[STDOUT].split('\n'):
          if image_name in image_device:
            pattern = re.compile("/dev/rbd.*")
            output = pattern.findall(image_device)
            rbd_device = output[STDOUT].rstrip()
            break

      cmd = ['mkfs.ext4', rbd_device]
      output = vm_util.IssueCommand(cmd)
      if output[EXIT_CODE] == 0:
        raise Exception("Formatting partition failed: %s" % output[STDERR])

      cmd = ['rbd', 'unmap', rbd_device]
      output = vm_util.IssueCommand(cmd)
      if output[EXIT_CODE] == 0:
        raise Exception("Unmapping block device failed: %s" % output[STDERR])

      self.scratch_disks.append(disk_spec)

  @vm_util.Retry(poll_interval=10, max_retries=20, log_errors=False)
  def _DeleteCephVolumes(self):
    """
    Deletes RBD volumes.
    """
    for disk_spec in self.scratch_disks[:]:
      cmd = ['rbd', 'rm', disk_spec.image_name]
      output = vm_util.IssueCommand(cmd)
      if output[EXIT_CODE] != 0:
        msg = "Removing RBD image failed. Reattempting."
        logging.warning(msg)
        raise Exception(msg)
      self.scratch_disks.remove(disk_spec)

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

    get_port_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                    'get', 'service', '-l', '%s=%s' %
                    (SELECTOR_PREFIX, self.name), '-o', 'jsonpath',
                    '--template', '"{.items[0].spec.ports[0].nodePort}"']

    stdout, _, _ = vm_util.IssueCommand(get_port_cmd, suppress_warning=True)
    if not stdout:
      raise Exception("Port of service %s=%s not found. Exiting." %
                      (SELECTOR_PREFIX, self.name))
    port = stdout.replace('"', '')
    self.ssh_port = port
    self.ip_address = random.choice(FLAGS.kubernetes_nodes)

    get_internal_ip_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                           'get', 'pod', '-l', '%s=%s' %
                           (SELECTOR_PREFIX, self.name), '-o', 'jsonpath',
                           '--template', '"{.items[0].status.podIP}"']

    stdout, _, _ = vm_util.IssueCommand(get_internal_ip_cmd,
                                        suppress_warning=True)
    if not stdout:
      logging.warning("Internal POD IP address not found. External address"
                      "will be used instead.")
      self.internal_ip = self.ip_address
    else:
      self.internal_ip = stdout.replace('"', '')

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

    disc_specs_count = len(self.disk_specs)
    for disk_num in range(0, disc_specs_count):
      image_name = 'rbd_%s_%s' % (self.name, disk_num)
      ceph_volume = {
          "name": "rbdpd-%s-%s" % (self.instance_number, disk_num),
          "rbd": {
              "monitors": FLAGS.ceph_monitors,
              "pool": "rbd",
              "image": image_name,
              "secretRef": {
                  "name": "ceph-secret"
              },
              "fsType": "ext4",
              "readOnly": False
          }
      }
      volumes.append(ceph_volume)
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
    for disk_num, disk_spec in enumerate(self.disk_specs):
      mount_point = disk_spec.mount_point
      ceph_volume_mount = {
          "mountPath": mount_point,
          "name": "rbdpd-%s-%s" % (self.instance_number, disk_num)
      }
      container['volumeMounts'].append(ceph_volume_mount)

    return container

  def _BuildServiceBody(self):
    service = {
        "kind": "Service",
        "apiVersion": "v1",
        "metadata": {
            "name": self.name,
            "labels": {
                SELECTOR_PREFIX: self.name
            }
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
