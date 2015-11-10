# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import os
import requests
import urlparse

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.mesos.mesos_disk import CephDisk
from perfkitbenchmarker.mesos.mesos_disk import LocalDisk

FLAGS = flags.FLAGS

flags.DEFINE_string('mesos_app_username', 'root',
                    'User name that Perfkit will attempt to use in order to '
                    'SSH into Docker instance.')

flags.DEFINE_boolean('mesos_docker_in_privileged_mode', True,
                     'If set to True, will attempt to create Docker containers '
                     'in a privileged mode. Note that some benchmarks execute '
                     'commands which are only allowed in privileged mode.')

flags.DEFINE_integer('docker_memory_mb', 2048,
                     'Memory limit for docker containers.')

flags.DEFINE_string('marathon_address', 'localhost:8080',
                    'Marathon IP address and port.')

MARATHON_API_PREFIX = '/v2/apps/'


class MesosVirtualMachine(virtual_machine.BaseVirtualMachine):

  CLOUD = 'Mesos'

  def __init__(self, vm_spec, network, firewall):
    super(MesosVirtualMachine, self).__init__(vm_spec, network, firewall)
    self.num_scratch_disks = 0
    self.name = self.name.replace('_', '-')
    self.user_name = FLAGS.mesos_app_username
    self.api_url = urlparse.urljoin(FLAGS.marathon_address,
                                    MARATHON_API_PREFIX)
    self.app_url = urlparse.urljoin(self.api_url, self.name)
    # TODO: verify if max_local_disks is needed
    # self.max_local_disks = 1000

  def _CreateDependencies(self):
    self._CheckPrerequisites()
    self._CreateVolumes()

  def _Create(self):
    self._CreateApp()
    self._WaitForBootCompletion()

  def _PostCreate(self):
    self._SetupSSH()
    self._ConfigureProxy()
    self._SetupDevicesPaths()

  def _Delete(self):
    self._DeleteApp()

  def _CheckPrerequisites(self):
    # TODO: implement the checks
    pass

  def _SetupDevicesPaths(self):
    for scratch_disk in self.scratch_disks:
      scratch_disk.SetDevicePath(self)

  def _CreateVolumes(self):
    for disk_num, disk_spec in enumerate(self.disk_specs):
      if disk_spec.disk_type == disk.LOCAL:
        scratch_disk = LocalDisk(disk_num, disk_spec, self.name)
      else:
        scratch_disk = CephDisk(disk_num, disk_spec, self.name)
      scratch_disk._Create()
      self.scratch_disks.append(scratch_disk)

  def _CreateApp(self):
    logging.info("Attempting to create App: %s" % self.name)
    body = self._BuildAppBody()
    output = requests.post(self.api_url, data=body)
    if output.status_code != requests.codes.CREATED:
      raise Exception("Unable to create App: %s" % output.text)
    logging.info("App %s created successfully." % self.name)

  def _BuildAppBody(self):
    cat_cmd = ['cat', vm_util.GetPublicKeyPath()]
    key_file, _ = vm_util.IssueRetryableCommand(cat_cmd)

    prepare_volumes_cmd = ''
    volumes_parameters = ''
    cleanup_volumes_cmd = ''
    for scratch_disk in self.scratch_disks:
      prepare_volumes_cmd += scratch_disk.GetPrepareCmd()
      volumes_parameters += scratch_disk.GetDockerVolumeParam()
      cleanup_volumes_cmd += scratch_disk.GetCleanupCmd()

    script_dir = os.path.dirname(__file__)
    script_name = 'run_docker.sh'
    with open(os.path.join(script_dir, script_name), 'r') as content_file:
      marathon_cmd = content_file.read().format(self.name, self.image,
                                                key_file, prepare_volumes_cmd,
                                                volumes_parameters,
                                                cleanup_volumes_cmd)

    body = {
        'id': self.name,
        # TODO: Make sure memory is limited for Docker
        # Maybe it is necessary to pass '-m' parameter to 'docker run'
        'mem': FLAGS.docker_memory_mb,
        'instances': 1,
        'constraints': [],
        'dependencies': [],
        # 1. Intentionally not relying on Marathon's support for Docker as it
        # would not allow us to dynamically create Ceph volumes before the
        # actual container creation. Instead, bash command is used which is a
        # composition of a) volume creation, b) Docker creation,
        # c) Docker & volume cleanup.
        # 2. Intentionally using SH script because internally a 'trap' bash
        # mechanism is used - it handles Docker and volume cleanup when
        # Marathon's application is removed.
        'cmd': 'echo \'%s\' > /tmp/create_docker.sh ; '
               'chmod +x /tmp/create_docker.sh; '
               '/tmp/create_docker.sh;' % marathon_cmd
    }

    return json.dumps(body)

  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=True)
  def _DeleteApp(self):
    logging.info('Attempting to delete App: %s' % self.name)
    output = requests.delete(self.app_url)
    if output.status_code != requests.codes.OK:
      raise Exception("Deleting App: %s failed. Reattempting."
                      % self.name)

  @vm_util.Retry(poll_interval=10, max_retries=300, log_errors=False)
  def _WaitForBootCompletion(self):
    logging.info("Waiting for App %s to get up and running. It may take a while"
                 "if a Docker image is being downloaded for the first time."
                 % self.name)
    output = requests.get(self.app_url)
    output = json.loads(output.text)
    tasks_running = output['app']['tasksRunning']
    if not tasks_running:
        raise Exception("Container is not booted yet. Retrying.")

  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=True)
  def _SetupSSH(self):
    output = requests.get(self.app_url)
    output = json.loads(output.text)
    tasks = output['app']['tasks']
    if not tasks or not tasks[0]['ports']:
      raise Exception("Unable to figure out where the container is running."
                      "Retrying to retrieve host and port.")
    self.ip_address = tasks[0]['host']
    self.ssh_port = tasks[0]['ports'][0]
    internal_ip, _ = self.RemoteCommand("ifconfig eth0 | grep 'inet addr' | awk"
                                        " -F: '{print $2}' | awk '{print $1}'")
    self.internal_ip = internal_ip.rstrip()

  # TODO: come out with a common version for Mesos and K8S
  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=True)
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


class DebianBasedMesosVirtualMachine(MesosVirtualMachine,
                                     linux_virtual_machine.DebianMixin):
  pass
