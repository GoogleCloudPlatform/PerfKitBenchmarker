# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing GlusterFS installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'gluster_replicas', 3,
    'The number of Gluster replicas.')
flags.DEFINE_integer(
    'gluster_stripes', 1,
    'The number of Gluster stripes.')

GLUSTER_REPO_URL = ('http://download.gluster.org/pub/gluster/glusterfs/LATEST/'
                    'RHEL/glusterfs-epel.repo')


def YumInstall(vm):
  """Installs the gluster package on the VM."""
  vm.Install('wget')
  vm.InstallEpelRepo()
  vm.RemoteCommand('sudo wget -P /etc/yum.repos.d %s' % GLUSTER_REPO_URL)
  vm.InstallPackages('glusterfs-server')
  vm.RemoteCommand('sudo glusterd')


def AptInstall(vm):
  """Installs the gluster package on the VM."""
  vm.InstallPackages('glusterfs-server')


def MountGluster(vm, gluster_server, volume_name, mount_point):
  """Mounts a Gluster volume on the Virtual Machine.

  Args:
    vm: The VM to mount the Gluster volume on.
    gluster_server: A Gluster server that knows about the volume.
    volume_name: The name of the volume to mount.
    mount_point: The location to mount the volume on 'vm'.
  """
  vm.Install('gluster')
  volume = '{ip}:/{volume_name}'.format(
      ip=gluster_server.internal_ip, volume_name=volume_name)
  vm.RemoteCommand('sudo mkdir -p %s' % mount_point)
  vm.RemoteCommand('sudo mount -t glusterfs {volume} {mount_point}'.format(
      volume=volume, mount_point=mount_point))


def _CreateVolume(vm, bricks, volume_name):
  """Creates a GlusterFS volume.

  Args:
    vm: The Virtual Machine to create the volume from.
    bricks: A list of strings of the form "ip_address:/path/to/brick" which
      will be combined to form the Gluster volume.
    volume_name: The name of the volume which is being created.
  """
  replicas = ('replica %s' % FLAGS.gluster_replicas
              if FLAGS.gluster_replicas > 1 else '')
  stripes = ('stripe %s' % FLAGS.gluster_stripes
             if FLAGS.gluster_stripes > 1 else '')

  vm.RemoteCommand(('sudo gluster volume create {volume_name} '
                    '{stripes} {replicas} {bricks}').format(
                        volume_name=volume_name, replicas=replicas,
                        stripes=stripes, bricks=' '.join(bricks)))


def _ProbePeer(vm1, vm2):
  vm1.RemoteCommand('sudo gluster peer probe {internal_ip}'.format(
      internal_ip=vm2.internal_ip))


def ConfigureServers(gluster_servers, volume_name):
  """Configures the Gluster cluster and creates a volume.

  This function installs Gluster on all VMs passed into it, creates Gluster
  bricks, adds all VMs to the trusted storage pool, creates a volume, and
  starts it. After the volume is started, it can be mounted via MountGluster.

  Args:
    gluster_servers: The VMs that will be used to create the GlusterFS volume.
    volume_name: The name of the volume to be created.
  """
  vm_util.RunThreaded(lambda vm: vm.Install('gluster'), gluster_servers)

  bricks = []
  for vm in gluster_servers:
    for disk in vm.scratch_disks:
      brick_path = posixpath.join(disk.mount_point, 'gluster_brick')
      bricks.append('{internal_ip}:{brick_path}'.format(
          internal_ip=vm.internal_ip, brick_path=brick_path))
      vm.RemoteCommand('mkdir -p %s' % brick_path)

  # Gluster servers need to be added to the trusted storage pool
  # before they can be used to create a volume. Peers can only be
  # added to the pool by probing them from a member of the pool.
  if len(gluster_servers) > 1:
    _ProbePeer(gluster_servers[0], gluster_servers[1])
    _ProbePeer(gluster_servers[1], gluster_servers[0])

    for vm in gluster_servers[2:]:
      _ProbePeer(gluster_servers[0], vm)

  _CreateVolume(gluster_servers[0], bricks, volume_name)
  gluster_servers[0].RemoteCommand(
      'sudo gluster volume start {volume_name}'.format(
          volume_name=volume_name))


def DeleteVolume(gluster_server, volume_name):
  """Stops and deletes a Gluster volume."""
  gluster_server.RemoteCommand(
      'yes | sudo gluster volume stop %s' % volume_name)
  gluster_server.RemoteCommand(
      'yes | sudo gluster volume delete %s' % volume_name)
