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
"""Runs fio against a remote gluster cluster."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.linux_packages import fio
from perfkitbenchmarker.linux_packages import gluster

FLAGS = flags.FLAGS

flags.DEFINE_string('fill_disk_size', '4G',
                    'Amount to fill the disk before reading.')
flags.DEFINE_string('fill_disk_bs', '128k',
                    'Block size used to fill the disk before reading.')
flags.DEFINE_integer('fill_disk_iodepth', 64, 'iodepth used to fill the disk.')
flags.DEFINE_string('read_size', '4G', 'Size of the file to read.')
flags.DEFINE_string('read_bs', '512k', 'Block size of the file to read.')
flags.DEFINE_integer('read_iodepth', 1, 'iodepth used in reading the file.')


BENCHMARK_NAME = 'gluster_fio'
BENCHMARK_CONFIG = """
gluster_fio:
  description: >
    Runs fio against a remote gluster cluster.
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: null
    gluster_servers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      vm_count: 1
"""

_VOLUME_NAME = 'vol01'
_MOUNT_POINT = '/glusterfs'
_NUM_SECTORS_READ_AHEAD = 16384


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Set up GlusterFS and install fio.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  gluster_servers = benchmark_spec.vm_groups['gluster_servers']
  clients = benchmark_spec.vm_groups['clients']
  client_vm = clients[0]

  vm_util.RunThreaded(lambda vm: vm.Install('fio'), gluster_servers + clients)
  for vm in gluster_servers:
    vm.SetReadAhead(_NUM_SECTORS_READ_AHEAD,
                    [d.GetDevicePath() for d in vm.scratch_disks])

  # Set up Gluster
  if gluster_servers:
    gluster.ConfigureServers(gluster_servers, _VOLUME_NAME)

    args = [((client, gluster_servers[0], _VOLUME_NAME, _MOUNT_POINT), {})
            for client in clients]
    vm_util.RunThreaded(gluster.MountGluster, args)

    gluster_address = gluster_servers[0].internal_ip
    client_vm.RemoteCommand('sudo mkdir -p /testdir')
    client_vm.RemoteCommand('sudo mount %s:/vol01 /testdir -t glusterfs' %
                            gluster_address)


def _RunFio(vm, fio_params, metadata):
  """Run fio.

  Args:
    vm: Virtual machine to run fio on.
    fio_params: fio parameters used to create the fio command to run.
    metadata: Metadata to add to the results.

  Returns:
    A list of sample.Sample objects
  """
  stdout, _ = vm.RemoteCommand('sudo {0} {1}'.format(fio.GetFioExec(),
                                                     fio_params))
  job_file_contents = fio.FioParametersToJob(fio_params)
  samples = fio.ParseResults(
      job_file_contents,
      json.loads(stdout),
      base_metadata=metadata,
      skip_latency_individual_stats=True)
  return samples


def Run(benchmark_spec):
  """Run fio against gluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  gluster_servers = benchmark_spec.vm_groups['gluster_servers']
  clients = benchmark_spec.vm_groups['clients']
  client_vm = clients[0]
  results = []

  metadata = {
      'fill_disk_size': FLAGS.fill_disk_size,
      'fill_disk_bs': FLAGS.fill_disk_bs,
      'fill_disk_iodepth': FLAGS.fill_disk_iodepth,
      'read_size': FLAGS.read_size,
      'read_bs': FLAGS.read_bs,
      'read_iodepth': FLAGS.read_iodepth,
  }
  fio_params = ' '.join([
      '--output-format=json', '--name=fill_disk',
      '--filename=/testdir/testfile',
      '--filesize=%s' % FLAGS.fill_disk_size, '--ioengine=libaio', '--direct=1',
      '--verify=0', '--randrepeat=0',
      '--bs=%s' % FLAGS.fill_disk_bs,
      '--iodepth=%s' % FLAGS.fill_disk_iodepth, '--rw=randwrite'
  ])
  samples = _RunFio(client_vm, fio_params, metadata)
  results += samples

  # In addition to dropping caches, increase polling to potentially reduce
  # variance in network operations
  for vm in gluster_servers + clients:
    vm.RemoteCommand('sudo /sbin/sysctl net.core.busy_poll=50')
    vm.DropCaches()

  fio_read_common_params = [
      '--output-format=json', '--randrepeat=1', '--ioengine=libaio',
      '--gtod_reduce=1', '--filename=/testdir/testfile',
      '--bs=%s' % FLAGS.read_bs,
      '--iodepth=%s' % FLAGS.read_iodepth,
      '--size=%s' % FLAGS.read_size, '--readwrite=randread'
  ]
  fio_params = '--name=first_read ' + ' '.join(fio_read_common_params)
  samples = _RunFio(client_vm, fio_params, metadata)
  results += samples

  # Run the command again. This time, the file should be cached.
  fio_params = '--name=second_read ' + ' '.join(fio_read_common_params)
  samples = _RunFio(client_vm, fio_params, metadata)
  results += samples

  return results


def Cleanup(benchmark_spec):
  """Cleanup gluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  clients = benchmark_spec.vm_groups['clients']
  gluster_servers = benchmark_spec.vm_groups['gluster_servers']

  for client in clients:
    client.RemoteCommand('sudo umount %s' % _MOUNT_POINT)

  if gluster_servers:
    gluster.DeleteVolume(gluster_servers[0], _VOLUME_NAME)
