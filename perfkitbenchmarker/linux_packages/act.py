# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing aerospike server installation and cleanup functions."""


import logging
import tempfile

from absl import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from six.moves import range

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/aerospike/act.git'
ACT_DIR = '%s/act' % linux_packages.INSTALL_DIR
flags.DEFINE_list('act_load', ['1.0'],
                  'Load multiplier for act test per device.')
flags.DEFINE_boolean('act_parallel', False,
                     'Run act tools in parallel. One copy per device.')
flags.DEFINE_integer('act_duration', 86400, 'Duration of act test in seconds.')
flags.DEFINE_integer('act_reserved_partitions', 0,
                     'Number of partitions reserved (not being used by act).')
flags.DEFINE_integer(
    'act_service_threads', None,
    'Total number of service threads on which requests are '
    'generated and done. Default is 5x the number of CPUs.')
# TODO(user): Support user provided config file.
ACT_CONFIG_TEMPLATE = """
device-names: {devices}
test-duration-sec: {duration}
read-reqs-per-sec: {read_iops}
write-reqs-per-sec: {write_iops}
"""
_READ_1X_1D = 2000
_WRITE_1X_1D = 1000
ACT_COMMIT = 'ac10c0cc2880ef8542af03296b15f85182943ad6'  # ACT 6.2


def _Install(vm):
  """Installs the act on the VM."""
  vm.Install('build_tools')
  vm.Install('openssl')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, ACT_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1} && make'.format(
      ACT_DIR, ACT_COMMIT))


def YumInstall(vm):
  """Installs act package on the VM."""
  vm.InstallPackages('zlib-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs act package on the VM."""
  vm.InstallPackages('zlib1g-dev')
  _Install(vm)


def Uninstall(vm):
  vm.RemoteCommand('rm -rf %s' % ACT_DIR)


def RunActPrep(vm):
  """Runs actprep binary to initialize the drive."""

  def _RunActPrep(device):
    vm.RobustRemoteCommand('cd {0} && sudo ./target/bin/act_prep {1}'.format(
        ACT_DIR, device.GetDevicePath()))

  assert len(vm.scratch_disks) > FLAGS.act_reserved_partitions, (
      'More reserved partition than total partitions available.')
  # Only salt partitions will be used.
  vm_util.RunThreaded(
      _RunActPrep, vm.scratch_disks[FLAGS.act_reserved_partitions:])


def PrepActConfig(vm, load, index=None):
  """Prepare act config file at remote VM."""
  if index is None:
    disk_lst = vm.scratch_disks
    # Treat first few partitions as reserved.
    disk_lst = disk_lst[FLAGS.act_reserved_partitions:]
    config_file = 'actconfig_{0}.txt'.format(load)
  else:
    disk_lst = [vm.scratch_disks[index]]
    config_file = 'actconfig_{0}_{1}.txt'.format(index, load)
  devices = ','.join([d.GetDevicePath() for d in disk_lst])
  num_disk = len(disk_lst)
  # render template:
  content = ACT_CONFIG_TEMPLATE.format(
      devices=devices,
      duration=FLAGS.act_duration,
      read_iops=_CalculateReadIops(num_disk, load),
      write_iops=_CalculateWriteIops(num_disk, load))
  if FLAGS.act_service_threads:
    content += 'service-threads: %d\n' % FLAGS.act_service_threads
  logging.info('ACT config: %s', content)
  with tempfile.NamedTemporaryFile(delete=False, mode='w+') as tf:
    tf.write(content)
    tf.close()
    vm.PushDataFile(tf.name, config_file)


def RunAct(vm, load, index=None):
  """Runs act binary with provided config."""
  if index is None:
    config = 'actconfig_{0}.txt'.format(load)
    output = 'output_{0}'.format(load)
    act_config_metadata = {'device_index': 'all'}
  else:
    config = 'actconfig_{0}_{1}.txt'.format(index, load)
    output = 'output_{0}_{1}'.format(index, load)
    act_config_metadata = {'device_index': index}
  # Push config file to remote VM.
  vm.RobustRemoteCommand(
      'cd {0} && sudo ./target/bin/act_storage ~/{1} > ~/{2}'.format(
          ACT_DIR, config, output))
  # Shows 1,2,4,8,..,64.
  out, _ = vm.RemoteCommand(
      'cd {0} ; ./analysis/act_latency.py -n 7 -e 1 -x -l ~/{1}; exit 0'.format(
          ACT_DIR, output), ignore_failure=True)
  samples = ParseRunAct(out)
  last_output_block, _ = vm.RemoteCommand('tail -n 100 ~/{0}'.format(output))

  # Early termination.
  if 'drive(s) can\'t keep up - test stopped' in last_output_block:
    act_config_metadata['ERROR'] = 'cannot keep up'
  act_config_metadata.update(
      GetActMetadata(
          len(vm.scratch_disks) - FLAGS.act_reserved_partitions, load))
  for s in samples:
    s.metadata.update(act_config_metadata)
  return samples


def ParseRunAct(out):
  """Parse act output.

  Raw output format:
          reads
          %>(ms)
  slice        1      2      4       rate
  -----   ------ ------ ------ ----------
      1     0.00   0.00   0.00     6000.0
      2     0.00   0.00   0.00     6000.0
      3     0.01   0.00   0.00     6000.0
  -----   ------ ------ ------ ----------
    avg     0.00   0.00   0.00     6000.0
    max     0.01   0.00   0.00     6000.0

  Args:
    out: string. Output from act test.

  Returns:
    A list of sample.Sample objects.
  """
  ret = []
  if 'could not find 3600 seconds of data' in out:
    ret.append(sample.Sample('Failed:NotEnoughSample', 0, '',
                             {}))
    return ret
  lines = out.split('\n')
  buckets = []
  for line in lines:
    vals = line.split()
    if not vals or '-' in vals[0]:
      continue
    if vals[0] == 'slice':
      for v in vals[1:]:
        buckets.append(v)
      continue
    if not buckets:
      continue
    matrix = ''
    if vals[0] in ('avg', 'max'):
      matrix = '_' + vals[0]
    num_buckets = len(vals) - 1
    for i in range(num_buckets - 1):
      ret.append(
          sample.Sample('reads' + matrix, float(vals[i + 1]), '%>(ms)',
                        {'slice': vals[0],
                         'bucket': int(buckets[i])}))
    ret.append(
        sample.Sample('read_rate' + matrix,
                      float(vals[num_buckets]), 'iops',
                      {'slice': vals[0]}))
  return ret


def GetActMetadata(num_disk, load):
  """Returns metadata for act test."""
  # TODO(user): Expose more stats and flags.
  metadata = {
      'act-version': '6.2',
      'act-parallel': FLAGS.act_parallel,
      'reserved_partition': FLAGS.act_reserved_partitions,
      'device-count': num_disk,
      'test-duration-sec': FLAGS.act_duration,
      'report-interval-sec': 1,
      'large-block-op-kbytes': 128,
      'record-bytes': 1536,
      'read-reqs-per-sec': _CalculateReadIops(num_disk, load),
      'write-reqs-per-sec': _CalculateWriteIops(num_disk, load),
      'microsecond-histograms': 'no',
      'scheduler-mode': 'noop'
  }
  metadata['service-threads'] = FLAGS.act_service_threads or 'default'
  # num-queues & threads-per-queues are deprecated in act v6+,
  # leaving as "default" for backward compatibility.
  metadata['num-queues'] = 'default'
  metadata['threads-per-queues'] = 'default'
  return metadata


def _CalculateReadIops(num_disk, load_multiplier):
  return int(_READ_1X_1D * num_disk * load_multiplier)


def _CalculateWriteIops(num_disk, load_multiplier):
  return int(_WRITE_1X_1D * num_disk * load_multiplier)


def IsRunComplete(samples):
  """Decides if the run is able to complete (regardless of latency)."""
  for s in samples:
    if s.metric == 'Failed:NotEnoughSample':
      return False
    if 'ERROR' in s.metadata:
      return False
  return True
