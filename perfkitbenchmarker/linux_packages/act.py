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

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/aerospike/act.git'
ACT_DIR = '%s/act' % INSTALL_DIR
flags.DEFINE_float('act_load', 1.0, 'Load multiplier for act test per device.')
flags.DEFINE_boolean('act_parallel', False,
                     'Run act tools in parallel. One copy per device.')
flags.DEFINE_integer('act_duration', 86400, 'Duration of act test in seconds.')
# TODO(user): Support user provided config file.
ACT_CONFIG_TEMPLATE = """
device-names: {devices}
num-queues: 8
threads-per-queue: 8
test-duration-sec: {duration}
report-interval-sec: 1
large-block-op-kbytes: 128
record-bytes: 1536
read-reqs-per-sec: {read_iops}
write-reqs-per-sec: {write_iops}
microsecond-histograms: no
scheduler-mode: noop
"""
_READ_1X_1D = 2000
_WRITE_1X_1D = 1000


def _Install(vm):
  """Installs the act on the VM."""
  vm.Install('build_tools')
  vm.Install('openssl')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, ACT_DIR))
  vm.RemoteCommand(
      'cd {0} && make && make -f Makesalt '.format(ACT_DIR))


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
    vm.RobustRemoteCommand('cd {0} && sudo ./actprep {1}'.format(
        ACT_DIR, device.GetDevicePath()))

  vm_util.RunThreaded(_RunActPrep, vm.scratch_disks)


def PrepActConfig(vm, index=None):
  """Prepare act config file at remote VM."""
  if index is None:
    disk_lst = vm.scratch_disks
    config_file = 'actconfig.txt'
  else:
    disk_lst = [vm.scratch_disks[index]]
    config_file = 'actconfig_{0}.txt'.format(index)
  devices = ','.join([d.GetDevicePath() for d in disk_lst])
  num_disk = len(disk_lst)
  # render template:
  content = ACT_CONFIG_TEMPLATE.format(
      devices=devices,
      duration=FLAGS.act_duration,
      read_iops=_CalculateReadIops(num_disk, FLAGS.act_load),
      write_iops=_CalculateWriteIops(num_disk, FLAGS.act_load))
  logging.info('ACT config: %s', content)
  with tempfile.NamedTemporaryFile(delete=False) as tf:
    tf.write(content)
    tf.close()
    vm.PushDataFile(tf.name, config_file)


def RunAct(vm, index=None):
  """Runs act binary with provided config."""
  if index is None:
    config = 'actconfig.txt'
    output = 'output'
    act_config_metadata = {'device_index': 'all'}
  else:
    config = 'actconfig_{0}.txt'.format(index)
    output = 'output_{0}'.format(index)
    act_config_metadata = {'device_index': index}
  # Push config file to remote VM.
  vm.RobustRemoteCommand(
      'cd {0} && sudo ./act ~/{1} > ~/{2}'.format(
          ACT_DIR, config, output))
  # Shows 1,2,4,8,..,64
  out, _ = vm.RemoteCommand(
      'cd {0} && ./latency_calc/act_latency.py -n 7 -e 1 -l ~/{1}'.format(
          ACT_DIR, output))
  samples = ParseRunAct(out)
  act_config_metadata.update(GetActMetadata(len(vm.scratch_disks)))
  for s in samples:
    s.metadata.update(act_config_metadata)
  return samples


def ParseRunAct(out):
  """Parse act output.

  Raw output format:
           trans                  device
         %>(ms)                 %>(ms)
   slice        1      8     64        1      8     64
   -----   ------ ------ ------   ------ ------ ------
       1     1.67   0.00   0.00     1.63   0.00   0.00
       2     1.38   0.00   0.00     1.32   0.00   0.00
       3     1.80   0.14   0.00     1.56   0.08   0.00
       4     1.43   0.00   0.00     1.39   0.00   0.00
       5     1.68   0.00   0.00     1.65   0.00   0.00
       6     1.37   0.00   0.00     1.33   0.00   0.00
       7     1.44   0.00   0.00     1.41   0.00   0.00
       8     1.41   0.00   0.00     1.35   0.00   0.00
       9     2.70   0.73   0.00     1.91   0.08   0.00
      10     1.54   0.00   0.00     1.51   0.00   0.00
      11     1.53   0.00   0.00     1.48   0.00   0.00
      12     1.47   0.00   0.00     1.43   0.00   0.00
   -----   ------ ------ ------   ------ ------ ------
     avg     1.62   0.07   0.00     1.50   0.01   0.00
     max     2.70   0.73   0.00     1.91   0.08   0.00

  Args:
    out: string. Output from act test.

  Returns:
    A list of sample.Sample objects.
  """
  lines = out.split('\n')
  buckets = []
  ret = []
  for line in lines:
    vals = line.split()
    if not vals or '-' in vals[0]:
      continue
    if vals[0] == 'slice':
      for v in vals[1:]:
        buckets.append(int(v))
      continue
    if not buckets:
      continue
    matrix = ''
    if vals[0] in ('avg', 'max'):
      matrix = '_' + vals[0]
    num_buckets = (len(vals) - 1) / 2
    for i in xrange(num_buckets):
      assert buckets[i] == buckets[i + num_buckets]
      ret.append(
          sample.Sample('trans' + matrix, float(vals[i + 1]), '%>(ms)',
                        {'slice': vals[0],
                         'bucket': buckets[i]}))
      ret.append(
          sample.Sample('device' + matrix,
                        float(vals[i + num_buckets + 1]), '%>(ms)',
                        {'slice': vals[0],
                         'bucket': buckets[i + num_buckets]}))
  return ret


def GetActMetadata(num_disk):
  # TODO(user): Expose more stats and flags.
  return {
      'act-parallel': FLAGS.act_parallel,
      'device-count': num_disk,
      'num-queues': 8,
      'threads-per-queues': 8,
      'test-duration-sec': FLAGS.act_duration,
      'report-interval-sec': 1,
      'large-block-op-kbytes': 128,
      'record-bytes': 1536,
      'read-reqs-per-sec': _CalculateReadIops(num_disk, FLAGS.act_load),
      'write-reqs-per-sec': _CalculateWriteIops(num_disk, FLAGS.act_load),
      'microsecond-histograms': 'no',
      'scheduler-mode': 'noop'}


def _CalculateReadIops(num_disk, load_multiplier):
  return int(_READ_1X_1D * num_disk * load_multiplier)


def _CalculateWriteIops(num_disk, load_multiplier):
  return int(_WRITE_1X_1D * num_disk * load_multiplier)
