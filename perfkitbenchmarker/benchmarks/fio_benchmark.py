# Copyright 2014 Google Inc. All rights reserved.
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

"""Runs fio benchmarks.

Man: http://manpages.ubuntu.com/manpages/natty/man1/fio.1.html
Quick howto: http://www.bluestop.org/fio/HOWTO.txt
"""

import json
import logging
import os

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import fio

LOCAL_JOB_FILE_NAME = 'fio.job'  # used with vm_util.PrependTempDir()
REMOTE_JOB_FILE_PATH = '~/fio.job'


FLAGS = flags.FLAGS

flags.DEFINE_string('fio_jobfile', None,
                    'Job file that fio will use. Giving a custom job file '
                    'overrides the other fio options.')
flags.DEFINE_boolean('against_device', False,
                     'Test direct against scratch disk. If set True, will '
                     'create a modified job file in temp directory, '
                     'which ignores directory and filename parameter.')
flags.DEFINE_string('device_fill_size', '100%',
                    'The amount of device to fill in prepare stage. '
                    'This flag is only valid when against_device=True. '
                    'The valid value can either be an integer, which '
                    'represents the number of bytes to fill or a '
                    'percentage, which represents the percentage '
                    'of the device.')
flags.DEFINE_string('io_depths', '1',
                    'IO queue depths to run on. Can specify a single number, '
                    'like --io_depths=1, or a range, like --io_depths=1-4')


BENCHMARK_INFO = {'name': 'fio',
                  'description': 'Runs fio in sequential, random, read '
                                 'and write modes.',
                  'scratch_disk': True,
                  'num_machines': 1}

GLOBALS_TEMPLATE = """
[global]
ioengine=libaio
invalidate=1
blocksize=4k
direct=1
runtime=10m
time_based
filename={filename}
do_verify=0
verify_fatal=0
randrepeat=0
size={size}

"""

SINGLE_JOB_TEMPLATE = """
[{rwkind}-io-depth-{iodepth}]
stonewall
rw={rwkind}
iodepth={iodepth}

"""


def GetIODepths(io_depths):
  """Parse the io_depths parameter.

  Args:
    io_depths: a string in the format of the --io_depths flag.

  Returns:
    An iterable of integers.

  Raises:
    ValueError if FLAGS.io_depths doesn't follow a format it recognizes.
  """

  try:
    return [int(io_depths)]
  except ValueError:
    bounds = io_depths.split('-', 1)

    if len(bounds) != 2:
      raise ValueError

    return range(int(bounds[0]), int(bounds[1]) + 1)


def WriteJobFile(mount_point):
  """Write a fio job file.

  Args:
    mount_point: the mount point of the disk we're testing against.

  Returns:
    The contents of a fio job file, as a string.
  """

  if FLAGS.against_device:
    filename = mount_point
    size = FLAGS.device_fill_size
  else:
    filename = os.path.join(mount_point, 'fio-temp-file')
    size = '100G'
  return (GLOBALS_TEMPLATE.format(filename=filename, size=size) +
          '\n'.join((SINGLE_JOB_TEMPLATE.format(rwkind='randread',
                                                iodepth=str(i))
                     for i in GetIODepths(FLAGS.io_depths))) +
          '\n'.join((SINGLE_JOB_TEMPLATE.format(rwkind='randwrite',
                                                iodepth=str(i))
                     for i in GetIODepths(FLAGS.io_depths))))


def JobFileString(vm):
  """Get the contents of our job file.

  Args:
    vm: the virtual_machine.BaseVirtualMachine that we will run on.

  Returns:
    A string containing the user's job file.
  """

  if FLAGS.fio_jobfile:
    return open(FLAGS.fio_jobfile, 'r').read()
  else:
    return WriteJobFile(vm.scratch_disks[0].mount_point)


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Prepare the virtual machine to run FIO.

     This includes installing fio, bc, and libaio1 and insuring that
     the attached disk is large enough to support the fio
     benchmark. We also make sure the job file is always located at
     the same path on the local machine.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """

  if FLAGS.fio_jobfile:
    ignored_flags = []
    if FLAGS.against_device:
      ignored_flags.append('--against_device')
    if FLAGS.device_fill_size != '100%':
      ignored_flags.append('--device_fill_size')
    if FLAGS.io_depths != '1':
      ignored_flags.append('--io_depths')

    if ignored_flags:
      logging.warning('Fio job file specified. Ignoring options "%s"',
                      ', '.join(ignored_flags))
  if FLAGS.device_fill_size and not FLAGS.against_device:
    logging.warning('--device_fill_size has no effect without --against_device')

  vm = benchmark_spec.vms[0]
  logging.info('FIO prepare on %s', vm)
  vm.Install('fio')

  if FLAGS.against_device and not FLAGS.fio_jobfile:
    mount_point = vm.scratch_disks[0].mount_point
    logging.info('Umount scratch disk on %s at %s', vm, mount_point)
    vm.RemoteCommand('sudo umount %s' % mount_point)

  job_file_path = vm_util.PrependTempDir(LOCAL_JOB_FILE_NAME)
  with open(job_file_path, 'w') as job_file:
    job_file.write(JobFileString(vm))
    logging.info('Wrote fio job file at %s', job_file_path)

  vm.PushFile(job_file_path, REMOTE_JOB_FILE_PATH)


def Run(benchmark_spec):
  """Spawn fio and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vm = benchmark_spec.vms[0]
  logging.info('FIO running on %s', vm)

  fio_command = 'sudo %s --output-format=json %s' % (fio.FIO_PATH,
                                                     REMOTE_JOB_FILE_PATH)
  # TODO(user): This only gives results at the end of a job run
  #      so the program pauses here with no feedback to the user.
  #      This is a pretty lousy experience.
  logging.info('FIO Results:')
  stdout, stderr = vm.RemoteCommand(fio_command, should_log=True)

  return fio.ParseResults(JobFileString(vm), json.loads(stdout))


def Cleanup(benchmark_spec):
  """Uninstall packages required for fio and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  logging.info('FIO Cleanup up on %s', vm)
  vm.RemoveFile(REMOTE_JOB_FILE_PATH)
  if not FLAGS.against_device and not FLAGS.fio_jobfile:
    # If the user supplies their own job file, then they have to clean
    # up after themselves, because we don't know their temp file name.
    vm.RemoveFile(vm.GetScratchDir() + 'fio-temp-file')
