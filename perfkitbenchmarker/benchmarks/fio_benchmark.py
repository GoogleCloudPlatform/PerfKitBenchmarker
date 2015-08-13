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

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import fio


FLAGS = flags.FLAGS

flags.DEFINE_string('fio_benchmark_filename', 'fio_benchmark_file',
                    'scratch file that fio will use')
flags.DEFINE_string('fio_jobfile', 'fio.job', 'job file that fio will use')
flags.DEFINE_integer('memory_multiple', 10,
                     'size of fio scratch file compared to main memory size.')
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


BENCHMARK_INFO = {'name': 'fio',
                  'description': 'Runs fio in sequential, random, read '
                                 'and write modes.',
                  'scratch_disk': True,
                  'num_machines': 1}


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Prepare the virtual machine to run FIO.

     This includes installing fio, bc. and libaio1 and insuring that the
     attached disk is large enough to support the fio benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('FIO prepare on %s', vm)
  vm.Install('fio')
  file_path = data.ResourcePath(flags.FLAGS.fio_jobfile)

  disk_size_kb = vm.GetDeviceSizeFromPath(vm.GetScratchDir())
  amount_memory_kb = vm.total_memory_kb
  if disk_size_kb < amount_memory_kb * flags.FLAGS.memory_multiple:
    logging.error('%s must be larger than %dx memory"',
                  vm.GetScratchDir(),
                  flags.FLAGS.memory_multiple)
    # TODO(user): exiting here is probably not the correct behavor.
    #    When FIO is run across a data set which is too not considerably
    #    larger than the amount of memory then the benchmark results will be
    #    invalid. Once the benchmark results are returned from Run() an
    #    invalid (or is that rather a 'valid' flag should be added.
    exit(1)
  if FLAGS.against_device:
    device_path = vm.scratch_disks[0].GetDevicePath()
    logging.info('Umount scratch disk on %s at %s', vm, device_path)
    vm.RemoteCommand('sudo umount %s' % vm.GetScratchDir())
    logging.info('Fill scratch disk on %s at %s', vm, device_path)
    command = (
        ('sudo %s --filename=%s --ioengine=libaio '
         '--name=fill-device --blocksize=512k --iodepth=64 '
         '--rw=write --direct=1 --size=%s') %
        (fio.FIO_PATH, device_path, FLAGS.device_fill_size))
    vm.RemoteCommand(command)
    logging.info('Removing directory and filename in job file.')
    with open(data.ResourcePath(flags.FLAGS.fio_jobfile)) as original_f:
      job_file = original_f.read()
    job_file = fio.DeleteParameterFromJobFile(job_file, 'directory')
    job_file = fio.DeleteParameterFromJobFile(job_file, 'filename')
    tmp_dir = vm_util.GetTempDir()
    file_path = vm_util.PrependTempDir(FLAGS.fio_jobfile)
    logging.info('Write modified job file to temp directory %s', tmp_dir)
    with open(file_path, 'w') as modified_f:
      modified_f.write(job_file)
  vm.PushFile(file_path)


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
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('FIO running on %s', vm)
  if FLAGS.against_device:
    file_path = vm_util.PrependTempDir(FLAGS.fio_jobfile)
    fio_command = '--filename=%s %s' % (vm.scratch_disks[0].GetDevicePath(),
                                        FLAGS.fio_jobfile)
  else:
    file_path = data.ResourcePath(FLAGS.fio_jobfile)
    fio_command = '--directory=%s %s' % (
        vm.GetScratchDir(), FLAGS.fio_jobfile)
  fio_command = 'sudo %s --output-format=json %s' % (fio.FIO_PATH, fio_command)
  # TODO(user): This only gives results at the end of a job run
  #      so the program pauses here with no feedback to the user.
  #      This is a pretty lousy experience.
  logging.info('FIO Results:')
  stdout, stderr = vm.RemoteCommand(fio_command, should_log=True)
  with open(file_path) as f:
    job_file = f.read()
  return fio.ParseResults(job_file, json.loads(stdout))


def Cleanup(benchmark_spec):
  """Uninstall packages required for fio and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('FIO Cleanup up on %s', vm)
  vm.RemoveFile(flags.FLAGS.fio_jobfile)
  vm.RemoveFile(vm.GetScratchDir() + flags.FLAGS.fio_benchmark_filename)
