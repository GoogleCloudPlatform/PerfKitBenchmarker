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
import os

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import fio_benchmark as linux_fio
from perfkitbenchmarker.linux_packages import fio as linux_fio_package
from perfkitbenchmarker.windows_packages import fio


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio'
BENCHMARK_CONFIG = linux_fio.BENCHMARK_CONFIG


def FillDevice(vm, disk, fill_size):
  """Fill the given disk on the given vm up to fill_size.

  Args:
    vm: a windows_virtual_machine.WindowsMixin object.
    disk: a disk.BaseDisk attached to the given vm.
    fill_size: amount of device to fill, in fio format.
  """

  command = (('%s --filename=%s --ioengine=windowsaio --thread=1 '
              '--name=fill-device --blocksize=512k --iodepth=64 '
              '--rw=write --direct=1 --size=%s') %
             (fio.FIO_PATH, disk.GetDeviceId(), fill_size))

  vm.RemoteCommand(command)


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.fio_target_mode != linux_fio.AGAINST_FILE_WITHOUT_FILL_MODE:
    disk_spec = config['vm_groups']['default']['disk_spec']
    for cloud in disk_spec:
      disk_spec[cloud]['mount_point'] = None
  return config


def RemoteJobFilePath(vm):
  return os.path.join(vm.temp_dir, 'fio.job')


def Prepare(benchmark_spec):

  linux_fio.WarnOnBadFlags()

  vm = benchmark_spec.vms[0]
  logging.info('Fio prepare on %s', vm)
  vm.Install('fio')

  disk = vm.scratch_disks[0]

  if linux_fio.FillTarget():
    logging.info('Fill device %s on %s', disk.GetDeviceId(), vm)
    FillDevice(vm, disk, FLAGS.fio_fill_size)

  if linux_fio.AgainstDevice():
    # fio gives a 'function not implemented' error when running
    # against a device on a Windows VM. But not when I run it manually
    # with apparently the same command. Need to investigate this.
    raise NotImplementedError()

  if FLAGS.fio_target_mode == linux_fio.AGAINST_FILE_WITH_FILL_MODE:
    raise NotImplementedError()


def Run(benchmark_spec):
  vm = benchmark_spec.vms[0]
  logging.info('Fio run on %s', vm)

  disk = vm.scratch_disks[0]
  mount_point = disk.mount_point

  job_file_string = linux_fio.GetOrGenerateJobFileString(
      FLAGS.fio_jobfile,
      FLAGS.fio_generate_scenarios,
      linux_fio.AgainstDevice(),
      disk,
      FLAGS.fio_io_depths,
      FLAGS.fio_working_set_size)
  job_file_path = vm_util.PrependTempDir(linux_fio.LOCAL_JOB_FILE_NAME)
  with open(job_file_path, 'w') as job_file:
    job_file.write(job_file_string)
    logging.info('Wrote fio job file at %s', job_file_path)

  vm.PushFile(job_file_path, RemoteJobFilePath())

  if linux_fio.AgainstDevice():
    fio_command = '%s --output-format=json --filename=%s %s' % (
        fio.FIO_PATH, disk.GetDeviceId(), RemoteJobFilePath())
  else:
    fio_command = '%s --output-format=json --directory=%s %s' % (
        fio.FIO_PATH, mount_point, RemoteJobFilePath())

  samples = []

  def RunIt(repeat_number=None, minutes_since_start=None, total_repeats=None):
    """Run the actual fio command on the VM and save the results.

    Args:
      repeat_number: if given, our number in a sequence of repetitions.
      minutes_since_start: if given, minutes since the start of repetition.
      total_repeats: if given, the total number of repetitions to do.
    """

    if repeat_number:
      logging.info('**** Repetition number %s of %s ****',
                   repeat_number, total_repeats)

    stdout, stderr = vm.RemoteCommand(fio_command, should_log=True)

    if repeat_number:
      base_metadata = {
          'repeat_number': repeat_number,
          'minutes_since_start': minutes_since_start
      }
    else:
      base_metadata = None

    samples.extend(linux_fio_package.ParseResults(job_file_string,
                                                  json.loads(stdout),
                                                  base_metadata=base_metadata))

  # TODO(user): This only gives results at the end of a job run
  #      so the program pauses here with no feedback to the user.
  #      This is a pretty lousy experience.
  logging.info('FIO Results:')

  if not FLAGS['fio_run_for_minutes'].present:
    RunIt()
  else:
    linux_fio.RunForMinutes(RunIt,
                            FLAGS.fio_run_for_minutes,
                            linux_fio.MINUTES_PER_JOB)

  return samples


def Cleanup(benchmark_spec):
  vm = benchmark_spec.vms[0]
  logging.info('Fio cleanup on %s', vm)
  vm.RemoveFile(RemoteJobFilePath())
  if not linux_fio.AgainstDevice() and not FLAGS.fio_jobfile:
    # If the user supplies their own job file, then they have to clean
    # up after themselves, because we don't know their temp file name.
    vm.RemoveFile(
        os.path.join(vm.GetScratchDir(), linux_fio.DEFAULT_TEMP_FILE_NAME))
