# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Utils for fio benchmarks."""

import json
import logging
import posixpath
import time

from absl import flags
import jinja2
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import fio_scenario_parser
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import result_parser


FLAGS = flags.FLAGS


def AgainstDevice():
  """Check whether we're running against a device or a file.

  Returns:
    True if running against a device, False if running against a file.
  """
  return fio_flags.FIO_TARGET_MODE.value in constants.AGAINST_DEVICE_MODES


def GetFilename(disks, device_path=None):
  if AgainstDevice():
    filename = device_path or ':'.join(
        [disk.GetDevicePath() for disk in disks]
    )
  else:
    # Since we pass --directory to fio, we must use relative file
    # paths or get an error.
    filename = constants.DEFAULT_TEMP_FILE_NAME
  return filename


def GetAllDiskPaths(disks):
  raw_files = []
  for disk in disks:
    if disk.is_striped:
      raw_files += [d.GetDevicePath() for d in disk.disks]
    else:
      raw_files += [disk.GetDevicePath()]
  return raw_files


def SeparateJobsForDisks(disk_paths):
  disks_list = [
      {
          'index': index,
          'disk_filename': disk_path,
      }
      for index, disk_path in enumerate(disk_paths)
  ]
  if fio_flags.FIO_TEST_DISK_COUNT.value:
    return disks_list[:fio_flags.FIO_TEST_DISK_COUNT.value]
  return disks_list


def GetFileAsString(file_path):
  if not file_path:
    return None
  with open(data.ResourcePath(file_path)) as jobfile:
    return jobfile.read()


def GenerateJobFile(
    disks,
    scenarios_list,
    benchmark_params=None,
    job_file='fio-parent.job',
    device_path=None,
):
  """Generates a fio job file based on the provided disks and flags.

  Args:
    disks: A list of disk objects.
    scenarios_list: A list of scenario strings.
    benchmark_params: A dict for fio test specific parameters.
    job_file: The name of the job file.
    device_path: The device path on which to run fio.

  Returns:
    A string containing the contents of the fio job file.
  """
  if benchmark_params is None:
    benchmark_params = {}

  scenarios = [
      fio_scenario_parser.FioScenarioParser().GetScenarioFromScenarioStringAndParams(
          scenario_string.strip('"'), benchmark_params
      )
      for scenario_string in scenarios_list
  ]

  # Higher priority to custom job file, if passed by the user.
  job_file_name = (
      fio_flags.FIO_JOBFILE.value if fio_flags.FIO_JOBFILE.value else job_file
  )
  default_fio_job_file = GetFileAsString(
      data.ResourcePath(f'fio/{job_file_name}')
  )
  job_file_template = jinja2.Template(
      default_fio_job_file, undefined=jinja2.StrictUndefined
  )
  filename = GetFilename(disks, device_path=device_path)
  disks_list = [{'index': 0}]
  if fio_flags.FIO_SEPARATE_JOBS_FOR_DISKS.value:
    disks_list = SeparateJobsForDisks(GetAllDiskPaths(disks))
  return job_file_template.render(
      ioengine=fio_flags.FIO_IOENGINE.value,
      runtime=benchmark_params.get('runtime') or fio_flags.FIO_RUNTIME.value,
      ramptime=benchmark_params.get('ramptime') or fio_flags.FIO_RAMPTIME.value,
      fio_run_parallel_jobs_on_disks=fio_flags.FIO_RUN_PARALLEL_JOBS_ON_DISKS.value,
      scenarios=scenarios,
      direct=int(fio_flags.DIRECT_IO.value),
      disks_list=disks_list,
      filename=filename,
      separate_jobs=fio_flags.FIO_SEPARATE_JOBS_FOR_DISKS.value,
      extra_params=benchmark_params,
  )


def FillTarget():
  """Check whether we should pre-fill our target or not.

  Returns:
    True if we should pre-fill our target, False if not.
  """
  return fio_flags.FIO_TARGET_MODE.value in constants.FILL_TARGET_MODES


def FillDevice(vm, disk, fill_size, exec_path):
  """Fill the given disk on the given vm up to fill_size.

  Args:
    vm: a linux_virtual_machine.BaseLinuxMixin object.
    disk: a disk.BaseDisk attached to the given vm.
    fill_size: amount of device to fill, in fio format.
    exec_path: string path to the fio executable
  """
  command = (
      f'sudo {exec_path} --filename={disk.GetDevicePath()} '
      f'--ioengine={fio_flags.FIO_IOENGINE.value} --name=fill-device '
      f'--blocksize={fio_flags.FIO_FILL_BLOCK_SIZE.value} --iodepth=64 --rw=write --direct=1 --size={fill_size}'
  )

  vm.RobustRemoteCommand(command)


def PrefillIfEnabled(vm, exec_path):
  """Prefills the target device or file on the given VM."""
  if len(vm.scratch_disks) > 1 and not AgainstDevice():
    raise errors.Setup.InvalidSetupError(
        f'Target mode {fio_flags.FIO_TARGET_MODE.value} tests against 1 file, '
        'but multiple scratch disks are configured.'
    )
  if FillTarget():
    logging.info(
        'Fill devices %s on %s',
        [disk.GetDevicePath() for disk in vm.scratch_disks],
        vm,
    )
    background_tasks.RunThreaded(
        lambda disk: FillDevice(
            vm, disk, fio_flags.FIO_FILL_SIZE.value, exec_path
        ),
        vm.scratch_disks,
    )


def MountDisk(vm):
  """Mounts Disks for fio.

  Args:
    vm: a linux_virtual_machine.BaseLinuxMixin object.
  """
  # We only need to format and mount if the target mode is against
  # file with fill because 1) if we're running against the device, we
  # don't want it mounted and 2) if we're running against a file
  # without fill, it was never unmounted (see GetConfig()).
  if (
      len(vm.scratch_disks) == 1
      and fio_flags.FIO_TARGET_MODE.value
      == constants.AGAINST_FILE_WITH_FILL_MODE
  ):
    disk = vm.scratch_disks[0]
    disk.mount_point = FLAGS.scratch_dir or constants.MOUNT_POINT
    disk_spec = vm.create_disk_strategy.disk_specs[0]
    vm.FormatDisk(disk.GetDevicePath(), disk_spec.disk_type)
    vm.MountDisk(
        disk.GetDevicePath(),
        disk.mount_point,
        disk_spec.disk_type,
        disk.mount_options,
        disk.fstab_options,
    )


def WriteJobFileToTempFile(vm, job_file_string):
  job_file_path = vm_util.PrependTempDir(vm.name + '_fio.job')
  with open(job_file_path, 'w') as job_file:
    job_file.write(job_file_string)
    logging.info('Wrote fio job file at %s', job_file_path)
    logging.info(job_file_string)
  return job_file_path


def RunTest(vm, exec_path, job_file_string):
  """Runs the fio test on the VM using the provided executable path.

  Args:
    vm: a linux_virtual_machine.BaseLinuxMixin object.
    exec_path: string path to the fio executable.
    job_file_string: string content of the fio job file.

  Returns:
    A list of sample.Sample objects.
  """
  logging.info('FIO running on %s', vm)
  job_file_temp_path = WriteJobFileToTempFile(vm, job_file_string)
  remote_fio_job_file_path = posixpath.join(vm_util.VM_TMP_DIR, 'fio.job')
  vm.PushFile(job_file_temp_path, remote_fio_job_file_path)
  disks = vm.scratch_disks
  if AgainstDevice():
    fio_command = (
        f'sudo {exec_path} --output-format=json '
        f'--random_generator={fio_flags.FIO_RNG.value} {remote_fio_job_file_path}'
    )
  else:
    assert(len(disks) == 1)
    fio_command = (
        f'sudo {exec_path} --output-format=json '
        f'--random_generator={fio_flags.FIO_RNG.value} '
        f'--directory={disks[0].mount_point} {remote_fio_job_file_path}'
    )
  logging.info('FIO Results:')
  start_time = time.time()
  stdout, _ = vm.RobustRemoteCommand(
      fio_command, timeout=fio_flags.FIO_COMMAND_TIMEOUT_SEC.value
  )
  end_time = time.time()
  samples = result_parser.ParseResults(
      job_file_string,
      json.loads(stdout),
  )

  samples.append(
      sample.Sample('start_time', start_time, 'sec', samples[0].metadata)
  )
  samples.append(
      sample.Sample('end_time', end_time, 'sec', samples[0].metadata)
  )
  return samples
