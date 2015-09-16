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
import posixpath
import re

import jinja2

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import fio

LOCAL_JOB_FILE_NAME = 'fio.job'  # used with vm_util.PrependTempDir()
REMOTE_JOB_FILE_PATH = posixpath.join(vm_util.VM_TMP_DIR, 'fio.job')
DEFAULT_TEMP_FILE_NAME = 'fio-temp-file'
DISK_USABLE_SPACE_FRACTION = 0.9


# This dictionary maps scenario names to dictionaries of fio settings.
SCENARIOS = {
    'sequential_write': {
        'name': 'sequential_write',
        'rwkind': 'write',
        'blocksize': '512k'
    },
    'sequential_read': {
        'name': 'sequential_read',
        'rwkind': 'read',
        'blocksize': '512k'
    },
    'random_write': {
        'name': 'random_write',
        'rwkind': 'randwrite',
        'blocksize': '4k'
    },
    'random_read': {
        'name': 'random_read',
        'rwkind': 'randread',
        'blocksize': '4k'
    }
}


FLAGS = flags.FLAGS

flags.DEFINE_string('fio_jobfile', None,
                    'Job file that fio will use. If not given, use a job file '
                    'bundled with PKB. Cannot use with --generate_scenarios.')
flags.DEFINE_list('generate_scenarios', None,
                  'Generate a job file with the given scenarios. Special '
                  'scenario \'all\' generates all scenarios. Available '
                  'scenarios are sequential_write, sequential_read, '
                  'random_write, and random_read. Cannot use with '
                  '--fio_jobfile.')
flags.DEFINE_boolean('against_device', False,
                     'Unmount the device\'s filesystem so we can test against '
                     'the raw block device. If --generate-scenarios is given, '
                     'will generate a job file that uses the block device.')
flags.DEFINE_string('device_fill_size', '100%',
                    'The amount of device to fill in prepare stage. '
                    'The valid value can either be an integer, which '
                    'represents the number of bytes to fill or a '
                    'percentage, which represents the percentage '
                    'of the device. Default to filling 100% of a raw device or '
                    '90% of a filesystem.')
flags.DEFINE_string('io_depths', '1',
                    'IO queue depths to run on. Can specify a single number, '
                    'like --io_depths=1, or a range, like --io_depths=1-4')
flags.DEFINE_integer('working_set_size', None,
                     'The size of the working set, in GB. If not given, use '
                     'the full size of the device.',
                     lower_bound=0)



FLAGS_IGNORED_FOR_CUSTOM_JOBFILE = {
    'generate_scenarios', 'io_depths'}


IODEPTHS_REGEXP = re.compile(r'(\d+)(-(\d+))?$')


def IODepthsValidator(string):
  match = IODEPTHS_REGEXP.match(string)
  return match and int(match.group(1)) > 0


flags.RegisterValidator('io_depths',
                        IODepthsValidator,
                        message='--io_depths must be an integer '
                                'or a range of integers, all > 0')


def GenerateFillCommand(fio_path, fill_path, fill_size):
  """Generate a command to sequentially write a device or file.

  Args:
    fio_path: path to the fio executable.
    fill_path: path to the device or file to fill.
    fill_size: amount of device or file to fill, in fio format.

  Returns:
    A string containing the command.
  """

  return (('sudo %s --filename=%s --ioengine=libaio '
           '--name=fill-device --blocksize=512k --iodepth=64 '
           '--rw=write --direct=1 --size=%s') %
          (fio_path, fill_path, fill_size))


BENCHMARK_INFO = {'name': 'fio',
                  'description': 'Runs fio in sequential, random, read '
                                 'and write modes.',
                  'scratch_disk': True,
                  'num_machines': 1}


JOB_FILE_TEMPLATE = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=10m
time_based
filename={{filename}}
do_verify=0
verify_fatal=0
randrepeat=0

{% for scenario in scenarios %}
{% for iodepth in iodepths %}
[{{scenario['name']}}-io-depth-{{iodepth}}]
stonewall
rw={{scenario['rwkind']}}
blocksize={{scenario['blocksize']}}
iodepth={{iodepth}}
size={{size}}
{% endfor %}
{% endfor %}
"""


def GetIODepths(io_depths):
  """Parse the io_depths parameter.

  Args:
    io_depths: a string in the format of the --io_depths flag.

  Returns:
    An iterable of integers.

  Raises:
    ValueError if io_depths doesn't follow a format it recognizes.
  """

  match = IODEPTHS_REGEXP.match(io_depths)

  if match.group(2) is None:
    return [int(match.group(1))]
  else:
    return range(int(match.group(1)), int(match.group(3)) + 1)


def GenerateJobFileString(disk, against_device,
                          scenarios, io_depths, working_set_size):
  """Make a string with our fio job file.

  Args:
    disk: the disk.BaseDisk object we're benchmarking with.
    against_device: bool. True if we're using a raw disk.
    scenarios: iterable of dicts, taken from SCENARIOS.
    io_depths: iterable. The IO queue depths to test.
    working_set_size: int or None. If int, the size of the working set in GB.

  Returns:
    The contents of a fio job file, as a string.
  """

  if against_device:
    filename = disk.GetDevicePath()
  else:
    filename = posixpath.join(disk.mount_point, DEFAULT_TEMP_FILE_NAME)

  size_string = str(working_set_size) + 'G' if working_set_size else '100%'

  job_file_template = jinja2.Template(JOB_FILE_TEMPLATE,
                                      undefined=jinja2.StrictUndefined)

  return str(job_file_template.render(
      filename=filename,
      size=size_string,
      scenarios=scenarios,
      iodepths=io_depths))


def GetOrGenerateJobFileString(fio_jobfile, disk, against_device,
                               scenario_strings, io_depths, working_set_size):
  """Get the contents of the fio job file we're working with.

  This will either read the user's job file, if given, or generate a
  new one.

  Args:
    fio_jobfile: string or None. The path to the user's jobfile, if provided.
    disk: the disk.BaseDisk object we're benchmarking with.
    against_device: bool. True if we're using a raw disk.
    scenario_strings: list of strings or None. The workload scenarios to
      generate.
    io_depths: iterable. The IO queue depths to test.
    working_set_size: int or None. If int, the size of the working set in GB.

  Returns:
    A string containing a fio job file.
  """

  # The default behavior is to run with the standard fio job file.
  if scenario_strings is None and not fio_jobfile:
    fio_jobfile = data.ResourcePath('fio.job')

  if fio_jobfile:
    with open(fio_jobfile, 'r') as jobfile:
      return jobfile.read()
  else:
    if 'all' in scenario_strings:
      scenarios = SCENARIOS.itervalues()
    else:
      for name in scenario_strings:
        if name not in SCENARIOS:
          logging.error('Unknown scenario name %s', name)
      scenarios = (SCENARIOS[name] for name in scenario_strings)

    return GenerateJobFileString(disk, against_device, scenarios,
                                 io_depths, working_set_size)


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Prepare the virtual machine to run FIO.

     This includes installing fio, bc, and libaio1 and pre-filling the
     attached disk. We also make sure the job file is always located
     at the same path on the local machine.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """

  if FLAGS.fio_jobfile:
    ignored_flags = {'--' + flag_name
                     for flag_name in FLAGS_IGNORED_FOR_CUSTOM_JOBFILE
                     if FLAGS[flag_name].present}

    if ignored_flags:
      logging.warning('Fio job file specified. Ignoring options "%s"',
                      ', '.join(ignored_flags))

  vm = benchmark_spec.vms[0]
  logging.info('FIO prepare on %s', vm)
  vm.Install('fio')

  # Fill the disk or file we're using
  disk = vm.scratch_disks[0]
  device_path = disk.GetDevicePath()
  mount_point = disk.mount_point
  if FLAGS.against_device:
    logging.info('Umount scratch disk on %s at %s', vm, mount_point)
    vm.RemoteCommand('sudo umount %s' % mount_point)

  if FLAGS.device_fill_size is not '0':
    if FLAGS.against_device:
      fill_path = device_path
      fill_size = FLAGS.device_fill_size
    else:
      fill_path = posixpath.join(mount_point, DEFAULT_TEMP_FILE_NAME)
      if FLAGS['device_fill_size'].present:
        fill_size = FLAGS.device_fill_size
      else:
        # Default to 90% of capacity because the file system will add
        # some overhead.
        fill_size = str(int(DISK_USABLE_SPACE_FRACTION *
                            1000 * disk.disk_size)) + 'M'

    logging.info('Fill file %s on %s', fill_path, vm)
    command = GenerateFillCommand(fio.FIO_PATH,
                                  fill_path,
                                  fill_size)
    vm.RemoteCommand(command)

  job_file_path = vm_util.PrependTempDir(LOCAL_JOB_FILE_NAME)
  with open(job_file_path, 'w') as job_file:
    job_file.write(GetOrGenerateJobFileString(FLAGS.fio_jobfile,
                                              disk,
                                              FLAGS.against_device,
                                              FLAGS.generate_scenarios,
                                              GetIODepths(FLAGS.io_depths),
                                              FLAGS.working_set_size))
    logging.info('Wrote fio job file at %s', job_file_path)

  vm.PushFile(job_file_path, REMOTE_JOB_FILE_PATH)


def Run(benchmark_spec):
  """Spawn fio and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
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

  disk = vm.scratch_disks[0]
  return fio.ParseResults(
      GetOrGenerateJobFileString(FLAGS.fio_jobfile,
                                 disk,
                                 FLAGS.against_device,
                                 FLAGS.generate_scenarios,
                                 GetIODepths(FLAGS.io_depths),
                                 FLAGS.working_set_size),
      json.loads(stdout))


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
    vm.RemoveFile(posixpath.join(vm.GetScratchDir(), DEFAULT_TEMP_FILE_NAME))
