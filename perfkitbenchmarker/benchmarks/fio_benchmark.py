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

import datetime
import json
import logging
import posixpath
import re

import jinja2

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import fio

LOCAL_JOB_FILE_NAME = 'fio.job'  # used with vm_util.PrependTempDir()
REMOTE_JOB_FILE_PATH = posixpath.join(vm_util.VM_TMP_DIR, 'fio.job')
DEFAULT_TEMP_FILE_NAME = 'fio-temp-file'
MINUTES_PER_JOB = 10


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
                     'will generate a job file that uses the block device. '
                     'When using --against_device, default to pre-filling the '
                     'device to 100% unless modified with --device_fill_size.')
flags.DEFINE_string('device_fill_size', '100%',
                    'The amount of device to fill in prepare stage. '
                    'The valid value can either be an integer, which '
                    'represents the number of bytes to fill or a '
                    'percentage, which represents the percentage '
                    'of the device. A filesystem will be unmounted before '
                    'filling and remounted afterwards. Default is not to fill.')
flags.DEFINE_string('io_depths', '1',
                    'IO queue depths to run on. Can specify a single number, '
                    'like --io_depths=1, a range, like --io_depths=1-4, or a '
                    'list, like --io_depths=1-4,6-8')
flags.DEFINE_integer('working_set_size', None,
                     'The size of the working set, in GB. If not given, use '
                     'the full size of the device. If using '
                     '--generate_scenarios and not --against_device, you must '
                     'pass --working_set_size.',
                     lower_bound=0)
flags.DEFINE_integer('run_for_minutes', 10,
                     'Repeat the job scenario(s) for the given number of '
                     'minutes. Only valid when using --generate_scenarios. '
                     'When using multiple scenarios, each one is run for the '
                     'given number of minutes. Time will be rounded up to the '
                     'next multiple of %s minutes.' % MINUTES_PER_JOB,
                     lower_bound=0)


FLAGS_IGNORED_FOR_CUSTOM_JOBFILE = {
    'generate_scenarios', 'io_depths', 'run_for_minutes'}


IODEPTHS_REGEXP = re.compile(r'(\d+)(-(\d+))?$')


def GetIODepths(io_depths):
  """Parse the io_depths parameter.

  Args:
    io_depths: a string in the format of the --io_depths flag.

  Returns:
    A list of integers.

  Raises:
    ValueError if io_depths doesn't follow a format it recognizes.
  """

  groups = io_depths.split(',')
  result = []

  for group in groups:
    match = IODEPTHS_REGEXP.match(group)
    if match is None:
      raise ValueError('Invalid io_depths expression %s', io_depths)
    elif match.group(2) is None:
      result.append(int(match.group(1)))
    else:
      result.extend(range(int(match.group(1)), int(match.group(3)) + 1))

  return result


def WarnIODepths(depths_list):
  """Given a list of IO depths, log warnings if it seems "weird".

  Args:
    depths_list: a list of IO depths.
  """

  for i in range(len(depths_list) - 1):
    if depths_list[i] >= depths_list[i + 1]:
      logging.warning('IO depths list is not monotonically increasing: '
                      '%s comes before %s.', depths_list[i], depths_list[i + 1])

  values = set()
  warned_on = set()
  for val in depths_list:
    if val in values and val not in warned_on:
      logging.warning('IO depths list contains duplicate entry %s.', val)
      warned_on.add(val)
    else:
      values.add(val)


def IODepthsValidator(string):
  try:
    WarnIODepths(GetIODepths(string))
    return True
  except ValueError:
    return False


flags.RegisterValidator('io_depths',
                        IODepthsValidator,
                        message='--io_depths must be an integer, '
                                'range of integers, or a list of '
                                'integers and ranges, all > 0')


def FillDevice(vm, disk, fill_size):
  """Fill the given disk on the given vm up to fill_size.

  Args:
    vm: a virtual_machine.VirtualMachine object.
    disk: a disk.BaseDisk attached to the given vm.
    fill_size: amount of device to fill, in fio format.
  """

  command = (('sudo %s --filename=%s --ioengine=libaio '
              '--name=fill-device --blocksize=512k --iodepth=64 '
              '--rw=write --direct=1 --size=%s') %
             (fio.FIO_PATH, disk.GetDevicePath(), fill_size))

  vm.RemoteCommand(command)


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
runtime={{minutes_per_job}}m
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

SECONDS_PER_MINUTE = 60


def GenerateJobFileString(filename, scenario_strings,
                          io_depths_string, working_set_size):
  """Make a string with our fio job file.

  Args:
    filename: the file or disk we pre-filled, if any.
    scenario_strings: list of strings with names in SCENARIOS.
    io_depths_string: string. The IO queue depths to test.
    working_set_size: int or None. If int, the size of the working set in GB.

  Returns:
    The contents of a fio job file, as a string.
  """

  if 'all' in scenario_strings:
    scenarios = SCENARIOS.itervalues()
  else:
    for name in scenario_strings:
      if name not in SCENARIOS:
        logging.error('Unknown scenario name %s', name)
    scenarios = (SCENARIOS[name] for name in scenario_strings)

  size_string = str(working_set_size) + 'G' if working_set_size else '100%'

  job_file_template = jinja2.Template(JOB_FILE_TEMPLATE,
                                      undefined=jinja2.StrictUndefined)

  io_depths = GetIODepths(io_depths_string)

  return str(job_file_template.render(
      minutes_per_job=MINUTES_PER_JOB,
      filename=filename,
      size=size_string,
      scenarios=scenarios,
      iodepths=io_depths))


FILENAME_PARAM_REGEXP = re.compile('filename\s*=.*$', re.MULTILINE)


def ProcessedJobFileString(fio_jobfile, remove_filename):
  """Get the contents of a job file as a string, slightly edited.

  Args:
    fio_jobfile: the path to a fio job file.
    remove_filename: bool. If true, remove the filename parameter from
      the job file.

  Returns:
    The job file as a string, possibly without filename parameters.
  """

  with open(fio_jobfile, 'r') as jobfile:
    if remove_filename:
      return FILENAME_PARAM_REGEXP.sub('', jobfile.read())
    else:
      return jobfile.read()


def GetOrGenerateJobFileString(job_file_path, scenario_strings,
                               against_device, disk,
                               io_depths, working_set_size):
  """Get the contents of the fio job file we're working with.

  This will either read the user's job file, if given, or generate a
  new one.

  Args:
    job_file_path: string or None. The path to the user's job file, if
      provided.
    scenario_strings: list of strings or None. The workload scenarios
      to generate.
    against_device: bool. True if testing against a raw device, False
      if testing against a filesystem.
    disk: the disk.BaseDisk object to test against.
    io_depths: string. The IO queue depths to test.
    working_set_size: int or None. If int, the size of the working set
      in GB.

  Returns:
    A string containing a fio job file.
  """

  use_user_jobfile = job_file_path or not scenario_strings

  if use_user_jobfile:
    remove_filename = against_device
    return ProcessedJobFileString(job_file_path or data.ResourcePath('fio.job'),
                                  remove_filename)
  else:
    if against_device:
      filename = disk.GetDevicePath()
    else:
      # Since we pass --directory to fio, we must use relative file
      # paths or get an error.
      filename = DEFAULT_TEMP_FILE_NAME

    return GenerateJobFileString(filename, scenario_strings,
                                 io_depths, working_set_size)


NEED_SIZE_MESSAGE = ('You must specify the working set size when using '
                     'generated scenarios with a filesystem.')


def WarnOnBadFlags():
  """Warn the user if they pass bad flag combinations."""

  if FLAGS.fio_jobfile:
    ignored_flags = {'--' + flag_name
                     for flag_name in FLAGS_IGNORED_FOR_CUSTOM_JOBFILE
                     if FLAGS[flag_name].present}

    if ignored_flags:
      logging.warning('Fio job file specified. Ignoring options "%s"',
                      ', '.join(ignored_flags))

  if FLAGS.run_for_minutes % MINUTES_PER_JOB != 0:
    logging.warning('Runtime %s will be rounded up to the next multiple of %s '
                    'minutes.', FLAGS.run_for_minutes, MINUTES_PER_JOB)

  if (FLAGS.fio_jobfile is None and
      FLAGS.generate_scenarios and
      not FLAGS.working_set_size and
      not FLAGS.against_device):
    logging.error(NEED_SIZE_MESSAGE)
    raise errors.Benchmarks.PrepareException(NEED_SIZE_MESSAGE)


def RunForMinutes(proc, mins_to_run, mins_per_call):
  """Call func until expected execution time passes threshold.

  Args:
    proc: a procedure to call.
    mins_to_run: the minimum number of minutes to run func for.
    mins_per_call: the expected elapsed time of each call to func.
  """

  run_reps = mins_to_run // mins_per_call
  if mins_to_run % mins_per_call != 0:
    run_reps += 1

  start_time = datetime.datetime.now()
  for rep_num in xrange(run_reps):
    run_start = datetime.datetime.now()
    seconds_since_start = int(round((run_start - start_time).total_seconds()))
    minutes_since_start = int(round(float(seconds_since_start)
                                    / float(SECONDS_PER_MINUTE)))
    proc(repeat_number=rep_num,
         minutes_since_start=minutes_since_start,
         total_repeats=run_reps)


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

  WarnOnBadFlags()

  vm = benchmark_spec.vms[0]
  logging.info('FIO prepare on %s', vm)
  vm.Install('fio')

  # Choose a disk or file name and optionally fill it
  disk = vm.scratch_disks[0]
  mount_point = disk.mount_point

  logging.info('Umount scratch disk on %s at %s', vm, mount_point)
  vm.RemoteCommand('sudo umount %s' % mount_point)

  if FLAGS['device_fill_size'].present or FLAGS.against_device:
    logging.info('Fill device %s on %s', disk.GetDevicePath(), vm)
    FillDevice(vm, disk, FLAGS.device_fill_size)

  if not FLAGS.against_device:
    vm.FormatDisk(disk.GetDevicePath())
    # Don't use vm.MountDisk() because that also creates the mount point
    # and changes its permissions, which we have already done.
    vm.RemoteCommand('sudo mount %s %s' % (disk.GetDevicePath(), mount_point))


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

  disk = vm.scratch_disks[0]
  mount_point = disk.mount_point

  job_file_string = GetOrGenerateJobFileString(FLAGS.fio_jobfile,
                                               FLAGS.generate_scenarios,
                                               FLAGS.against_device,
                                               disk,
                                               FLAGS.io_depths,
                                               FLAGS.working_set_size)
  job_file_path = vm_util.PrependTempDir(LOCAL_JOB_FILE_NAME)
  with open(job_file_path, 'w') as job_file:
    job_file.write(job_file_string)
    logging.info('Wrote fio job file at %s', job_file_path)

  vm.PushFile(job_file_path, REMOTE_JOB_FILE_PATH)

  if FLAGS.against_device:
    fio_command = 'sudo %s --output-format=json --filename=%s %s' % (
        fio.FIO_PATH, disk.GetDevicePath(), REMOTE_JOB_FILE_PATH)
  else:
    fio_command = 'sudo %s --output-format=json --directory=%s %s' % (
        fio.FIO_PATH, mount_point, REMOTE_JOB_FILE_PATH)

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

    samples.extend(fio.ParseResults(job_file_string,
                                    json.loads(stdout),
                                    base_metadata=base_metadata))

  # TODO(user): This only gives results at the end of a job run
  #      so the program pauses here with no feedback to the user.
  #      This is a pretty lousy experience.
  logging.info('FIO Results:')

  if not FLAGS['run_for_minutes'].present:
    RunIt()
  else:
    RunForMinutes(RunIt, FLAGS.run_for_minutes, MINUTES_PER_JOB)

  return samples


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
