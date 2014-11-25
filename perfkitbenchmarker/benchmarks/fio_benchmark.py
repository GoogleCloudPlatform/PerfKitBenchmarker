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



import gflags as flags
import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('fio_benchmark_filename', 'fio_benchmark_file',
                    'scratch file that fio will use')
flags.DEFINE_string('fio_jobfile', 'fio.job', 'job file that fio will use')
flags.DEFINE_integer('memory_multiple', 10,
                     'size of fio scratch file compared to main memory size.')

jobfile_directory = 'data/'

BENCHMARK_INFO = {'name': 'fio_benchmark',
                  'description': 'Runs FIO in sequenctial, random, read '
                                 'and write modes.',
                  'scratch_disk': True,
                  'num_machines': 1}
REQUIRED_PACKAGES = 'bc fio libaio1'


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
  vm.InstallPackage(REQUIRED_PACKAGES)
  file_path = jobfile_directory + flags.FLAGS.fio_jobfile
  vm.PushFile(file_path)
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
  # TODO(user): what, this doesn't need the scratch directory?
  logging.info('FIO running on %s', vm)
  fio_command = 'fio %s' % (flags.FLAGS.fio_jobfile)
  # TODO(user): This only gives results at the end of a job run
  #      so the program pauses here with no feedback to the user.
  #      This is a pretty lousy experience.
  logging.info('FIO Results:')
  vm.RemoteCommand(fio_command, should_log=True)
  # TODO(user): The hard work! Parsing this output!
  return []


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
  vm.UninstallPackage(REQUIRED_PACKAGES)
  vm.RemoveFile(vm.GetScratchDir() + flags.FLAGS.fio_benchmark_filename)
