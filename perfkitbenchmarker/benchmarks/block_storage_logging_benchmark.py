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

"""Runs fio benchmarks to simulate logging.

Man: http://manpages.ubuntu.com/manpages/natty/man1/fio.1.html
Quick howto: http://www.bluestop.org/fio/HOWTO.txt

Simulated logging benchmark does the following things (PD only):
0) Do NOT use direct IO for any tests below, simply go through the FS.
1) Sequentially write x GB with queue depth equal to 8, where x is decided by
   the test VM's total memory. (A larger VM will write more bytes)
2) Random read of 10% of the bytes written.
3) Sequential read of all of the bytes written.

For AWS, where use PD, we should use EBS-GP and EBS Magnetic, for PD-SSD use
EBS-GP and PIOPS.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker.packages import fio

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'block_storage_logging',
                  'description': 'Runs FIO to simulate logging scenarios.',
                  'scratch_disk': True,
                  'num_machines': 1}

DEFAULT_IODEPTH = 8


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Prepare the virtual machine to run FIO.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  logging.info('Prepare fio on vm: %s', vm)
  vm.Install('fio')


def Run(benchmark_spec):
  """Spawn fio to simulate logging and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]
  logging.info('Simulating logging on vm: %s', vm)
  test_size = vm.total_memory_kb
  cmd = (
      '--filesize=10g '
      '--directory=%s '
      '--ioengine=libaio '
      '--filename=fio_test_file '
      '--invalidate=1 '
      '--randrepeat=0 '
      '--direct=0 '
      '--size=%dk '
      '--iodepth=%d ') % (vm.GetScratchDir(),
                          test_size,
                          DEFAULT_IODEPTH)
  if FLAGS.maxjobs:
    cmd += '--max-jobs=%s ' % FLAGS.maxjobs
  cmd += (
      '--name=sequential_write '
      '--overwrite=0 '
      '--rw=write '
      '--end_fsync=1 '
      '--name=random_read '
      '--size=%dk '
      '--stonewall '
      '--rw=randread '
      '--name=sequential_read '
      '--stonewall '
      '--rw=read ') % (test_size / 10)
  logging.info('Block_storage_logging results:')
  res, _ = vm.RemoteCommand('%s %s' % (fio.FIO_CMD_PREFIX, cmd),
                            should_log=True)
  results = fio.ParseResults(fio.FioParametersToJob(cmd), json.loads(res))
  return results


def Cleanup(benchmark_spec):
  """Uninstall packages required for fio and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  logging.info('Cleanup on vm: %s', vm)
  vm.RemoveFile(vm.GetScratchDir() + '/fio_test_file')
