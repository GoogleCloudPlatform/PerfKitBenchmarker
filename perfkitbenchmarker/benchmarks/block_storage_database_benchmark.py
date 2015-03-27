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

"""Runs fio benchmarks to simulate database.

Man: http://manpages.ubuntu.com/manpages/natty/man1/fio.1.html
Quick howto: http://www.bluestop.org/fio/HOWTO.txt

Simulated database benchmark does the following things (PD, PD-SSD, local SSD):
1) 4K Random R on a file using queue depths 16 and 64 (each queue depth
   is a different benchmark).
2) 4K Random W on a file using queue depths 16 and 64 (each queue depth
   is a different benchmark).
3) 4K Random 90% R/ 10% W on a file using queue depths 1, 16 and 64 (each
   queue depth is a different benchmark).
4) The size of the test file is decided by the test VM's total memory and capped
   at 1GB to ensure this test finishes within reasonable time.

For AWS, where use PD, we should use EBS-GP and EBS Magnetic, for PD-SSD use
EBS-GP and PIOPS.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker.packages import fio

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'block_storage_database',
                  'description': 'Runs FIO to simulate database scenarios.',
                  'scratch_disk': True,
                  'num_machines': 1}

DEFAULT_DATABASE_SIMULATION_IODEPTH_LIST = [16, 64]


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
  """Spawn fio to simulate database and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]
  logging.info('Simulating logging on vm: %s', vm)
  test_size = min(vm.total_memory_kb / 10, 1000000)
  iodepth_list = FLAGS.iodepth_list or DEFAULT_DATABASE_SIMULATION_IODEPTH_LIST
  results = []
  for depth in iodepth_list:
    cmd = (
        '--filesize=10g '
        '--directory=%s '
        '--ioengine=libaio '
        '--filename=fio_test_file '
        '--overwrite=1 '
        '--invalidate=0 '
        '--direct=1 '
        '--randrepeat=0 '
        '--iodepth=%s '
        '--size=%dk '
        '--blocksize=4k ') % (vm.GetScratchDir(),
                              depth,
                              test_size)
    if FLAGS.maxjobs:
      cmd += '--max-jobs=%s ' % FLAGS.maxjobs
    cmd += (
        '--name=random_write '
        '--rw=randwrite '
        '--end_fsync=1 '
        '--name=random_read '
        '--stonewall '
        '--rw=randread '
        '--name=mixed_randrw '
        '--stonewall '
        '--rw=randrw '
        '--rwmixread=90 '
        '--rwmixwrite=10 '
        '--end_fsync=1 ')
    logging.info('Block_storage_database results, iodepth %s', depth)
    res, _ = vm.RemoteCommand('%s %s' % (fio.FIO_CMD_PREFIX, cmd),
                              should_log=True)
    results.extend(
        fio.ParseResults(fio.FioParametersToJob(cmd), json.loads(res)))
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
