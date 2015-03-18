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

"""Runs fio benchmarks to simulate logging, database and streaming.

Man: http://manpages.ubuntu.com/manpages/natty/man1/fio.1.html
Quick howto: http://www.bluestop.org/fio/HOWTO.txt

Simulated logging benchmark does the following things (PD only):
0) Do NOT use direct IO for any tests below, simply go through the FS.
1) Sequentially write x GB with queue depth equal to 8, where x is decided by
   the test VM's total memory. (A larger VM will write more bytes)
2) Random read of 10% of the bytes written.
3) Sequential read of all of the bytes written.

Simulated database benchmark does the following things (PD, PD-SSD, local SSD):
1) 4K Random R on a file using queue depths 1, 16 and 64 (each queue depth
   is a different benchmark).
2) 4K Random W on a file using queue depths 1, 16 and 64 (each queue depth
   is a different benchmark).
3) 4K Random 90% R/ 10% W on a file using queue depths 1, 16 and 64 (each
   queue depth is a different benchmark).
4) The size of the test file is decided by the test VM's total memory and capped
   at 1GB to ensure this test finishes within reasonable time.

Simulated streaming benchmark (PD only):
1) 1M Seq R at queue depth 1 and 16 (streaming).
2) 1M Seq W at queue depth 1 and 16 (streaming).

For AWS, where use PD, we should use EBS-GP and EBS Magnetic, for PD-SSD use
EBS-GP and PIOPS.
"""

import json
import logging
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.packages import fio

LOGGING = 'logging'
DATABASE = 'database'
STREAMING = 'streaming'

flags.DEFINE_enum('workload_mode', LOGGING,
                  [LOGGING, DATABASE, STREAMING],
                  'Simulate a logging, database or streaming scenario.')

flags.DEFINE_list('iodepth_list', None, 'A list of iodepth parameter used by '
                  'fio command in simulated database and streaming scenarios '
                  'only.')

flags.DEFINE_integer('maxjobs', 0,
                     'The maximum allowed number of jobs to support.')

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'block_storage_workload',
                  'description': 'Runs FIO in sequential, random, read and '
                                 'write modes to simulate various scenarios.',
                  'scratch_disk': True,
                  'num_machines': 1}
DESCRIPTION = 'description'
METHOD = 'method'

DEFAULT_IODEPTH = 8
DEFAULT_DATABASE_SIMULATION_IODEPTH_LIST = [16, 64]
DEFAULT_STREAMING_SIMULATION_IODEPTH_LIST = [1, 16]

LATENCY_REGEX = r'[=\s]+([\d\.]+)[\s,]+'
BANDWIDTH_REGEX = r'(\d+)(\w+/*\w*)'


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


def RunSimulatedLogging(vm):
  """Spawn fio to simulate logging and gather the results.
  Args:
    vm: The vm that synthetic_storage_workloads_benchmark will be run upon.
  Returns:
    A list of sample.Sample objects
  """
  test_size = vm.total_memory_kb
  cmd = (
      '%s --output-format=json '
      '--filesize=10g '
      '--directory=%s '
      '--ioengine=libaio '
      '--filename=fio_test_file '
      '--invalidate=1 '
      '--randrepeat=0 '
      '--direct=0 '
      '--size=%dk '
      '--iodepth=%d ') % (fio.FIO_PATH,
                          vm.GetScratchDir(),
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
  logging.info('FIO Results for simulated %s', LOGGING)
  res, _ = vm.RemoteCommand(cmd, should_log=True)
  fio.ParseResults(cmd, json.loads(res))
  return results


def RunSimulatedDatabase(vm):
  """Spawn fio to simulate database and gather the results.

  Args:
    vm: The vm that synthetic_storage_workloads_benchmark will be run upon.
  Returns:
    A list of sample.Sample objects
  """
  test_size = min(vm.total_memory_kb / 10, 1000000)
  iodepth_list = FLAGS.iodepth_list or DEFAULT_DATABASE_SIMULATION_IODEPTH_LIST
  results = []
  for depth in iodepth_list:
    cmd = (
        '%s '
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
        '--blocksize=4k ') % (fio.FIO_PATH,
                              vm.GetScratchDir(),
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
    logging.info('FIO Results for simulated %s, iodepth %s', DATABASE, depth)
    res, _ = vm.RemoteCommand(cmd, should_log=True)
    latency, bandwidth = ParseFioResult(res)
    result = [CreateSampleFromBandwidthTuple(bandwidth[0],
                                             'random_write:bandwidth', depth,
                                             test_size),
              CreateSampleFromBandwidthTuple(bandwidth[1],
                                             'random_read:bandwidth', depth,
                                             test_size),
              CreateSampleFromBandwidthTuple(bandwidth[2],
                                             'mixed_randrw:read:bandwidth',
                                             depth, test_size),
              CreateSampleFromBandwidthTuple(bandwidth[3],
                                             'mixed_randrw:write:bandwidth',
                                             depth, test_size),
              CreateSampleFromLatencyTuple(latency[0], 'random_write:latency',
                                           depth, test_size),
              CreateSampleFromLatencyTuple(latency[1], 'random_read:latency',
                                           depth, test_size),
              CreateSampleFromLatencyTuple(latency[2],
                                           'mixed_randrw:read:latency', depth,
                                           test_size),
              CreateSampleFromLatencyTuple(latency[3],
                                           'mixed_randrw:write:latency', depth,
                                           test_size)]
    results.extend(result)
  return results


def RunSimulatedStreaming(vm):
  """Spawn fio to simulate streaming and gather the results.

  Args:
    vm: The vm that synthetic_storage_workloads_benchmark will be run upon.
  Returns:
    A list of sample.Sample objects
  """
  test_size = min(vm.total_memory_kb / 10, 1000000)
  iodepth_list = FLAGS.iodepth_list or DEFAULT_STREAMING_SIMULATION_IODEPTH_LIST
  results = []
  for depth in iodepth_list:
    cmd = (
        '%s '
        '--filesize=10g '
        '--directory=%s '
        '--ioengine=libaio '
        '--overwrite=0 '
        '--invalidate=1 '
        '--direct=1 '
        '--randrepeat=0 '
        '--iodepth=%s '
        '--blocksize=1m '
        '--size=%dk '
        '--filename=fio_test_file ') % (fio.FIO_PATH,
                                        vm.GetScratchDir(),
                                        depth,
                                        test_size)
    if FLAGS.maxjobs:
      cmd += '--max-jobs=%s ' % FLAGS.maxjobs
    cmd += (
        '--name=sequential_write '
        '--rw=write '
        '--end_fsync=1 '
        '--name=sequential_read '
        '--stonewall '
        '--rw=read ')
    logging.info('FIO Results for simulated %s', STREAMING)
    res, _ = vm.RemoteCommand(cmd, should_log=True)
    latency, bandwidth = ParseFioResult(res)
    result = [
        CreateSampleFromBandwidthTuple(bandwidth[0],
                                       'sequential_write:bandwidth', depth,
                                       test_size),
        CreateSampleFromBandwidthTuple(bandwidth[1],
                                       'sequential_read:bandwidth', depth,
                                       test_size),
        CreateSampleFromLatencyTuple(latency[0],
                                     'sequential_write:latency', depth,
                                     test_size),
        CreateSampleFromLatencyTuple(latency[1],
                                     'sequential_read:latency', depth,
                                     test_size)]
    results.extend(result)
  return results


RUN_SCENARIO_FUNCTION_DICT = {
    LOGGING: {DESCRIPTION: 'simulated_logging', METHOD: RunSimulatedLogging},
    DATABASE: {DESCRIPTION: 'simulated_database', METHOD: RunSimulatedDatabase},
    STREAMING: {DESCRIPTION: 'simulated_streaming',
                METHOD: RunSimulatedStreaming}}


def Run(benchmark_spec):
  """Spawn fio and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  logging.info('Simulating %s scenario.', FLAGS.workload_mode)
  vms = benchmark_spec.vms
  vm = vms[0]
  # Add mode name into benchmark name, so perfkitbenchmarker will publish each
  # mode as a different benchmark, instead of mixing them together.
  BENCHMARK_INFO['name'] = '%s:%s' % (BENCHMARK_INFO['name'].split(':')[0],
                                      FLAGS.workload_mode)
  return RUN_SCENARIO_FUNCTION_DICT[FLAGS.workload_mode][METHOD](vm)


def Cleanup(benchmark_spec):
  """Uninstall packages required for fio and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('FIO Cleanup up on %s', vm)
  vm.RemoveFile(vm.GetScratchDir() + '/fio_test_file')
