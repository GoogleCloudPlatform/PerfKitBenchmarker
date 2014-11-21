#!/usr/bin/env python
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
1) Write a 1GB file using 1K sequential writes.
2) Random read of the 1GB file using 1K IOs.
3) Sequential read of the 1GB file using 1K IOs.

Simulated database benchmark does the following things (PD, PD-SSD, local SSD):
1) 4K Random R on a 10GB file using queue depths 1, 16 and 64 (each queue depth
   is a different benchmark).
2) 4K Random W on a 10GB file using queue depths 1, 16 and 64 (each queue depth
   is a different benchmark).
3) 4K Random 90% R/ 10% W on a 10GB file using queue depths 1, 16 and 64 (each
   queue depth is a different benchmark).

Simulated streaming benchmark (PD only):
1) 1M Seq R at queue depth 1 and 16 (streaming).
2) 1M Seq W at queue depth 1 and 16 (streaming).

For AWS, where use PD, we should use EBS-GP and EBS Magnetic, for PD-SSD use
EBS-GP and PIOPS.
"""

import re

import gflags as flags
import logging

LOGGING = 'logging'
DATABASE = 'database'
STREAMING = 'streaming'

flags.DEFINE_enum('workload_mode', LOGGING,
                  [LOGGING, DATABASE, STREAMING],
                  'Simuate a senario in logging, database or streaming.')

flags.DEFINE_list('iodepth_list', None, 'A list of iodepth parameter used by '
                  'fio command in simulated database and streaming scenarios '
                  'only.')

flags.DEFINE_integer('maxjobs', 0,
                     'The maximum allowed number of jobs to support.')

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'synthetic_storage_workloads_benchmark',
                  'description': 'Runs FIO in sequenctial, random, read '
                                 'and write modes to simulate logging.',
                  'scratch_disk': True,
                  'num_machines': 1}
REQUIRED_PACKAGES = 'bc fio libaio1'
DESCRIPTION = 'description'
METHOD = 'method'

DEFAULT_DATABASE_SIMULATION_IODEPTH_LIST = [1, 16, 64]
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
  vm.InstallPackage(REQUIRED_PACKAGES)


def ParseFioResult(res):
  """Parse result from fio commands.

  Args:
    res: Result from fio commands in string format.
  Returns:
    A list of tuples represents latency. Each tuple contains unit and
    latency number.
    A list of tuples represents bandwidth. Each tuple contains aggrb,
    unit of aggrb, minb, unit of minb, maxb, unit of maxb, mint, unit of mint,
    maxt, unit of maxt.
  """
  latency = re.findall(r'\s+lat \((\w+)\)[\s:]+'
                       r'min%smax%savg%sstdev%s' % (
                           LATENCY_REGEX, LATENCY_REGEX,
                           LATENCY_REGEX, LATENCY_REGEX), res)
  bandwidth = re.findall(
      r'aggrb=%s, minb=%s, maxb=%s, mint=%s, maxt=%s' % (
          BANDWIDTH_REGEX, BANDWIDTH_REGEX, BANDWIDTH_REGEX, BANDWIDTH_REGEX,
          BANDWIDTH_REGEX), res)
  print latency, bandwidth
  return latency, bandwidth


def CreateSampleFromBandwidthTuple(result, test, iodepth=1):
  """Create a sample from bandwidth result tuple.

  Args:
    result: A tuple, containing (aggrb, unit of aggrb, minb, unit of minb,
        maxb, unit of maxb, mint, unit of mint, maxt, unit of maxt).
    test: Name of test.
    iodepth: Iodepth parameter used by fio command.
  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  return [test, float(result[0]), result[1],
          {'minb': result[2] + result[3],
           'maxb': result[4] + result[5],
           'mint': result[6] + result[7],
           'maxt': result[8] + result[9],
           'max_jobs': FLAGS.maxjobs,
           'iodepth': iodepth}]


def CreateSampleFromLatencyTuple(result, test, iodepth=1):
  """Create a sample from latency result tuple.

  Args:
    result: A tuple, containing (unit, min, max, avg, stdev).
    test: Name of test.
    iodepth: Iodepth parameter used by fio command.
  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  return [test, float(result[3]), result[0],
          {'min': result[1] + result[0],
           'max': result[2] + result[0],
           'stdev': result[4] + result[0],
           'max_jobs': FLAGS.maxjobs,
           'iodepth': iodepth}]


def RunSimulatedLogging(vm):
  """Spawn fio to simulate logging and gather the results.

  Args:
    vm: The vm that synthetic_storage_workloads_benchmark will be run upon.
  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  cmd = (
      'fio '
      '--filesize=100g '
      '--directory=%s '
      '--ioengine=libaio '
      '--filename=fio_test_file '
      '--invalidate=1 '
      '--randrepeat=0 '
      '--direct=1 '
      '--size=%dk ') % (vm.GetScratchDir(),
                        vm.total_memory_kb)
  if FLAGS.maxjobs:
    cmd += '--max-jobs=%s ' % FLAGS.maxjobs
  cmd += (
      '--name=sequential_write '
      '--overwrite=0 '
      '--rw=write '
      '--end_fsync=1 '
      '--name=random_read '
      '--stonewall '
      '--rw=randread '
      '--name=sequential_read '
      '--stonewall '
      '--rw=read ')
  logging.info('FIO Results for simulated %s', LOGGING)
  res, _ = vm.RemoteCommand(cmd, should_log=True)
  latency, bandwidth = ParseFioResult(res)
  results = [
      CreateSampleFromBandwidthTuple(bandwidth[0],
                                     'sequential_write:bandwidth'),
      CreateSampleFromBandwidthTuple(bandwidth[1], 'random_read:bandwidth'),
      CreateSampleFromBandwidthTuple(bandwidth[2], 'sequential_read:bandwidth'),
      CreateSampleFromLatencyTuple(latency[0], 'sequential_write:latency'),
      CreateSampleFromLatencyTuple(latency[1], 'random_read:latency'),
      CreateSampleFromLatencyTuple(latency[2], 'sequential_read:latency')]
  return results


def RunSimulatedDatabase(vm):
  """Spawn fio to simulate database and gather the results.

  Args:
    vm: The vm that synthetic_storage_workloads_benchmark will be run upon.
  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the same metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  iodepth_list = FLAGS.iodepth_list or DEFAULT_DATABASE_SIMULATION_IODEPTH_LIST
  results = []
  for depth in iodepth_list:
    cmd = (
        'fio '
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
                              vm.total_memory_kb,
                              depth)
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
                                             'random_write:bandwidth', depth),
              CreateSampleFromBandwidthTuple(bandwidth[1],
                                             'random_read:bandwidth', depth),
              CreateSampleFromBandwidthTuple(bandwidth[2],
                                             'mixed_randrw:read:bandwidth',
                                             depth),
              CreateSampleFromBandwidthTuple(bandwidth[3],
                                             'mixed_randrw:write:bandwidth',
                                             depth),
              CreateSampleFromLatencyTuple(latency[0], 'random_write:latency',
                                           depth),
              CreateSampleFromLatencyTuple(latency[1], 'random_read:latency',
                                           depth),
              CreateSampleFromLatencyTuple(latency[2],
                                           'mixed_randrw:read:latency', depth),
              CreateSampleFromLatencyTuple(latency[3],
                                           'mixed_randrw:write:latency', depth)]
    results.extend(result)
  return results


def RunSimulatedStreaming(vm):
  """Spawn fio to simulate streaming and gather the results.

  Args:
    vm: The vm that synthetic_storage_workloads_benchmark will be run upon.
  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the same metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  iodepth_list = FLAGS.iodepth_list or DEFAULT_STREAMING_SIMULATION_IODEPTH_LIST
  results = []
  for depth in iodepth_list:
    cmd = (
        'fio '
        '--filesize=100g '
        '--directory=%s '
        '--ioengine=libaio '
        '--overwrite=0 '
        '--invalidate=1 '
        '--direct=1 '
        '--randrepeat=0 '
        '--iodepth=%s '
        '--blocksize=1m '
        '--size=%dk '
        '--filename=fio_test_file ') % (vm.GetScratchDir(), depth,
                                        vm.total_memory_kb)
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
                                       'sequential_write:bandwidth', depth),
        CreateSampleFromBandwidthTuple(bandwidth[1],
                                       'sequential_read:bandwidth', depth),
        CreateSampleFromLatencyTuple(latency[0],
                                     'sequential_write:latency', depth),
        CreateSampleFromLatencyTuple(latency[1],
                                     'sequential_read:latency', depth)]
    results.extend(result)
  return results


RUN_SENARIO_FUNCTION_DICT = {
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
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  logging.info('Simulating %s senario.', FLAGS.workload_mode)
  vms = benchmark_spec.vms
  vm = vms[0]
  # Add mode name into benchmark name, so perfkitbenchmarker will publish each
  # mode as a different benchmark, instead of mixing them together.
  BENCHMARK_INFO['name'] = '%s:%s' % (BENCHMARK_INFO['name'].split(':')[0],
                                      FLAGS.workload_mode)
  return RUN_SENARIO_FUNCTION_DICT[FLAGS.workload_mode][METHOD](vm)


def Cleanup(benchmark_spec):
  """Uninstall packages required for fio and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('FIO Cleanup up on %s', vm)
  vm.UninstallPackage(REQUIRED_PACKAGES)
  vm.RemoveFile(vm.GetScratchDir() + '/fio_test_file')
