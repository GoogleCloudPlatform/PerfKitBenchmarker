# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Perform Distributed i/o benchmark on data processing backends.
This test writes into and then subsequently reads a specified number of
files. File size is also specified as a parameter to the test.
The benchmark implementation accepts list of arguments for both the above
parameters and generates one sample for each cross product of the two
parameter values. Each file is accessed in a separate map task.
"""

import copy
import datetime


from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'dpb_testdfsio_benchmark'

BENCHMARK_CONFIG = """
dpb_testdfsio_benchmark:
  description: Run testdfsio on dataproc and emr
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          boot_disk_size: 1500
        AWS:
          machine_type: m4.xlarge
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 1500
          disk_type: gp2
    worker_count: 8
"""

flags.DEFINE_enum('dfsio_fs', BaseDpbService.GCS_FS,
                  [BaseDpbService.GCS_FS, BaseDpbService.S3_FS,
                   BaseDpbService.HDFS_FS],
                  'File System to use in the dfsio operations')

flags.DEFINE_list(
    'dfsio_file_sizes_list', [1], 'A list of file sizes to use for each of the'
                                  ' dfsio files.')

flags.DEFINE_list(
    'dfsio_num_files_list', [10], 'A list of number of dfsio files to use'
                                  ' during individual runs.')


FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [dpb_service.DATAPROC, dpb_service.EMR]


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

    Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue('Invalid backend for dfsio. Not in:{}'.
                                     format(str(SUPPORTED_DPB_BACKENDS)))


def Prepare(benchmark_spec):
  del benchmark_spec  # Unused.


def Run(benchmark_spec):
  """Runs testdfsio benchmark and reports the results.

  Args:
    benchmark_spec: Spec needed to run the testdfsio benchmark

  Returns:
    A list of samples
  """
  run_uri = benchmark_spec.uuid.split('-')[0]
  source = '/{}'.format(run_uri)
  update_source_default_fs = False

  if FLAGS.dfsio_fs != BaseDpbService.HDFS_FS:
    source = '{}:/{}'.format(FLAGS.dfsio_fs, source)
    update_source_default_fs = True

  source_dir = '{}{}'.format(source, '/dfsio')

  results = []
  for file_size in FLAGS.dfsio_file_sizes_list:
    for num_files in FLAGS.dfsio_num_files_list:
      benchmark_spec.dpb_service.CreateBucket(source)
      # TODO(saksena): Respond to data generation failure
      start = datetime.datetime.now()
      benchmark_spec.dpb_service.generate_data(source_dir,
                                               update_source_default_fs,
                                               num_files, file_size)
      end_time = datetime.datetime.now()
      write_run_time = (end_time - start).total_seconds()

      start = datetime.datetime.now()
      benchmark_spec.dpb_service.read_data(source_dir,
                                           update_source_default_fs,
                                           num_files, file_size)
      end_time = datetime.datetime.now()
      read_run_time = (end_time - start).total_seconds()

      metadata = copy.copy(benchmark_spec.dpb_service.GetMetadata())
      metadata.update({'dfsio_fs': FLAGS.dfsio_fs})
      metadata.update({'dfsio_num_files': num_files})
      metadata.update({'dfsio_file_size_mbs': file_size})
      if FLAGS.zones:
        zone = FLAGS.zones[0]
        region = zone.rsplit('-', 1)[0]
        metadata.update({'regional': True})
        metadata.update({'region': region})
      elif FLAGS.cloud == 'AWS':
        metadata.update({'regional': True})
        metadata.update({'region': 'aws_default'})

      results.append(sample.Sample('write_run_time', write_run_time, 'seconds',
                                   metadata))
      results.append(sample.Sample('read_run_time', read_run_time, 'seconds',
                                   metadata))
      benchmark_spec.dpb_service.cleanup_data(source, update_source_default_fs)
  return results


def Cleanup(benchmark_spec):
  """Cleans up the testdfsio benchmark"""
  del benchmark_spec  # Unused.
