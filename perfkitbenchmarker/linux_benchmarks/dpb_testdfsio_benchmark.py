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
import logging
import time
from typing import List
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import hadoop

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
        AWS:
          machine_type: m4.xlarge
      disk_spec:
        GCP:
          disk_size: 1500
          disk_type: pd-standard
          mount_point: /scratch
        AWS:
          disk_size: 1500
          disk_type: gp2
          mount_point: /scratch
    worker_count: 2
"""

flags.DEFINE_enum(
    'dfsio_fs',
    dpb_constants.GCS_FS,
    [dpb_constants.GCS_FS, dpb_constants.S3_FS, dpb_constants.HDFS_FS],
    'File System to use in the dfsio operations',
)

flags.DEFINE_list(
    'dfsio_file_sizes_list',
    [1],
    'A list of file sizes to use for each of the dfsio files.',
)

flags.DEFINE_list(
    'dfsio_num_files_list',
    [4],
    'A list of number of dfsio files to use during individual runs.',
)

flags.DEFINE_integer(
    'dfsio_delay_read_sec',
    0,
    'Time to wait in seconds before reading the files.',
)

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [
    dpb_constants.DATAPROC,
    dpb_constants.EMR,
    # add unmanged hadoop yarn cluster support for dfsio
    dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
]

# TestDSIO commands
WRITE = 'write'
READ = 'read'
CLEAN = 'clean'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: The config used to construct the BenchmarkSpec.

  Raises:
    InvalidValue: On encountering invalid configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend for dfsio. Not in:{}'.format(
            str(SUPPORTED_DPB_BACKENDS)
        )
    )


def Prepare(benchmark_spec):
  del benchmark_spec  # unused


def ParseResults(command: str, stdout: str) -> List[sample.Sample]:
  regex = r'Throughput mb/sec: (\d+.\d+)'
  throughput = regex_util.ExtractFloat(regex, stdout)
  return [sample.Sample(f'{command}_throughput', throughput, 'MB/s')]


def Run(benchmark_spec):
  """Runs testdfsio benchmark and reports the results.

  Args:
    benchmark_spec: Spec needed to run the testdfsio benchmark

  Returns:
    A list of samples
  """
  service = benchmark_spec.dpb_service

  if FLAGS.dfsio_fs == dpb_constants.HDFS_FS:
    base_dir = 'hdfs:/dfsio'
  elif service.base_dir.startswith(FLAGS.dfsio_fs):
    base_dir = service.base_dir + '/dfsio'
  else:
    raise errors.Config.InvalidValue(
        'Service type {} cannot use dfsio_fs: {}'.format(
            service.type, FLAGS.dfsio_fs
        )
    )

  results = []
  for file_size in FLAGS.dfsio_file_sizes_list:
    for num_files in FLAGS.dfsio_num_files_list:
      metadata = copy.copy(service.GetResourceMetadata())
      metadata.update({'dfsio_fs': FLAGS.dfsio_fs})
      metadata.update({'dfsio_num_files': num_files})
      metadata.update({'dfsio_file_size_mbs': file_size})
      if FLAGS.zone:
        zone = FLAGS.zone[0]
        region = zone.rsplit('-', 1)[0]
        metadata.update({'regional': True})
        metadata.update({'region': region})
      elif FLAGS.cloud == 'AWS':
        metadata.update({'regional': True})
        metadata.update({'region': 'aws_default'})
      metadata.update(hadoop.GetHadoopData())

      service.metadata.update(metadata)

      # This order is important. Write generates the data for read and clean
      # deletes it for the next write.
      for command in (WRITE, READ, CLEAN):
        result = RunTestDfsio(service, command, base_dir, num_files, file_size)
        results.append(
            sample.Sample(
                command + '_run_time', result.run_time, 'seconds', metadata
            )
        )
        if command in (WRITE, READ):
          results += ParseResults(command, result.stderr)
  return results


def RunTestDfsio(service, command, data_dir, num_files, file_size):
  """Run the given TestDFSIO command."""
  args = [
      '-' + command,
      '-nrFiles',
      str(num_files),
      '-fileSize',
      str(file_size),
  ]
  properties = {'test.build.data': data_dir}
  if not (
      data_dir.startswith(dpb_constants.HDFS_FS + ':')
      or data_dir.startswith('/')
  ):
    properties['fs.default.name'] = data_dir
  if command == READ and FLAGS.dfsio_delay_read_sec:
    logging.info(
        'Sleeping for %s seconds before reading', FLAGS.dfsio_delay_read_sec
    )
    time.sleep(FLAGS.dfsio_delay_read_sec)
  return service.SubmitJob(
      classname='org.apache.hadoop.fs.TestDFSIO',
      properties=properties,
      job_arguments=args,
      job_type=dpb_constants.HADOOP_JOB_TYPE,
  )


def Cleanup(benchmark_spec):
  del benchmark_spec  # unused
