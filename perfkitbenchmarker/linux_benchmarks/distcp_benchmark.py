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

"""Perform distributed copy of data on data processing backends.
Apache Hadoop MapReduce distcp is an open-source tool used to copy large 
amounts of data. DistCp is very efficient because it uses MapReduce to copy the 
files or datasets and this means the copy operation is distributed across 
multiple nodes in a cluster. 
Benchmark to compare the performance of of the same distcp workload on clusters 
of various cloud providers.
"""

import copy
import datetime
import os
import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService
from perfkitbenchmarker.providers.aws import aws_dpb_emr
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow

BENCHMARK_NAME = 'distcp_benchmark'

BENCHMARK_CONFIG = """
distcp_benchmark:
  description: Run distcp on dataproc and emr
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
        AWS:
          machine_type: m3.medium
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
"""

flags.DEFINE_enum('distcp_source_fs', BaseDpbService.GCS_FS,
                  [BaseDpbService.GCS_FS, BaseDpbService.S3_FS,
                   BaseDpbService.HDFS_FS],
                  'File System to use as the source of the distcp operation')

flags.DEFINE_enum('distcp_dest_fs', BaseDpbService.GCS_FS,
                  [BaseDpbService.GCS_FS, BaseDpbService.S3_FS,
                   BaseDpbService.HDFS_FS],
                  'File System to use as destination of the distcp operation')

flags.DEFINE_integer('individual _file_size_in_megabytes', 10,
                     'File size to use for the distcp source files')

flags.DEFINE_integer('num_files', 10, 'Number of distcp source files')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid 
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if(dpb_service_type != dpb_service.DATAPROC or
     dpb_service_type != dpb_service.DATAPROC):
    raise errors.Config.InvalidValue('Invalid backend for distcp benchmark.')


def Prepare(benchmark_spec):
  run_uri = benchmark_spec.uuid.split('-')[0]
  source_dir, update_default_fs = dynamic_configuration(FLAGS.distcp_source_fs,
                                                        run_uri)
  # TODO(saksena): Respond to data generation failure
  benchmark_spec.dpb_service.generate_data(source_dir, update_default_fs,
                                           FLAGS.num_files,
                                           10)


def Run(benchmark_spec):
  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service
  run_uri = benchmark_spec.uuid.split('-')[0]

  source_dir, _ = dynamic_configuration(FLAGS.distcp_source_fs,
                                                        run_uri)
  destination_dir, _ = dynamic_configuration(FLAGS.distcp_dest_fs, run_uri,
                                             suffix='_destination')

  results = []
  metadata = copy.copy(dpb_service_instance.GetMetadata())
  metadata.update({'source_location': source_dir})
  metadata.update({'destination_location': destination_dir})

  start = datetime.datetime.now()
  dpb_service_instance.distributed_copy(source_location=source_dir,
                                        destination_location=destination_dir)
  end_time = datetime.datetime.now()

  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  dpb_service_instance = benchmark_spec.dpb_service
  run_uri = benchmark_spec.uuid.split('-')[0]

  if FLAGS.distcp_source_fs == BaseDpbService.HDFS_FS:
    source_dir = '/{}/dfsio'.format(run_uri)
  else:
    source_dir = '{}://{}/dfsio'.format(FLAGS.distcp_source_fs, run_uri)

  destination_dir = '{}_destination'.format(source_dir)
  dpb_service_instance.cleanup_data(source_dir, destination_dir)


def dynamic_configuration(fs, run_uri, suffix=''):
  if fs == BaseDpbService.HDFS_FS:
    return '/{}/dfsio{}'.format(run_uri, suffix), False
  else:
    return '{}://{}/dfsio{}'.format(fs, run_uri, suffix), True


