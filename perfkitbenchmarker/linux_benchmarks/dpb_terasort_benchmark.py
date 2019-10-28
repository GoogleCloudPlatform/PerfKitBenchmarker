# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Executes the 3 phases of Teasort phases on a Apache Hadoop MapReduce cluster.

TeraSort is a popular benchmark that measures the amount of time to sort a
configured amount of randomly distributed data on a given cluster. It is
commonly used to measure MapReduce performance of an Apache Hadoop cluster.
The following report compares performance of a YARN-scheduled TeraSort job on

A full TeraSort benchmark run consists of the following three steps:

* Generating the input data via TeraGen.
* Running the actual TeraSort on the input data.
* Validating the sorted output data via TeraValidate.

The benchmark reports the detailed latency of executing each phase.
"""
from __future__ import division

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'dpb_terasort_benchmark'

BENCHMARK_CONFIG = """
dpb_terasort_benchmark:
  description: Run terasort on dataproc and emr
  dpb_service:
    service_type: emr
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
        AWS:
          machine_type: m5.xlarge
      disk_spec:
        GCP:
          disk_size: 200
          disk_type: pd-standard
        AWS:
          disk_size: 500
          disk_type: st1
    worker_count: 2
"""

_FS_TYPE_EPHEMERAL = 'ephemeral'
_FS_TYPE_PERSISTENT = 'persistent'

flags.DEFINE_enum('dpb_terasort_storage_type', _FS_TYPE_PERSISTENT,
                  [_FS_TYPE_EPHEMERAL, _FS_TYPE_PERSISTENT],
                  'The type of storage for executing the Terasort benchmark')
flags.DEFINE_integer('dpb_terasort_num_records', 10000,
                     'Number of 100-byte rows to generate.')
flags.DEFINE_bool(
    'dpb_terasort_pre_cleanup', False,
    'Cleanup the terasort directories on the specified filesystem.')

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [dpb_service.DATAPROC, dpb_service.EMR]
JOB_CATEGORY = BaseDpbService.HADOOP_JOB_TYPE
JOB_TYPE = 'terasort'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Config needed to run the terasort benchmark

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend {} for terasort. Not in:{}'.format(
            dpb_service_type, str(SUPPORTED_DPB_BACKENDS)))


def Prepare(benchmark_spec):
  del benchmark_spec  # Unused.


def Run(benchmark_spec):
  """Runs the 3 phases of the terasort benchmark.

  The following phases are executed based on the selected Job Type:
    * Generating the input data via TeraGen.
    * Running the actual TeraSort on the input data.
    * Validating the sorted output data via TeraValidate.

  The samples report the cumulative results along with the results for the
  individual phases.

  Args:
    benchmark_spec: Spec needed to run the terasort benchmark

  Returns:
    A list of samples, comprised of the detailed run times of individual phases.
    The samples have associated metadata detailing the cluster details and used
    filesystem.
  """
  dpb_service_instance = benchmark_spec.dpb_service
  terasort_jar = dpb_service_instance.GetExecutionJar(JOB_CATEGORY, JOB_TYPE)

  if FLAGS.dpb_terasort_storage_type == _FS_TYPE_PERSISTENT:
    run_uri = benchmark_spec.uuid.split('-')[0]
    dpb_service_instance.CreateBucket(run_uri)
    base_dir = dpb_service_instance.PERSISTENT_FS_PREFIX + run_uri + '/'
  else:
    base_dir = '/'

  metadata = {}
  metadata.update(benchmark_spec.dpb_service.GetMetadata())
  metadata.update({'base_dir': base_dir})
  metadata.update({
      'dpb_terasort_storage_type': FLAGS.dpb_terasort_storage_type})
  metadata.update({'terasort_num_record': FLAGS.dpb_terasort_num_records})
  storage_in_gb = (FLAGS.dpb_terasort_num_records * 100) // (1000 * 1000 * 1000)
  metadata.update({'terasort_dataset_size_in_GB': storage_in_gb})
  logging.info('metadata %s ', str(metadata))

  logging.info('Resource create_start_time %s ',
               str(dpb_service_instance.create_start_time))
  logging.info('Resource resource_ready_time %s ',
               str(dpb_service_instance.resource_ready_time))
  create_time = (
      dpb_service_instance.resource_ready_time -
      dpb_service_instance.create_start_time)
  logging.info('create_time %s ', str(create_time))

  results = []
  results.append(
      sample.Sample('dpb_cluster_create_time', create_time, 'seconds',
                    metadata))
  stages = [(dpb_service.TERAGEN, 'GenerateDataForTerasort'),
            (dpb_service.TERASORT, 'SortDataForTerasort'),
            (dpb_service.TERAVALIDATE, 'ValidateDataForTerasort')]
  cumulative_runtime = 0
  for (phase, phase_execution_method) in stages:
    func = getattr(dpb_service_instance, phase_execution_method)
    wall_time, phase_stats = func(base_dir, terasort_jar, JOB_CATEGORY)
    logging.info(phase_stats)
    results.append(sample.Sample(phase + '_wall_time', wall_time, 'seconds',
                                 metadata))
    results.append(sample.Sample(phase + '_run_time',
                                 phase_stats['running_time'],
                                 'seconds', metadata))
    cumulative_runtime += phase_stats['running_time']
  results.append(sample.Sample('cumulative_runtime', cumulative_runtime,
                               'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  """Cleans up the terasort benchmark.

  Args:
    benchmark_spec: Spec needed to run the terasort benchmark
  """
  del benchmark_spec  # Unused.
