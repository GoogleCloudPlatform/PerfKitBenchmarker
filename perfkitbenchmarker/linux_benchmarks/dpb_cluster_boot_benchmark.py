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
"""The benchmark reports the latency of creating a dpb cluster."""

import copy
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'dpb_cluster_boot_benchmark'

BENCHMARK_CONFIG = """
dpb_cluster_boot_benchmark:
  description: Run dpb cluster boot on dataproc and emr
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 50
        AWS:
          machine_type: m5.large
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 1500
          disk_type: gp2
    worker_count: 2
"""

flags.DEFINE_enum('dpb_cluster_boot_fs_type', 'ephemeral',
                  ['ephemeral', 'persistent'],
                  'File System to use in dpb cluster boot benchmark')

flags.DEFINE_enum(
    'dpb_cluster_boot_fs', BaseDpbService.GCS_FS,
    [BaseDpbService.GCS_FS, BaseDpbService.S3_FS, BaseDpbService.HDFS_FS],
    'File System to use in the dpb cluster boot benchmark')

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [dpb_service.DATAPROC, dpb_service.EMR]


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Config needed to run the dpb cluster boot benchmark.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend for dpb cluster boot . Not in:{}'.format(
            str(SUPPORTED_DPB_BACKENDS)))


def Prepare(benchmark_spec):
  del benchmark_spec  # Unused.


def Run(benchmark_spec):
  """Runs the dpb cluster boot benchmark.

  The benchmark computes and publishes the time taken from the issuance of
  cluster creation command till the cluster is ready to accept jobs for
  processing.

  Args:
    benchmark_spec: Spec needed to run the dpb cluster boot benchmark

  Returns:
    A list of samples, comprised of the dpb cluster boot latency in seconds.
  """

  results = []  # list of the samples that will be returned
  dpb_service_instance = benchmark_spec.dpb_service

  metadata = copy.copy(benchmark_spec.dpb_service.GetMetadata())

  logging.info('metadata %s ', str(metadata))
  logging.info('Resource create_start_time %s ',
               str(dpb_service_instance.create_start_time))
  logging.info('Resource resource_ready_time %s ',
               str(dpb_service_instance.resource_ready_time))

  create_time = (
      dpb_service_instance.resource_ready_time -
      dpb_service_instance.create_start_time)
  logging.info('create_time %s ', str(create_time))

  results.append(
      sample.Sample('dpb_cluster_create_time', create_time, 'seconds',
                    metadata))
  return results


def Cleanup(benchmark_spec):
  """Cleans up the dpb cluster boot benchmark.

  Args:
    benchmark_spec: Spec needed to run the dpb cluster boot benchmark
  """
  del benchmark_spec  # Unused.
