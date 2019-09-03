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
"""Runs a IO intensive Spark Application on a managed Yarn cluster.

The benchmark runs a Spark SQL application that queries data persisted in object
store. A Hive compatible schema is provisioned from scratch, i.e. DDL and DML
scripts create and load data and then queried for performance measurement.

The wall time may be inflated due to the use of polling to ascertain job
completion.
"""
import copy
import logging
import os

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'dpb_spark_io_benchmark'
BENCHMARK_CONFIG = """
dpb_spark_io_benchmark:
  flags:
    cloud: GCP
    dpb_service_zone: us-east1-b
  description: >
      Create a dpb cluster and Run a Spark IO application.
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
      disk_spec:
        GCP:
          disk_type: pd-standard
          disk_size: 500
    worker_count: 2
"""

FLAGS = flags.FLAGS

SPARK_SAMPLE_SCRIPT = 'query.sql'
RESOURCE_LIFECYCLE_ARTIFACTS = {
    'dml_script': {
        'artifact': 'dml_script.py'
    },
    'data': {
        'artifact': 'data.snappy.parquet',
        'prefix': '/data/'
    },
    'query_script': {
        'artifact': SPARK_SAMPLE_SCRIPT
    }
}


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def ManageLifecycleResources(base_folder, dpb_service_instance,
                             storage_service):
  """Takes the static artifacts and persists them in object store for execution.


  Args:
    base_folder: Base folder for the current benchmark run.
    dpb_service_instance: An instance of the dpb service being benchmarked.
    storage_service: The object store to use for persistence of the artifacts.

  Returns:
    A dictionary of lifecycle to resource uris.
  """
  resource_uri_dictionary = {}
  for lifecycle_step, artifact_details in RESOURCE_LIFECYCLE_ARTIFACTS.items():
    lifecycle_bucket_name = '{}_{}'.format(base_folder, lifecycle_step)
    dpb_service_instance.CreateBucket(lifecycle_bucket_name)

    lifecycle_folder_uri = '{}{}'.format(
        dpb_service_instance.PERSISTENT_FS_PREFIX, lifecycle_bucket_name)
    if 'prefix' in artifact_details.keys():
      lifecycle_folder_uri = '{}{}'.format(lifecycle_folder_uri,
                                           artifact_details['prefix'])

    static_artifact_url = data.ResourcePath(
        os.path.join('spark_io', artifact_details['artifact']))
    storage_service.Copy(static_artifact_url, lifecycle_folder_uri)

    if 'prefix' in artifact_details.keys():
      lifecycle_artifact_uri = lifecycle_folder_uri[0:len(
          lifecycle_folder_uri) - 1]
    else:
      lifecycle_artifact_uri = '{}/{}'.format(lifecycle_folder_uri,
                                              artifact_details['artifact'])
    resource_uri_dictionary[lifecycle_step] = lifecycle_artifact_uri
  return resource_uri_dictionary


def Prepare(benchmark_spec):
  """Prepare phase uses schema creation script and sample data to prepare table.

  Args:
    benchmark_spec: Configuration that holds the definition and instance details
      of the resources used for benchmarking.
  """
  storage_service = object_storage_service.GetObjectStorageClass(FLAGS.cloud)()
  dpb_service_instance = benchmark_spec.dpb_service
  run_uri = benchmark_spec.uuid.split('-')[0]
  uri_map = ManageLifecycleResources(run_uri, dpb_service_instance,
                                     storage_service)
  dml_script_uri = uri_map['dml_script']
  data_folder_uri = uri_map['data']
  stats = dpb_service_instance.SubmitJob(
      None,
      None,
      pyspark_file=dml_script_uri,
      job_type=BaseDpbService.PYSPARK_JOB_TYPE,
      job_arguments=[data_folder_uri])
  logging.info(stats)
  if not stats['success']:
    logging.warning('Table Creation Failed')


def Run(benchmark_spec):
  """Executes the sql script on the specified Spark cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  dpb_service_instance = benchmark_spec.dpb_service
  run_uri = benchmark_spec.uuid.split('-')[0]
  metadata = copy.copy(dpb_service_instance.GetMetadata())
  query_script_folder = '{}_query_script'.format(run_uri)
  query_script_folder_uri = '{}{}'.format(
      dpb_service_instance.PERSISTENT_FS_PREFIX, query_script_folder)
  query_script_uri = '{}/{}'.format(query_script_folder_uri,
                                    SPARK_SAMPLE_SCRIPT)

  stats = dpb_service_instance.SubmitJob(
      None,
      None,
      query_file=query_script_uri,
      job_type=BaseDpbService.SPARKSQL_JOB_TYPE)
  logging.info(stats)

  if stats['success']:
    results.append(
        sample.Sample('run_time', stats['running_time'], 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  storage_service = object_storage_service.GetObjectStorageClass(FLAGS.cloud)()
  base_folder = benchmark_spec.uuid.split('-')[0]
  for lifecycle_step, _ in RESOURCE_LIFECYCLE_ARTIFACTS.items():
    dml_script_folder = '{}_{}'.format(base_folder, lifecycle_step)
    storage_service.DeleteBucket(dml_script_folder)
