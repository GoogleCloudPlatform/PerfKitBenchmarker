# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Executes adhoc queries using Apache Spark SQL and records latencies.

This benchmarks helps in running the adhoc queries unlike the other spark
benchmarks which executes against a standard set of queries picked from
https://github.com/databricks/spark-sql-perf.git

Queries:
This benchmark uses custom query which can be provided via storage/bucket
to benchmark using `--dpb_spark_query_uri_path`.

Data:
For now benchmark is only supporting write queries hence dataset is not
required.
But later on it can be used to run even read queries and support for loading the
dataset can be added.
"""

import json
import logging
import os
import time
from typing import List, Dict

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir

BENCHMARK_NAME = 'dpb_sparksql_custom_benchmark'

BENCHMARK_CONFIG = """
dpb_sparksql_custom_benchmark:
  description: Run Spark SQL on dataproc and emr
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
        AWS:
          machine_type: m5.xlarge
      disk_spec:
        GCP:
          disk_size: 1000
          disk_type: pd-standard
          # Only used by unmanaged
          mount_point: /scratch
        AWS:
          disk_size: 1000
          disk_type: gp2
          # Only used by unmanaged
          mount_point: /scratch
        Azure:
          disk_size: 1000
          disk_type: Standard_LRS
          # Only used by unmanaged
          mount_point: /scratch
    worker_count: 2
"""

_QUERY_URI_PATH = flags.DEFINE_string(
    'dpb_spark_query_uri_path', None,
    'Uri path of query stored in storage/bucket i.e. gcs|s3|blob_storage.')

_QUERY_SUBSTITUTION_JSON_STRING = flags.DEFINE_string(
    'dpb_spark_query_param_json_string', '{}',
    'Values of the names variable used in query provided via '
    " `dpb_spark_query_uri_path`. It's a comma separated json string where"
    "each value looks like 'key' : 'value'"
    "Imp: 'key' are stripped for spaces and then used, 'value' is used as is."
    'Cluster parameters like $BASE_DIR, $REGION, $CLUSTER_ID can also be used'
    'as variables in sql query.')
FLAGS = flags.FLAGS

SCRIPT_DIR = 'spark_sql_test_scripts'
SPARK_SQL_RUNNER_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_sql_runner.py')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Copies scripts and all the queries to cloud.

  Args:
    benchmark_spec: The benchmark specification
  """
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  benchmark_spec.staged_queries = _LoadAndStageQueries(cluster)

  scripts_to_upload = [
      SPARK_SQL_RUNNER_SCRIPT,
  ] + cluster.GetServiceWrapperScriptsToUpload()
  for script in scripts_to_upload:
    src_url = data.ResourcePath(script)
    storage_service.CopyToBucket(src_url, cluster.bucket, script)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs a Spark SQL Query.

  Args:
    benchmark_spec: Spec needed to run the Spark SQL.

  Returns:
    A list of samples, comprised of the detailed run times of query.

  Raises:
    Benchmarks.RunError if no query succeeds.
  """
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  metadata = benchmark_spec.dpb_service.GetMetadata()

  # Run PySpark Spark SQL Runner
  report_dir = '/'.join([cluster.base_dir, f'report-{int(time.time()*1000)}'])
  args = [
      '--sql-scripts',
      ','.join(benchmark_spec.staged_queries),
      '--report-dir',
      report_dir,
  ]

  job_result = cluster.SubmitJob(
      pyspark_file='/'.join([cluster.base_dir, SPARK_SQL_RUNNER_SCRIPT]),
      job_arguments=args,
      job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE)

  # Spark can only write data to directories not files. So do a recursive copy
  # of that directory and then search it for the single JSON file with the
  # results.
  temp_run_dir = temp_dir.GetRunDirPath()
  storage_service.Copy(report_dir, temp_run_dir, recursive=True)
  report_file = None
  for dir_name, _, files in os.walk(
      os.path.join(temp_run_dir, os.path.basename(report_dir))):
    for filename in files:
      if filename.endswith('.json'):
        report_file = os.path.join(dir_name, filename)
        logging.info(report_file)
  if not report_file:
    raise errors.Benchmarks.RunError('Job report not found.')

  results = []
  with open(report_file, 'r') as file:
    result = json.loads(file.read())
    results.append(
        sample.Sample('sparksql_run_time', result['duration'], 'seconds',
                      metadata))
  results.append(
      sample.Sample('sparksql_total_wall_time', job_result.wall_time, 'seconds',
                    metadata))
  cluster_create_time = cluster.GetClusterCreateTime()
  if cluster_create_time is not None:
    results.append(
        sample.Sample('dpb_cluster_create_time', cluster_create_time, 'seconds',
                      metadata))
  results.append(
      sample.Sample('dpb_sparksql_job_pending', job_result.pending_time,
                    'seconds', metadata))
  return results


def _DownloadTemplatedQuery(
    storage_service: object_storage_service.ObjectStorageService) -> str:
  """Downloads the template query to a temporary location.

  Args:
    storage_service: object_storage_service to download parameterised query.

  Returns:
    path of temporary location where file is downloaded.
  """
  temp_run_dir = temp_dir.GetRunDirPath()
  template_query_path = os.path.join(temp_run_dir, 'query.tpl.sql')
  storage_service.Copy(_QUERY_URI_PATH.value, template_query_path)
  return template_query_path


def _GetQuerySubstitutions(
    cluster: dpb_service.BaseDpbService) -> Dict[str, str]:
  """Creates a sql query substitution dictionary.

  Substitutions is composed of two set of variables
  1. list of custom query variables paased in via
  'dpb_spark_query_param_json_string'
  2. list of cluster specific parameters i.e. cluster_id, region etc.

  Args:
    cluster: BaseDpbService object.

  Returns:
    Dictionary of substituion of variables.
  """
  substitute_dict = json.loads(_QUERY_SUBSTITUTION_JSON_STRING.value)
  substitute_dict.update({
      '$BASE_DIR': cluster.base_dir,
      '$REGION': cluster.region,
      '$CLUSTER_ID': cluster.cluster_id
  })
  return substitute_dict


def _GetSubstitutedSqlQuery(cluster: dpb_service.BaseDpbService):
  """Downloads the query and convert it to string.

  Args:
    cluster: BaseDpbService object.

  Returns:
    Sql query with all substitutions in place.
  """
  storage_service = cluster.storage_service
  query_string = ''
  template_query_path = _DownloadTemplatedQuery(storage_service)
  substitute_dict = _GetQuerySubstitutions(cluster)
  with open(template_query_path, 'r') as query_read:
    for line in query_read:
      for subs in substitute_dict:
        line = line.replace(subs, substitute_dict[subs])
      query_string += line
  return query_string


def _LoadAndStageQueries(cluster: dpb_service.BaseDpbService) -> List[str]:
  """Loads query from gcs and stages them in object storage.

  Query is picked from location `--dpb_spark_query_uri_path`.
  Variables or parameter in query are substituted using
  `--dpb_spark_query_param_json_string`.

  Args:
    cluster: BaseDpbService object.

  Returns:
    The paths to the stage queries.

  Raises:
    PrepareException if a requested query is not found.
  """
  storage_service = cluster.storage_service
  base_dir = cluster.base_dir
  temp_run_dir = temp_dir.GetRunDirPath()
  local_script_file_name = 'query.sql'
  local_script_file = os.path.join(temp_run_dir, local_script_file_name)
  with open(local_script_file, 'w') as f:
    query_string = _GetSubstitutedSqlQuery(cluster)
    f.write(query_string)
  query_file = local_script_file_name
  staged_file = '/'.join([base_dir, query_file])
  storage_service.Copy(local_script_file, staged_file)
  return [staged_file]


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up the Benchmark."""
  del benchmark_spec  # unused
