# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Executes a series of DML queries using Apache Spark SQL and records latencies."""

import logging
import os
from typing import Any

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'dpb_sparksql_dml_benchmark'
BENCHMARK_CONFIG = """
dpb_sparksql_dml_benchmark:
  description: Run DML Spark SQL on a DPB cluster like Dataproc or EMR.
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
        AWS:
          disk_size: 1000
          disk_type: gp2
    worker_count: 2
"""

# Query templates, assuming TPC-DS schema for store_sales
CREATE_TABLE_TEMPLATE = """
CREATE TABLE spark_sql_dml_table
USING DELTA
LOCATION '{delta_table_path}'
AS SELECT * FROM parquet.`{source_table_path}`
"""

UPDATE_TEMPLATE = """
UPDATE spark_sql_dml_table
SET ss_list_price = 3.14159
WHERE MOD(ss_ticket_number, {proportion}) = 1
"""

DELETE_TEMPLATE = """
DELETE FROM spark_sql_dml_table
WHERE MOD(ss_item_sk, {proportion}) = 1
"""

_DPB_DML_TABLE_PATH = flags.DEFINE_string(
    'dpb_dml_table_path',
    None,
    'The Hadoop File System URL to the parquet files of the table that the '
    'UPDATE and DELETE statements will be benchmarked against.',
)

_DPB_DML_PROPORTION = flags.DEFINE_integer(
    'dpb_dml_proportion',
    100,
    'The proportion of rows for the DML benchmark to modify, as a fractional'
    ' denominator (i.e. "100" means 1 in 100, etc.).',
)


def GetConfig(user_config: dict[Any, Any]) -> dict[Any, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present."""
  del benchmark_config
  if _DPB_DML_TABLE_PATH.value is None:
    raise errors.Config.InvalidValue(
        'Missing required flag: dpb_dml_table_path'
    )


def _RunQuery(
    cluster: dpb_service.BaseDpbService, query_path: str
) -> dpb_service.JobResult:
  """Runs a single SparkSQL query."""
  return cluster.SubmitJob(
      query_file=query_path,
      job_type=dpb_constants.SPARKSQL_JOB_TYPE,
  )


def _StageQuery(
    cluster, query_template, query_name, context: dict[str, Any]
) -> str:
  """Renders, stages, and uploads a query to the cluster's bucket."""
  query_content = query_template.format(**context).strip()
  temp_run_dir = temp_dir.GetRunDirPath()
  local_query_file = os.path.join(temp_run_dir, query_name)
  with open(local_query_file, 'w') as f:
    f.write(query_content)
  staged_query_path = os.path.join(cluster.base_dir, query_name)
  cluster.storage_service.Copy(local_query_file, staged_query_path)
  return staged_query_path


def Prepare(benchmark_spec):
  """Prepares the benchmark by creating the Delta Lake table and staging DML queries."""
  cluster = benchmark_spec.dpb_service

  # Create Delta Lake table from source Parquet data
  source_table_path = _DPB_DML_TABLE_PATH.value
  delta_table_path = os.path.join(cluster.base_dir, 'delta_table_data')

  logging.info(
      'Creating Delta Lake table from %s at %s',
      source_table_path,
      delta_table_path,
  )
  create_table_query_path = _StageQuery(
      cluster=cluster,
      query_template=CREATE_TABLE_TEMPLATE,
      query_name='create_table.sql',
      context={
          'source_table_path': source_table_path,
          'delta_table_path': delta_table_path,
      },
  )
  _RunQuery(cluster, create_table_query_path)

  # Stage DML queries
  benchmark_spec.staged_update_query_path = _StageQuery(
      cluster=cluster,
      query_template=UPDATE_TEMPLATE,
      query_name='update_query.sql',
      context={'proportion': _DPB_DML_PROPORTION.value},
  )
  benchmark_spec.staged_delete_query_path = _StageQuery(
      cluster=cluster,
      query_template=DELETE_TEMPLATE,
      query_name='delete_query.sql',
      context={'proportion': _DPB_DML_PROPORTION.value},
  )


def _GetSampleMetadata(benchmark_spec):
  """Gets metadata dict to be attached to exported benchmark samples/metrics."""
  metadata = benchmark_spec.dpb_service.GetResourceMetadata()
  metadata['benchmark'] = BENCHMARK_NAME
  metadata['dml_proportion'] = _DPB_DML_PROPORTION.value
  metadata['dml_table_path'] = _DPB_DML_TABLE_PATH.value
  return metadata


def Run(benchmark_spec):
  """Runs the DML benchmark."""
  cluster = benchmark_spec.dpb_service

  results = []
  metadata = _GetSampleMetadata(benchmark_spec)

  for operation, query_path in [
      ('update', benchmark_spec.staged_update_query_path),
      ('delete', benchmark_spec.staged_delete_query_path),
  ]:
    logging.info('Running DML operation: %s', operation)
    job_result = _RunQuery(cluster, query_path)

    metadata_copy = metadata.copy()
    metadata_copy['operation'] = operation
    results.append(
        sample.Sample(
            f'dpb_dml_{operation}_run_time',
            job_result.wall_time,
            'seconds',
            metadata_copy,
        )
    )

  return results


def Cleanup(benchmark_spec):
  del benchmark_spec
