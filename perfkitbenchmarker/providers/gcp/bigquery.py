# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's Bigquery EDW service."""

import datetime
import json
import logging

from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util as gcp_util


FLAGS = flags.FLAGS

DEFAULT_TABLE_EXPIRATION = 3600 * 24 * 365  # seconds


class Bigquery(edw_service.EdwService):
  """Object representing a Bigquery cluster.

  Attributes:
    job_id_prefix: A string prefix for the job id for bigquery job.
  """

  CLOUD = providers.GCP
  SERVICE_TYPE = 'bigquery'

  def _Create(self):
    """Create a BigQuery cluster.

    Bigquery clusters creation is out of scope of the benchmarking.
    """
    raise NotImplementedError

  def _Exists(self):
    """Method to validate the existence of a Bigquery cluster.

    Returns:
      Boolean value indicating the existence of a cluster.
    """
    return True

  def _Delete(self):
    """Delete a BigQuery cluster.

    Bigquery cluster deletion is out of scope of benchmarking.
    """
    raise NotImplementedError

  def GetMetadata(self):
    """Return a dictionary of the metadata for the BigQuery cluster."""
    basic_data = super(Bigquery, self).GetMetadata()
    return basic_data

  def RunCommandHelper(self):
    """Bigquery specific run script command components."""
    bq = self.cluster_identifier.split('.')
    return '--bq_project_id={} --bq_dataset_id={}'.format(bq[0], bq[1])

  def FormatProjectAndDatasetForCommand(self, dataset=None):
    """Returns the project and dataset in the format needed for bq commands.

    E.g., project:dataset.

    Args:
      dataset: The dataset to run commands against. If None, extracts the
        dataset from the cluster identifier whose format is "project.dataset").
    """
    return ((self.cluster_identifier.split('.')[0] + ':' +
             dataset) if dataset else self.cluster_identifier.replace('.', ':'))

  def InstallAndAuthenticateRunner(self, vm):
    """Method to perform installation and authentication of bigquery runner.

    Native Bigquery client that ships with the google_cloud_sdk
    https://cloud.google.com/bigquery/docs/bq-command-line-tool used as client.

    Args:
      vm: Client vm on which the script will be run.
    """
    vm.Install('google_cloud_sdk')
    gcp_util.AuthenticateServiceAccount(vm)

  def GetDatasetLastUpdatedTime(self, dataset=None):
    """Get the formatted last modified timestamp of the dataset."""
    cmd = [
        'bq', 'show', '--format=prettyjson',
        self.FormatProjectAndDatasetForCommand(dataset)
    ]
    dataset_metadata, _, _ = vm_util.IssueCommand(cmd)
    metadata_json = json.loads(str(dataset_metadata))
    return datetime.datetime.fromtimestamp(
        float(metadata_json['lastModifiedTime']) /
        1000.0).strftime('%Y-%m-%d_%H-%M-%S')

  def GetAllTablesInDataset(self, dataset=None):
    """Returns a list of the IDs of all the tables in the dataset."""
    cmd = [
        'bq', 'ls', '--format=prettyjson',
        self.FormatProjectAndDatasetForCommand(dataset)
    ]
    tables_list, _, _ = vm_util.IssueCommand(cmd)
    all_tables = []
    for table in json.loads(str(tables_list)):
      if table['type'] == 'TABLE':
        all_tables.append(table['tableReference']['tableId'])
    return all_tables

  def ExtractDataset(self,
                     dest_bucket,
                     dataset=None,
                     tables=None,
                     dest_format='CSV'):
    """Extract all tables in a dataset to a GCS bucket.

    Args:
      dest_bucket: Name of the bucket to extract the data to. Should already
        exist.
      dataset: Optional name of the dataset. If none, will be extracted from the
        cluster_identifier.
      tables: Optional list of table names to extract. If none, all tables in
        the dataset will be extracted.
      dest_format: Format to extract data in. Can be one of: CSV, JSON, or Avro.
    """
    if tables is None:
      tables = self.GetAllTablesInDataset(dataset)
    gcs_uri = 'gs://' + dest_bucket

    # Make sure the bucket is empty.
    vm_util.IssueCommand(['gsutil', '-m', 'rm', gcs_uri + '/**'],
                         raise_on_failure=False)

    project_dataset = self.FormatProjectAndDatasetForCommand(dataset)
    for table in tables:
      cmd = [
          'bq', 'extract',
          '--destination_format=%s' % dest_format,
          '%s.%s' % (project_dataset, table),
          '%s/%s/*.csv' % (gcs_uri, table)
      ]
      _, stderr, retcode = vm_util.IssueCommand(cmd)
      # There is a 10T daily limit on extracting from BQ. Large datasets will
      # inherently hit this limit and benchmarks shouldn't use those.
      gcp_util.CheckGcloudResponseKnownFailures(stderr, retcode)

  def RemoveDataset(self, dataset=None):
    """Removes a dataset.

    See https://cloud.google.com/bigquery/docs/managing-tables#deleting_tables

    Args:
      dataset: Optional name of the dataset. If none, will be extracted from the
        cluster_identifier.
    """
    project_dataset = self.FormatProjectAndDatasetForCommand(dataset)
    vm_util.IssueCommand(['bq', 'rm', '-r', '-f', '-d', project_dataset],
                         raise_on_failure=False)

  def CreateDataset(self, dataset=None, description=None):
    """Creates a new dataset.

    See https://cloud.google.com/bigquery/docs/tables

    Args:
      dataset: Optional name of the dataset. If none, will be extracted from the
        cluster_identifier.
      description: Optional description of the dataset. Escape double quotes.
    """
    project_dataset = self.FormatProjectAndDatasetForCommand(dataset)
    cmd = [
        'bq', 'mk', '--dataset',
        '--default_table_expiration=%d' % DEFAULT_TABLE_EXPIRATION
    ]
    if description:
      cmd.extend(['--description', '"%s"' % description])
    cmd.append(project_dataset)
    vm_util.IssueCommand(cmd)

  def LoadDataset(self,
                  source_bucket,
                  tables,
                  schema_dir,
                  dataset=None,
                  append=True,
                  skip_header_row=True):
    """Load all tables in a dataset to a database from CSV object storage.

    See https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv

    Args:
      source_bucket: Name of the bucket to load the data from. Should already
        exist. Each table must have its own subfolder in the bucket named after
        the table, containing one or more csv files that make up the table data.
      tables: List of table names to load.
      schema_dir: GCS directory containing json schemas of all tables to load.
      dataset: Optional name of the dataset. If none, will be extracted from the
        cluster_identifier.
      append: If True, appends loaded data to the existing set. If False,
        replaces the existing data (if any).
      skip_header_row: If True, skips the first row of data being loaded.
    """
    project_dataset = self.FormatProjectAndDatasetForCommand(dataset)
    for table in tables:
      schema_path = schema_dir + table + '.json'
      local_schema = './%s.json' % table
      vm_util.IssueCommand(['gsutil', 'cp', schema_path, local_schema])
      cmd = [
          'bq', 'load', '--noreplace' if append else '--replace',
          '--source_format=CSV',
          '--skip_leading_rows=%d' % 1 if skip_header_row else 0,
          '%s.%s' % (project_dataset, table),
          'gs://%s/%s/*.csv' % (source_bucket, table), local_schema
      ]
      _, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
      if retcode:
        logging.warning('Loading table %s failed. stderr: %s, retcode: %s',
                        table, stderr, retcode)
