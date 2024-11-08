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

import copy
import datetime
import json
import logging
import os
import re

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import google_cloud_sdk
from perfkitbenchmarker.providers.gcp import util as gcp_util


FLAGS = flags.FLAGS

BQ_CLIENT_FILE = 'bq-jdbc-simba-client-1.8-temp-labels.jar'
BQ_PYTHON_CLIENT_FILE = 'bq_python_driver.py'
BQ_PYTHON_CLIENT_DIR = 'edw/bigquery/clients/python'
DEFAULT_TABLE_EXPIRATION = 3600 * 24 * 365  # seconds

BQ_JDBC_INTERFACES = [
    'SIMBA_JDBC_1_2_4_1007',
    'SIMBA_JDBC_1_3_3_1004',
    'SIMBA_JDBC_1_5_0_1001',
    'SIMBA_JDBC_1_5_2_1005',
]
BQ_JDBC_CLIENT_FILE = {
    'SIMBA_JDBC_1_2_4_1007': 'bq-jdbc-client-1.0.jar',
    'SIMBA_JDBC_1_3_3_1004': 'bq-jdbc-client-2.0.jar',
    'SIMBA_JDBC_1_5_0_1001': 'bq-jdbc-client-2.1.jar',
    'SIMBA_JDBC_1_5_2_1005': 'bq-jdbc-client-2.2.jar',
}
BQ_JDBC_JAR_FILE = {
    'SIMBA_JDBC_1_2_4_1007': 'GoogleBigQueryJDBC42.jar',
    'SIMBA_JDBC_1_3_3_1004': 'GoogleBigQueryJDBC42-1.3.3.1004.jar',
    'SIMBA_JDBC_1_5_0_1001': 'GoogleBigQueryJDBC42-1.5.0.1001.jar',
    'SIMBA_JDBC_1_5_2_1005': 'GoogleBigQueryJDBC42-1.5.2.1005.jar',
}


def GetBigQueryClientInterface(
    project_id: str, dataset_id: str
) -> edw_service.EdwClientInterface:
  """Builds and Returns the requested BigQuery client Interface.

  Args:
    project_id: String name of the BigQuery project to benchmark
    dataset_id: String name of the BigQuery dataset to benchmark

  Returns:
    A concrete Client Interface object (subclass of GenericClientInterface)

  Raises:
    RuntimeError: if an unsupported bq_client_interface is requested
  """
  if FLAGS.bq_client_interface == 'CLI':
    return CliClientInterface(project_id, dataset_id)
  if FLAGS.bq_client_interface == 'JAVA':
    return JavaClientInterface(project_id, dataset_id)
  if FLAGS.bq_client_interface in BQ_JDBC_INTERFACES:
    return JdbcClientInterface(project_id, dataset_id)
  if FLAGS.bq_client_interface == 'PYTHON':
    return PythonClientInterface(project_id, dataset_id)
  raise RuntimeError('Unknown BigQuery Client Interface requested.')


class GenericClientInterface(edw_service.EdwClientInterface):
  """Generic Client Interface class for BigQuery.

  Attributes:
    project_id: String name of the BigQuery project to benchmark
    dataset_id: String name of the BigQuery dataset to benchmark
  """

  def __init__(self, project_id: str, dataset_id: str):
    self.project_id = project_id
    self.dataset_id = dataset_id

  def GetMetadata(self) -> dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.bq_client_interface}


class CliClientInterface(GenericClientInterface):
  """Command Line Client Interface class for BigQuery.

  Uses the native Bigquery client that ships with the google_cloud_sdk
  https://cloud.google.com/bigquery/docs/bq-command-line-tool.
  """

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the bq tool dependencies and authenticates using a service account.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand('sudo pip install absl-py')
    self.client_vm.Install('google_cloud_sdk')

    # Push the service account file to the working directory on client vm
    key_file_name = FLAGS.gcp_service_account_key_file.split('/')[-1]
    if '/' in FLAGS.gcp_service_account_key_file:
      self.client_vm.PushFile(FLAGS.gcp_service_account_key_file)
    else:
      self.client_vm.InstallPreprovisionedPackageData(
          package_name, [FLAGS.gcp_service_account_key_file], ''
      )

    # Authenticate using the service account file
    vm_gcloud_path = google_cloud_sdk.GCLOUD_PATH
    activate_cmd = '{} auth activate-service-account {} --key-file={}'.format(
        vm_gcloud_path, FLAGS.gcp_service_account, key_file_name
    )
    self.client_vm.RemoteCommand(activate_cmd)

    # Push the framework to execute a sql query and gather performance details
    service_specific_dir = os.path.join('edw', Bigquery.SERVICE_TYPE)
    self.client_vm.PushFile(
        data.ResourcePath(
            os.path.join(service_specific_dir, 'script_runner.sh')
        )
    )
    runner_permission_update_cmd = 'chmod 755 {}'.format('script_runner.sh')
    self.client_vm.RemoteCommand(runner_permission_update_cmd)
    self.client_vm.PushFile(
        data.ResourcePath(os.path.join('edw', 'script_driver.py'))
    )
    self.client_vm.PushFile(
        data.ResourcePath(
            os.path.join(
                service_specific_dir, 'provider_specific_script_driver.py'
            )
        )
    )

  def ExecuteQuery(self, query_name: str) -> tuple[float, dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = (
        'python script_driver.py --script={} --bq_project_id={} '
        '--bq_dataset_id={}'
    ).format(query_name, self.project_id, self.dataset_id)
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    performance = json.loads(stdout)
    details = copy.copy(self.GetMetadata())  # Copy the base metadata
    details['job_id'] = performance[query_name]['job_id']
    return float(performance[query_name]['execution_time']), details


class JdbcClientInterface(GenericClientInterface):
  """JDBC Client Interface class for BigQuery.

  https://cloud.google.com/bigquery/providers/simba-drivers
  """

  def SetProvisionedAttributes(self, benchmark_spec):
    super().SetProvisionedAttributes(benchmark_spec)
    self.project_id = re.split(
        r'\.', benchmark_spec.edw_service.cluster_identifier
    )[0]
    self.dataset_id = re.split(
        r'\.', benchmark_spec.edw_service.cluster_identifier
    )[1]

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs
    a) Java Execution Environment,
    b) BigQuery Authnetication Credentials,
    c) JDBC Application to execute a query and gather execution details,
    d) Simba JDBC BigQuery client code dependencencies, and
    e) The Simba JDBC interface jar

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('openjdk')

    # Push the service account file to the working directory on client vm
    self.client_vm.InstallPreprovisionedPackageData(
        package_name, [FLAGS.gcp_service_account_key_file], ''
    )

    # Push the executable jars to the working directory on client vm
    self.client_vm.InstallPreprovisionedPackageData(
        package_name,
        [
            BQ_JDBC_CLIENT_FILE[FLAGS.bq_client_interface],
            BQ_JDBC_JAR_FILE[FLAGS.bq_client_interface],
        ],
        '',
    )

  def ExecuteQuery(self, query_name: str) -> tuple[float, dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = (
        'java -cp {}:{} '
        'com.google.cloud.performance.edw.App --project {} --service_account '
        '{} --credentials_file {} --dataset {} --query_file {}'.format(
            BQ_JDBC_CLIENT_FILE[FLAGS.bq_client_interface],
            BQ_JDBC_JAR_FILE[FLAGS.bq_client_interface],
            self.project_id,
            FLAGS.gcp_service_account,
            FLAGS.gcp_service_account_key_file,
            self.dataset_id,
            query_name,
        )
    )
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    details = copy.copy(self.GetMetadata())  # Copy the base metadata
    details.update(json.loads(stdout)['details'])
    return json.loads(stdout)['performance'], details


class JavaClientInterface(GenericClientInterface):
  """Native Java Client Interface class for BigQuery.

  https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-java
  """

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the Java Execution Environment and a uber jar with
    a) BigQuery Java client libraries,
    b) An application to execute a query and gather execution details, and
    c) their dependencies.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('openjdk')

    # Push the service account file to the working directory on client vm
    if '/' in FLAGS.gcp_service_account_key_file:
      self.client_vm.PushFile(FLAGS.gcp_service_account_key_file)
    else:
      self.client_vm.InstallPreprovisionedPackageData(
          package_name, [FLAGS.gcp_service_account_key_file], ''
      )
    # Push the executable jar to the working directory on client vm
    self.client_vm.InstallPreprovisionedPackageData(
        package_name, [BQ_CLIENT_FILE], ''
    )

  def ExecuteQuery(self, query_name: str) -> tuple[float, dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute.

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    key_file_name = FLAGS.gcp_service_account_key_file
    if '/' in FLAGS.gcp_service_account_key_file:
      key_file_name = FLAGS.gcp_service_account_key_file.split('/')[-1]

    query_command = (
        'java -Xmx6g -cp {} '
        'com.google.cloud.performance.edw.Single --project {} '
        '--credentials_file {} --dataset {} '
        '--query_file {}'
    ).format(
        BQ_CLIENT_FILE,
        self.project_id,
        key_file_name,
        self.dataset_id,
        query_name,
    )
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    details = copy.copy(self.GetMetadata())  # Copy the base metadata
    details.update(json.loads(stdout)['details'])
    return json.loads(stdout)['query_wall_time_in_secs'], details

  def ExecuteSimultaneous(
      self, submission_interval: int, queries: list[str]
  ) -> str:
    """Executes queries simultaneously on client and return performance details.

    Simultaneous app expects queries as white space separated query file names.

    Args:
      submission_interval: Simultaneous query submission interval in
        milliseconds.
      queries: List of strings (names) of queries to execute.

    Returns:
      A serialized dictionary of execution details.
    """
    key_file_name = FLAGS.gcp_service_account_key_file
    if '/' in FLAGS.gcp_service_account_key_file:
      key_file_name = os.path.basename(FLAGS.gcp_service_account_key_file)
    cmd = (
        'java -Xmx6g -cp {} '
        'com.google.cloud.performance.edw.Simultaneous --project {} '
        '--credentials_file {} --dataset {} --submission_interval {} '
        '--query_files {}'.format(
            BQ_CLIENT_FILE,
            self.project_id,
            key_file_name,
            self.dataset_id,
            submission_interval,
            ' '.join(queries),
        )
    )
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    key_file_name = FLAGS.gcp_service_account_key_file
    if '/' in FLAGS.gcp_service_account_key_file:
      key_file_name = os.path.basename(FLAGS.gcp_service_account_key_file)
    runlabels = ''
    if labels:
      for key, value in labels.items():
        runlabels += f' --label {key}={value}'
    cmd = (
        'java -Xmx6g -cp {} '
        'com.google.cloud.performance.edw.Throughput --project {} '
        '--credentials_file {} --dataset {} --query_streams {}'.format(
            BQ_CLIENT_FILE,
            self.project_id,
            key_file_name,
            self.dataset_id,
            ' '.join([','.join(stream) for stream in concurrency_streams]),
        )
        + runlabels
    )
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout


class PythonClientInterface(GenericClientInterface):
  """Python Client Interface class for BigQuery."""

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.key_file_name = FLAGS.gcp_service_account_key_file
    if '/' in FLAGS.gcp_service_account_key_file:
      self.key_file_name = os.path.basename(FLAGS.gcp_service_account_key_file)

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query."""
    # Push the service account file to the working directory on client vm
    if '/' in FLAGS.gcp_service_account_key_file:
      self.client_vm.PushFile(FLAGS.gcp_service_account_key_file)
    else:
      self.client_vm.InstallPreprovisionedPackageData(
          package_name, [FLAGS.gcp_service_account_key_file], ''
      )

    # Install dependencies for driver
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand('sudo pip install google-cloud-bigquery')
    self.client_vm.RemoteCommand(
        'sudo pip install google-cloud-bigquery-storage'
    )
    self.client_vm.RemoteCommand('sudo pip install pyarrow')

    # Push driver script to client vm
    self.client_vm.PushDataFile(
        os.path.join(BQ_PYTHON_CLIENT_DIR, BQ_PYTHON_CLIENT_FILE)
    )

  def ExecuteQuery(self, query_name: str) -> tuple[float, dict[str, str]]:
    """Executes a query and returns performance details."""
    cmd = (
        f'python3 {BQ_PYTHON_CLIENT_FILE} single --project'
        f' {self.project_id} --credentials_file {self.key_file_name} --dataset'
        f' {self.dataset_id} --query_file {query_name} --feature_config'
        f' {FLAGS.edw_bq_feature_config}'
    )
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    details = copy.copy(self.GetMetadata())
    details.update(json.loads(stdout)['details'])
    return json.loads(stdout)['query_wall_time_in_secs'], details

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    """Executes queries simultaneously on client and return performance details."""
    cmd = (
        f'python3 {BQ_PYTHON_CLIENT_FILE} throughput --project'
        f' {self.project_id} --credentials_file {self.key_file_name} --dataset'
        f" {self.dataset_id} --query_streams='{json.dumps(concurrency_streams)}'"
        f' --feature_config {FLAGS.edw_bq_feature_config} --labels'
        f" '{json.dumps(labels)}'"
    )
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout


class Bigquery(edw_service.EdwService):
  """Object representing a Bigquery cluster.

  Attributes:
    job_id_prefix: A string prefix for the job id for bigquery job.
  """

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'bigquery'

  def __init__(self, edw_service_spec):
    super().__init__(edw_service_spec)
    project_id = re.split(r'\.', self.cluster_identifier)[0]
    dataset_id = re.split(r'\.', self.cluster_identifier)[1]
    self.client_interface = GetBigQueryClientInterface(project_id, dataset_id)

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
    basic_data = super().GetMetadata()
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data

  def FormatProjectAndDatasetForCommand(self, dataset=None):
    """Returns the project and dataset in the format needed for bq commands.

    E.g., project:dataset.

    Args:
      dataset: The dataset to run commands against. If None, extracts the
        dataset from the cluster identifier whose format is "project.dataset").
    """
    return (
        (self.cluster_identifier.split('.')[0] + ':' + dataset)
        if dataset
        else self.cluster_identifier.replace('.', ':')
    )

  def GetDatasetLastUpdatedTime(self, dataset=None):
    """Get the formatted last modified timestamp of the dataset."""
    cmd = [
        'bq',
        'show',
        '--format=prettyjson',
        self.FormatProjectAndDatasetForCommand(dataset),
    ]
    dataset_metadata, _, _ = vm_util.IssueCommand(cmd)
    metadata_json = json.loads(str(dataset_metadata))
    return datetime.datetime.fromtimestamp(
        float(metadata_json['lastModifiedTime']) / 1000.0
    ).strftime('%Y-%m-%d_%H-%M-%S')

  def GetAllTablesInDataset(self, dataset=None):
    """Returns a list of the IDs of all the tables in the dataset."""
    cmd = [
        'bq',
        'ls',
        '--format=prettyjson',
        self.FormatProjectAndDatasetForCommand(dataset),
    ]
    tables_list, _, _ = vm_util.IssueCommand(cmd)
    all_tables = []
    for table in json.loads(str(tables_list)):
      if table['type'] == 'TABLE':
        all_tables.append(table['tableReference']['tableId'])
    return all_tables

  def ExtractDataset(
      self, dest_bucket, dataset=None, tables=None, dest_format='CSV'
  ):
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
    vm_util.IssueCommand(
        ['gsutil', '-m', 'rm', gcs_uri + '/**'], raise_on_failure=False
    )

    project_dataset = self.FormatProjectAndDatasetForCommand(dataset)
    for table in tables:
      cmd = [
          'bq',
          'extract',
          '--destination_format=%s' % dest_format,
          '%s.%s' % (project_dataset, table),
          '%s/%s/*.csv' % (gcs_uri, table),
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
    vm_util.IssueCommand(
        ['bq', 'rm', '-r', '-f', '-d', project_dataset], raise_on_failure=False
    )

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
        'bq',
        'mk',
        '--dataset',
        '--default_table_expiration=%d' % DEFAULT_TABLE_EXPIRATION,
    ]
    if description:
      cmd.extend(['--description', '"%s"' % description])
    cmd.append(project_dataset)
    vm_util.IssueCommand(cmd)

    cmd = ['bq', 'update']
    for key, value in gcp_util.GetDefaultTags().items():
      cmd.extend(['--set_label', f'{key}:{value}'])
    cmd.append(project_dataset)
    vm_util.IssueCommand(cmd)

  def LoadDataset(
      self,  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      source_bucket,
      tables,
      schema_dir,
      dataset=None,
      append=True,
      skip_header_row=True,
      field_delimiter=',',
  ):
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
      field_delimiter: The separator for fields in the CSV file.
    """
    project_dataset = self.FormatProjectAndDatasetForCommand(dataset)
    for table in tables:
      schema_path = schema_dir + table + '.json'
      local_schema = './%s.json' % table
      vm_util.IssueCommand(['gsutil', 'cp', schema_path, local_schema])
      cmd = [
          'bq',
          'load',
          '--noreplace' if append else '--replace',
          '--source_format=CSV',
          '--field_delimiter=%s' % field_delimiter,
          '--skip_leading_rows=%d' % (1 if skip_header_row else 0),
          '%s.%s' % (project_dataset, table),
          'gs://%s/%s/*.csv' % (source_bucket, table),
          local_schema,
      ]
      _, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
      if retcode:
        logging.warning(
            'Loading table %s failed. stderr: %s, retcode: %s',
            table,
            stderr,
            retcode,
        )

      cmd = ['bq', 'update']
      for key, value in gcp_util.GetDefaultTags().items():
        cmd.extend(['--set_label', f'{key}:{value}'])
      cmd.append(f'{project_dataset}.{table}')
      vm_util.IssueCommand(cmd)

  def OpenDataset(self, dataset: str):
    self.client_interface.dataset_id = dataset

  def CopyTable(self, copy_table_name: str, to_dataset: str) -> None:
    source = f'{self.FormatProjectAndDatasetForCommand()}.{copy_table_name}'
    dest = f'{self.FormatProjectAndDatasetForCommand(to_dataset)}.{copy_table_name}'

    cmd = ['bq', 'cp', source, dest]
    vm_util.IssueCommand(cmd)


class Endor(Bigquery):
  """Class representing BigQuery Endor service."""

  SERVICE_TYPE = 'endor'

  def GetMetadata(self) -> dict[str, str]:
    """Return a dictionary of the metadata for the BigQuery Endor service.

    Returns:
      A dictionary set to Endor service details.
    """
    basic_data = super().GetMetadata()
    basic_data['edw_service_type'] = 'endor'
    basic_data.update(self.client_interface.GetMetadata())
    basic_data.update(self.GetDataDetails())
    return basic_data

  def GetDataDetails(self) -> dict[str, str]:
    """Returns a dictionary with underlying data details.

    cluster_identifier = <project_id>.<dataset_id>
    Data details are extracted from the dataset_id that follows the format:
    <dataset>_<format>_<compression>_<partitioning>_<location>
    eg.
    tpch100_parquet_uncompressed_unpartitoned_s3

    Returns:
      A dictionary set to underlying data's details (format, etc.)
    """
    data_details = {}
    dataset_id = re.split(r'\.', self.cluster_identifier)[1]
    parsed_id = re.split(r'_', dataset_id)
    data_details['format'] = parsed_id[1]
    data_details['compression'] = parsed_id[2]
    data_details['partitioning'] = parsed_id[3]
    data_details['location'] = parsed_id[4]
    return data_details


class Endorazure(Endor):
  """Class representing BigQuery Endor Azure service."""

  SERVICE_TYPE = 'endorazure'

  def GetMetadata(self) -> dict[str, str]:
    """Return a dictionary of the metadata for the BigQuery Endor Azure service.

    Returns:
      A dictionary set to Endor Azure service details.
    """
    basic_data = super().GetMetadata()
    basic_data['edw_service_type'] = 'endorazure'
    return basic_data


class Bqfederated(Bigquery):
  """Class representing BigQuery Federated service."""

  SERVICE_TYPE = 'bqfederated'

  def GetMetadata(self) -> dict[str, str]:
    """Return a dictionary of the metadata for the BigQuery Federated service.

    Returns:
      A dictionary set to Federated service details.
    """
    basic_data = super().GetMetadata()
    basic_data['edw_service_type'] = self.SERVICE_TYPE
    basic_data.update(self.client_interface.GetMetadata())
    basic_data.update(self.GetDataDetails())
    return basic_data

  def GetDataDetails(self) -> dict[str, str]:
    """Returns a dictionary with underlying data details.

    cluster_identifier = <project_id>.<dataset_id>
    Data details are extracted from the dataset_id which follows one of these
    three formats:

    <dataset>_<format>_<compression>_<partitioning>_<location>
    or
    <dataset>_<format>_<compression>_<partitioning>_<storage>_<location>
    or
    <dataset>_<format>_<table_format>_<compression>_<partitioning>_<storage>_<location>

    E.g:

    tpcds1000_parquet_compressed_partitioned_gcs
    tpcds1000_parquet_snappy_part_gcs_uscentral1
    tpcds1000_parquet_iceberg_snappy_part_gcs_us

    Returns:
      A dictionary set to underlying data's details (format, etc.)
    """
    data_details = {}
    project_id, dataset_id = re.split(r'\.', self.cluster_identifier)
    data_details['metadata_caching'] = str('metadata-caching' in project_id)
    parsed_id = re.split(r'_', dataset_id)
    if len(parsed_id) == 5:
      data_details['format'] = parsed_id[1]
      data_details['table_format'] = 'None'
      data_details['compression'] = parsed_id[2]
      data_details['partitioning'] = parsed_id[3]
      data_details['storage'] = parsed_id[4]
      data_details['location'] = 'us'
    elif len(parsed_id) == 6:
      data_details['format'] = parsed_id[1]
      data_details['table_format'] = 'None'
      data_details['compression'] = parsed_id[2]
      data_details['partitioning'] = parsed_id[3]
      data_details['storage'] = parsed_id[4]
      data_details['location'] = parsed_id[5]
    elif len(parsed_id) == 7:
      data_details['format'] = parsed_id[1]
      data_details['table_format'] = parsed_id[2]
      data_details['compression'] = parsed_id[3]
      data_details['partitioning'] = parsed_id[4]
      data_details['storage'] = parsed_id[5]
      data_details['location'] = parsed_id[6]
    else:
      data_details['format'] = 'unknown'
      data_details['table_format'] = 'unknown'
      data_details['compression'] = 'unknown'
      data_details['partitioning'] = 'unknown'
      data_details['storage'] = 'unknown'
      data_details['location'] = 'unknown'
    return data_details
