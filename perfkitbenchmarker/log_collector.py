# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Utilities for collecting logs."""

import datetime
from absl import flags
from perfkitbenchmarker import vm_util

GSUTIL_MV = 'mv'
GSUTIL_CP = 'cp'
GSUTIL_OPERATIONS = [GSUTIL_MV, GSUTIL_CP]

PKB_LOG_BUCKET = flags.DEFINE_string(
    'pkb_log_bucket',
    None,
    'Name of the GCS bucket that PKB logs should route to. If this is not '
    'specified, then PKB logs will remain on the VM. This bucket must exist '
    'and the caller must have write permissions on the bucket for a successful '
    'export.',
)
VM_LOG_BUCKET = flags.DEFINE_string(
    'vm_log_bucket',
    None,
    'The GCS bucket to store VM logs in. If not provided, VM logs will go to '
    'the calling machine only. This only applies if --capture_vm_logs is '
    'set.',
)
_SAVE_LOG_TO_BUCKET_OPERATION = flags.DEFINE_enum(
    'save_log_to_bucket_operation',
    GSUTIL_MV,
    GSUTIL_OPERATIONS,
    'How to save the log to the bucket, available options are mv, cp',
)
_RELATIVE_GCS_LOG_PATH = flags.DEFINE_string(
    'relative_gcs_log_path',
    None,
    'The relative path inside the GCS bucket where to save the log, e.g. '
    '"root_dir/sub_dir", and the full file path would be '
    'gs://<bucket>/<relative_gcs_log_path>',
)


def CollectPKBLogs(run_uri: str, log_local_path: str) -> None:
  """Move PKB log files over to a GCS bucket (`pkb_log_bucket` flag).

  All failures in the process of log collection are suppressed to avoid causing
  a run to fail unnecessarily.

  Args:
    run_uri: The run URI of the benchmark run.
    log_local_path: Path to local log file.
  """
  if PKB_LOG_BUCKET.value:
    # Generate the log path to the cloud bucket based on the invocation date of
    # this function.
    gcs_log_path = GetLogCloudPath(PKB_LOG_BUCKET.value, f'{run_uri}-pkb.log')
    vm_util.IssueCommand(
        [
            'gcloud',
            'storage',
            _SAVE_LOG_TO_BUCKET_OPERATION.value,
            '--gzip-local-all',
            log_local_path,
            gcs_log_path,
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
    )
    vm_util.IssueCommand(
        [
            'gcloud',
            'storage',
            'objects',
            'update',
            gcs_log_path,
            '--content-type=text/plain',
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
    )


def CollectVMLogs(run_uri: str, source_path: str) -> None:
  """Move VM log files over to a GCS bucket (`vm_log_bucket` flag).

  All failures in the process of log collection are suppressed to avoid causing
  a run to fail unnecessarily.

  Args:
    run_uri: The run URI of the benchmark run.
    source_path: The path to the log file.
  """
  if VM_LOG_BUCKET.value:
    source_filename = source_path.split('/')[-1]
    gcs_directory_path = GetLogCloudPath(VM_LOG_BUCKET.value, run_uri)
    gcs_path = f'{gcs_directory_path}/{source_filename}'
    vm_util.IssueCommand(
        [
            'gcloud',
            'storage',
            'mv',
            '--gzip-local-all',
            source_path,
            gcs_path,
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
    )
    vm_util.IssueCommand(
        [
            'gcloud',
            'storage',
            'objects',
            'update',
            gcs_path,
            '--content-type=text/plain',
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
    )


def GetLogCloudPath(log_bucket: str, path_suffix: str) -> str:
  """Returns the GCS path, to where the logs should be saved.

  Args:
    log_bucket: The GCS bucket to save the logs to.
    path_suffix: The suffix to append to the GCS path.

  Returns:
    The GCS path to where the logs should be saved.
  """
  run_date = datetime.date.today()
  gcs_path_prefix = _GetGcsPathPrefix(log_bucket)
  return (
      f'{gcs_path_prefix}/'
      + f'{run_date.year:04d}/{run_date.month:02d}/'
      + f'{run_date.day:02d}/'
      + path_suffix
  )


def _GetGcsPathPrefix(bucket: str) -> str:
  """Returns the GCS path prefix, to where the logs should be saved.

  Args:
    bucket: The GCS bucket to save the logs to.

  Returns:
    The GCS path prefix, to where the logs should be saved. If
    `relative_gcs_log_path` is specified, the prefix will be
    gs://<bucket>/<relative_gcs_log_path>. Otherwise, it will be gs://<bucket>.
  """
  if _RELATIVE_GCS_LOG_PATH.value:
    return f'gs://{bucket}/{_RELATIVE_GCS_LOG_PATH.value}'
  return f'gs://{bucket}'
