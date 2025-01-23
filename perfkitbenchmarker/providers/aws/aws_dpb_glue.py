"""Module containing class for AWS Glue as a Data Processing Backend service.

Docs:
- Authoring ETL scripts in Python:
  https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html
- AWS Glue CLI:
  https://docs.aws.amazon.com/cli/latest/reference/glue/index.html
"""

import json
import os
from typing import List

from absl import flags
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_dpb_glue_prices
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

GLUE_TIMEOUT = 14400


def _ModuleFromPyFilename(py_filename):
  return os.path.splitext(os.path.basename(py_filename))[0]


class AwsDpbGlue(
    dpb_service.DpbServiceServerlessMixin, dpb_service.BaseDpbService
):
  """Resource that allows spawning serverless AWS Glue Jobs."""

  CLOUD = provider_info.AWS
  SERVICE_TYPE = 'glue'
  SPARK_SQL_GLUE_WRAPPER_SCRIPT = (
      'spark_sql_test_scripts/spark_sql_glue_wrapper.py'
  )

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    self.project = None
    self.cmd_prefix = list(util.AWS_PREFIX)
    if not self.dpb_service_zone:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.'
      )
    self.region = util.GetRegionFromZone(self.dpb_service_zone)
    self.cmd_prefix += ['--region', self.region]
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(location=self.region)
    self.persistent_fs_prefix = 's3://'
    self.role = FLAGS.aws_glue_job_role
    self._cluster_create_time = None
    self._job_counter = 0

    # Last job run cost
    self._run_cost = dpb_service.JobCosts()
    self._FillMetadata()

  @property
  def _glue_script_wrapper_url(self):
    return os.path.join(self.base_dir, self.SPARK_SQL_GLUE_WRAPPER_SCRIPT)

  def _FetchStderr(self, job_run_id: str) -> str:
    cmd = self.cmd_prefix + [
        'logs',
        'get-log-events',
        '--log-group-name',
        '/aws-glue/jobs/error',
        '--log-stream-name',
        job_run_id,
        '--output',
        'text',
        '--query',
        'events[*].[message]',
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    return stdout

  def _GetCompletedJob(self, job_id):
    """See base class."""
    job_name, job_run_id = job_id
    cmd = self.cmd_prefix + [
        'glue',
        'get-job-run',
        '--job-name',
        job_name,
        '--run-id',
        job_run_id,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      raise errors.VmUtil.IssueCommandError(
          f'Getting step status failed:\n{stderr}'
      )
    result = json.loads(stdout)
    state = result['JobRun']['JobRunState']
    if state in ('FAILED', 'ERROR', 'TIMEOUT'):
      raise dpb_service.JobSubmissionError(result['JobRun'].get('ErrorMessage'))
    if state == 'SUCCEEDED':
      started_on = result['JobRun']['StartedOn']
      completed_on = result['JobRun']['CompletedOn']
      execution_time = result['JobRun']['ExecutionTime']
      self._run_cost = self._ComputeJobRunCost(
          result['JobRun'].get('DPUSeconds'),
          result['JobRun'].get('MaxCapacity'),
          result['JobRun'].get('ExecutionTime'),
      )
      return dpb_service.JobResult(
          run_time=execution_time,
          pending_time=completed_on - started_on - execution_time,
          fetch_output_fn=lambda: (None, self._FetchStderr(job_run_id)),
      )

  def SubmitJob(
      self,
      jarfile=None,
      classname=None,
      pyspark_file=None,
      query_file=None,
      job_poll_interval=None,
      job_stdout_file=None,
      job_arguments=None,
      job_files=None,
      job_jars=None,
      job_py_files=None,
      job_type=None,
      properties=None,
  ):
    """See base class."""
    assert job_type

    # Create job definition
    job_name = f'{self.cluster_id}-{self._job_counter}'
    self.metadata['dpb_job_id'] = job_name
    self._job_counter += 1
    glue_command = {}
    glue_default_args = {}
    if job_type == dpb_constants.PYSPARK_JOB_TYPE:
      glue_command = {
          'Name': 'glueetl',
          'ScriptLocation': self._glue_script_wrapper_url,
      }
      all_properties = self.GetJobProperties()
      extra_py_files = [pyspark_file]
      if job_py_files:
        extra_py_files += job_py_files
      if properties:
        all_properties.update(properties)
      glue_default_args = {
          '--extra-py-files': ','.join(extra_py_files),
          **all_properties,
      }
    else:
      raise ValueError(f'Unsupported job type {job_type} for AWS Glue.')
    vm_util.IssueCommand(
        self.cmd_prefix
        + [
            'glue',
            'create-job',
            '--name',
            job_name,
            '--role',
            self.role,
            '--command',
            json.dumps(glue_command),
            '--default-arguments',
            json.dumps(glue_default_args),
            '--glue-version',
            self.GetDpbVersion(),
            '--number-of-workers',
            str(self.spec.worker_count),
            '--worker-type',
            self.spec.worker_group.vm_spec.machine_type,
        ]
    )

    # Run job definition
    stdout, _, _ = vm_util.IssueCommand(
        self.cmd_prefix
        + [
            'glue',
            'start-job-run',
            '--job-name',
            job_name,
            '--arguments',
            json.dumps({
                '--pkb_main': _ModuleFromPyFilename(pyspark_file),
                '--pkb_args': json.dumps(job_arguments),
            }),
        ]
    )
    job_run_id = json.loads(stdout)['JobRunId']

    return self._WaitForJob(
        (job_name, job_run_id), GLUE_TIMEOUT, job_poll_interval
    )

  def _Delete(self):
    """Deletes Glue Jobs created to avoid quota issues."""
    for i in range(self._job_counter):
      job_name = f'{self.cluster_id}-{i}'
      self._DeleteGlueJob(job_name)

  def _DeleteGlueJob(self, job_name: str):
    vm_util.IssueCommand(
        self.cmd_prefix + ['glue', 'delete-job', f'--job-name={job_name}'],
        raise_on_failure=False,
    )

  def _FillMetadata(self) -> None:
    """Gets a dict to initialize this DPB service instance's metadata."""
    basic_data = self.metadata

    # Disk size in GB as specified in
    # https://docs.aws.amazon.com/glue/latest/dg/add-job.html#:~:text=Own%20Custom%20Scripts.-,Worker%20type,-The%20following%20worker
    disk_size_by_worker_type = {'Standard': '50', 'G.1X': '64', 'G.2X': '128'}
    dpb_disk_size = disk_size_by_worker_type.get(
        self.spec.worker_group.vm_spec.machine_type, 'Unknown'
    )

    self.metadata = {
        'dpb_service': basic_data['dpb_service'],
        'dpb_version': basic_data['dpb_version'],
        'dpb_service_version': basic_data['dpb_service_version'],
        'dpb_cluster_shape': basic_data['dpb_cluster_shape'],
        'dpb_cluster_size': basic_data['dpb_cluster_size'],
        'dpb_hdfs_type': basic_data['dpb_hdfs_type'],
        'dpb_disk_size': dpb_disk_size,
        'dpb_service_zone': basic_data['dpb_service_zone'],
        'dpb_job_properties': basic_data['dpb_job_properties'],
    }

  def GetServiceWrapperScriptsToUpload(self) -> List[str]:
    """Gets service wrapper scripts to upload alongside benchmark scripts."""
    return [self.SPARK_SQL_GLUE_WRAPPER_SCRIPT]

  def CalculateLastJobCosts(self) -> dpb_service.JobCosts:
    return self._run_cost

  def _ComputeJobRunCost(
      self,
      dpu_seconds: float | None,
      max_capacity: float | None,
      execution_time: float | None,
  ) -> dpb_service.JobCosts:
    """Computes the job run cost.

    If dpu_seconds is not None, then the job run cost will be computed only with
    that argument. Otherwise max_capacity and execution_time will be used. (This
    is because DPU seconds are only reported for autoscaling jobs).

    Currently run costs can only be computed for the us-east-1 region.

    Args:
      dpu_seconds: float. DPU * seconds as reported by the AWS Glue API.
      max_capacity: float. Max capacity as reported by the AWS Glue API.
      execution_time: float. Execution time in seconds as reported by the AWS
        Glue API.

    Returns:
      A float representing the job run cost in USD, or None if the cost cannot
      be computed for the current region.
    """
    dpu_hourly_price = aws_dpb_glue_prices.GLUE_PRICES.get(self.region)
    if dpu_hourly_price is None:
      return dpb_service.JobCosts()
    # dpu_seconds is only reported directly in auto-scaling jobs.
    if dpu_seconds is None:
      dpu_seconds = max_capacity * execution_time
    hourly_dpu = dpu_seconds / 3600
    cost = hourly_dpu * dpu_hourly_price
    return dpb_service.JobCosts(
        total_cost=cost,
        compute_cost=cost,
        compute_units_used=hourly_dpu,
        compute_unit_name='DPU*hr',
        compute_unit_cost=dpu_hourly_price,
    )

  def GetHdfsType(self) -> str | None:
    """Gets human friendly disk type for metric metadata."""
    return 'default-disk'
