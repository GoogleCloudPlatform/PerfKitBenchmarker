"""Module containing class for AWS Glue as a Data Processing Backend service.

Docs:
- Authoring ETL scripts in Python:
  https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html
- AWS Glue CLI:
  https://docs.aws.amazon.com/cli/latest/reference/glue/index.html
"""

import json
import os
from typing import List, Optional

from absl import flags
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

GLUE_TIMEOUT = 14400


def _ModuleFromPyFilename(py_filename):
  return os.path.splitext(os.path.basename(py_filename))[0]


class AwsDpbGlue(dpb_service.BaseDpbService):
  """Resource that allows spawning serverless AWS Glue Jobs."""

  CLOUD = providers.AWS
  SERVICE_TYPE = 'glue'
  SPARK_SQL_GLUE_WRAPPER_SCRIPT = (
      'spark_sql_test_scripts/spark_sql_glue_wrapper.py'
  )

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    self.dpb_service_type = self.SERVICE_TYPE
    self.project = None
    self.cmd_prefix = list(util.AWS_PREFIX)
    if not self.dpb_service_zone:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.')
    self.region = util.GetRegionFromZone(self.dpb_service_zone)
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(location=self.region)
    self.persistent_fs_prefix = 's3://'
    self.role = FLAGS.aws_glue_job_role
    self._cluster_create_time = None
    self._job_counter = 0

  @property
  def _glue_script_wrapper_url(self):
    return os.path.join(self.base_dir, self.SPARK_SQL_GLUE_WRAPPER_SCRIPT)

  def _GetCompletedJob(self, job_id):
    """See base class."""
    job_name, job_run_id = job_id
    cmd = self.cmd_prefix + [
        'glue',
        'get-job-run',
        '--job-name', job_name,
        '--run-id', job_run_id
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      raise errors.VmUtil.IssueCommandError(
          f'Getting step status failed:\n{stderr}')
    result = json.loads(stdout)
    state = result['JobRun']['JobRunState']
    if state in ('FAILED', 'ERROR', 'TIMEOUT'):
      raise dpb_service.JobSubmissionError(result['JobRun'].get('ErrorMessage'))
    if state == 'SUCCEEDED':
      started_on = result['JobRun']['StartedOn']
      completed_on = result['JobRun']['CompletedOn']
      execution_time = result['JobRun']['ExecutionTime']
      return dpb_service.JobResult(
          run_time=execution_time,
          pending_time=completed_on - started_on - execution_time)

  def SubmitJob(self,
                jarfile=None,
                classname=None,
                pyspark_file=None,
                query_file=None,
                job_poll_interval=5,
                job_stdout_file=None,
                job_arguments=None,
                job_files=None,
                job_jars=None,
                job_type=None,
                properties=None):
    """See base class."""
    assert job_type

    # Create job definition
    job_name = f'{self.cluster_id}-{self._job_counter}'
    self._job_counter += 1
    glue_command = {}
    glue_default_args = {}
    if job_type == self.PYSPARK_JOB_TYPE:
      glue_command = {
          'Name': 'glueetl',
          'ScriptLocation': self._glue_script_wrapper_url,
      }
      all_properties = self.GetJobProperties()
      if properties:
        all_properties.update(properties)
      glue_default_args = {'--extra-py-files': pyspark_file, **all_properties}
    else:
      raise ValueError(f'Unsupported job type {job_type} for AWS Glue.')
    vm_util.IssueCommand(self.cmd_prefix + [
        'glue',
        'create-job',
        '--name', job_name,
        '--role', self.role,
        '--command', json.dumps(glue_command),
        '--default-arguments', json.dumps(glue_default_args),
        '--glue-version', self.dpb_version,
        '--number-of-workers', str(self.spec.worker_count),
        '--worker-type', self.spec.worker_group.vm_spec.machine_type,

    ])

    # Run job definition
    stdout, _, _ = vm_util.IssueCommand(self.cmd_prefix + [
        'glue',
        'start-job-run',
        '--job-name', job_name,
        '--arguments', json.dumps(
            {'--pkb_main': _ModuleFromPyFilename(pyspark_file),
             '--pkb_args': json.dumps(job_arguments)})])
    job_run_id = json.loads(stdout)['JobRunId']

    return self._WaitForJob((job_name, job_run_id), GLUE_TIMEOUT,
                            job_poll_interval)

  def _Create(self):
    # Since there's no managed infrastructure, this is a no-op.
    pass

  def _Delete(self):
    # Since there's no managed infrastructure, this is a no-op.
    pass

  def GetClusterCreateTime(self) -> Optional[float]:
    return None

  def GetMetadata(self):
    basic_data = super().GetMetadata()

    # Disk size in GB as specified in
    # https://docs.aws.amazon.com/glue/latest/dg/add-job.html#:~:text=Own%20Custom%20Scripts.-,Worker%20type,-The%20following%20worker
    disk_size_by_worker_type = {'Standard': '50', 'G.1X': '64', 'G.2X': '128'}
    dpb_disk_size = disk_size_by_worker_type.get(
        self.spec.worker_group.vm_spec.machine_type, 'Unknown')

    return {
        'dpb_service': basic_data['dpb_service'],
        'dpb_version': basic_data['dpb_version'],
        'dpb_service_version': basic_data['dpb_service_version'],
        'dpb_job_id': f"{basic_data['dpb_cluster_id']}-{self._job_counter - 1}",
        'dpb_cluster_shape': basic_data['dpb_cluster_shape'],
        'dpb_cluster_size': basic_data['dpb_cluster_size'],
        'dpb_hdfs_type': 'default-disk',
        'dpb_disk_size': dpb_disk_size,
        'dpb_service_zone': basic_data['dpb_service_zone'],
        'dpb_job_properties': basic_data['dpb_job_properties'],
    }

  def GetServiceWrapperScriptsToUpload(self) -> List[str]:
    """Gets service wrapper scripts to upload alongside benchmark scripts."""
    return [self.SPARK_SQL_GLUE_WRAPPER_SCRIPT]
