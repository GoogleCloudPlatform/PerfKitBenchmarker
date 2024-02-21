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
"""Module containing class for AWS's EMR services.

Clusters can be created and deleted.
"""

import collections
import dataclasses
import json
import logging
from typing import Any, Dict, Optional

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_dpb_emr_serverless_prices
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'dpb_emr_release_label', None, 'DEPRECATED use dpb_service.version.'
)

INVALID_STATES = ['TERMINATED_WITH_ERRORS', 'TERMINATED']
READY_CHECK_SLEEP = 30
READY_CHECK_TRIES = 60
READY_STATE = 'WAITING'
JOB_WAIT_SLEEP = 30
EMR_TIMEOUT = 14400

disk_to_hdfs_map = {
    aws_disk.ST1: 'HDD (ST1)',
    aws_disk.GP2: 'SSD (GP2)',
    disk.LOCAL: 'Local SSD',
}

DATAPROC_TO_EMR_CONF_FILES = {
    # https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
    'core': 'core-site',
    'hdfs': 'hdfs-site',
    # https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html
    'spark': 'spark-defaults',
}


def _GetClusterConfiguration(cluster_properties: list[str]) -> str:
  """Return a JSON string containing dpb_cluster_properties."""
  properties = collections.defaultdict(lambda: {})
  for entry in cluster_properties:
    file, kv = entry.split(':')
    key, value = kv.split('=')
    if file not in DATAPROC_TO_EMR_CONF_FILES:
      raise errors.Config.InvalidValue(
          'Unsupported EMR configuration file "{}". '.format(file)
          + 'Please add it to aws_dpb_emr.DATAPROC_TO_EMR_CONF_FILES.'
      )
    properties[DATAPROC_TO_EMR_CONF_FILES[file]][key] = value
  json_conf = []
  for file, props in properties.items():
    json_conf.append({
        # https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
        'Classification': file,
        'Properties': props,
    })
  return json.dumps(json_conf)


class EMRRetryableException(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class _AwsDpbEmrServerlessJobRun:
  """Holds one EMR Serverless job run info, such as IDs and usage stats.

  Attributes:
    application_id: The application ID the job run belongs to.
    job_run_id: The job run's ID.
    region: Job run's region.
    memory_gb_hour: RAM GB * hour used.
    storage_gb_hour: Shuffle storage GB * hour used.
    vcpu_hour: vCPUs * hour used.
  """

  application_id: Optional[str] = None
  job_run_id: Optional[str] = None
  region: Optional[str] = None
  memory_gb_hour: Optional[float] = None
  storage_gb_hour: Optional[float] = None
  vcpu_hour: Optional[float] = None

  def __bool__(self):
    """Returns if this represents a job run or is just a dummy placeholder."""
    return None not in (self.application_id, self.job_run_id, self.region)

  def HasStats(self):
    """Returns whether there are stats collected for the job run."""
    return None not in (
        self.memory_gb_hour,
        self.storage_gb_hour,
        self.vcpu_hour,
    )

  def ComputeJobRunCost(self) -> dpb_service.JobCosts:
    """Computes the cost of a run for region given this usage."""
    if not self.HasStats():
      return dpb_service.JobCosts()
    region_prices = aws_dpb_emr_serverless_prices.EMR_SERVERLESS_PRICES.get(
        self.region, {}
    )
    memory_gb_hour_price = region_prices.get('memory_gb_hours')
    storage_gb_hour_price = region_prices.get('storage_gb_hours')
    vcpu_hour_price = region_prices.get('vcpu_hours')
    if (
        memory_gb_hour_price is None
        or storage_gb_hour_price is None
        or vcpu_hour_price is None
    ):
      return dpb_service.JobCosts()
    vcpu_cost = self.vcpu_hour * vcpu_hour_price
    memory_cost = self.memory_gb_hour * memory_gb_hour_price
    storage_cost = self.storage_gb_hour * storage_gb_hour_price
    return dpb_service.JobCosts(
        total_cost=vcpu_cost + memory_cost + storage_cost,
        compute_cost=vcpu_cost,
        memory_cost=memory_cost,
        storage_cost=storage_cost,
        compute_units_used=self.vcpu_hour,
        memory_units_used=self.memory_gb_hour,
        storage_units_used=self.storage_gb_hour,
        compute_unit_cost=vcpu_hour_price,
        memory_unit_cost=memory_gb_hour_price,
        storage_unit_cost=storage_gb_hour_price,
        compute_unit_name='vCPU*hr',
        memory_unit_name='GB*hr',
        storage_unit_name='GB*hr',
    )


class AwsDpbEmr(dpb_service.BaseDpbService):
  """Object representing a AWS EMR cluster.

  Attributes:
    cluster_id: ID of the cluster.
    project: ID of the project in which the cluster is being launched.
    dpb_service_type: Set to 'emr'.
    cmd_prefix: Setting default prefix for the emr commands (region optional).
    network: Dedicated network for the EMR cluster
    storage_service: Region specific instance of S3 for bucket management.
    bucket_to_delete: Cluster associated bucket to be cleaned up.
    dpb_version: EMR version to use.
  """

  CLOUD = provider_info.AWS
  SERVICE_TYPE = 'emr'
  SUPPORTS_NO_DYNALLOC = True

  def __init__(self, dpb_service_spec):
    super(AwsDpbEmr, self).__init__(dpb_service_spec)
    self.project = None
    self.cmd_prefix = list(util.AWS_PREFIX)
    if self.dpb_service_zone:
      self.region = util.GetRegionFromZone(self.dpb_service_zone)
    else:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.'
      )
    self.cmd_prefix += ['--region', self.region]
    self.network = aws_network.AwsNetwork.GetNetworkFromNetworkSpec(
        aws_network.AwsNetworkSpec(zone=self.dpb_service_zone)
    )
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(self.region)
    self.persistent_fs_prefix = 's3://'
    self.bucket_to_delete = None
    self._cluster_create_time: Optional[float] = None
    self._cluster_ready_time: Optional[float] = None
    self._cluster_delete_time: Optional[float] = None
    if not self.GetDpbVersion():
      raise errors.Setup.InvalidSetupError(
          'dpb_service.version must be provided.'
      )

  def GetDpbVersion(self) -> Optional[str]:
    return FLAGS.dpb_emr_release_label or super().GetDpbVersion()

  def GetClusterCreateTime(self) -> Optional[float]:
    """Returns the cluster creation time.

    On this implementation, the time returned is based on the timestamps
    reported by the EMR API (which is stored in the _cluster_create_time
    attribute).

    Returns:
      A float representing the creation time in seconds or None.
    """
    return self._cluster_ready_time - self._cluster_create_time

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused

  @property
  def security_group_id(self):
    """Returns the security group ID of this Cluster."""
    return self.network.regional_network.vpc.default_security_group_id

  def _CreateDependencies(self):
    """Set up the ssh key."""
    super(AwsDpbEmr, self)._CreateDependencies()
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _Create(self):
    """Creates the cluster."""
    name = 'pkb_' + FLAGS.run_uri

    # Set up ebs details if disk_spec is present in the config
    ebs_configuration = None
    if self.spec.worker_group.disk_spec:
      # Make sure nothing we are ignoring is included in the disk spec
      assert self.spec.worker_group.disk_spec.device_path is None
      assert self.spec.worker_group.disk_spec.disk_number is None
      assert self.spec.worker_group.disk_spec.provisioned_iops is None
      if self.spec.worker_group.disk_spec.disk_type != disk.LOCAL:
        ebs_configuration = {
            'EbsBlockDeviceConfigs': [{
                'VolumeSpecification': {
                    'SizeInGB': self.spec.worker_group.disk_spec.disk_size,
                    'VolumeType': self.spec.worker_group.disk_spec.disk_type,
                },
                'VolumesPerInstance': self.spec.worker_group.disk_count,
            }]
        }

    # Create the specification for the master and the worker nodes
    instance_groups = []
    core_instances = {
        'InstanceCount': self.spec.worker_count,
        'InstanceGroupType': 'CORE',
        'InstanceType': self.spec.worker_group.vm_spec.machine_type,
    }
    if ebs_configuration:
      core_instances.update({'EbsConfiguration': ebs_configuration})

    master_instance = {
        'InstanceCount': 1,
        'InstanceGroupType': 'MASTER',
        'InstanceType': self.spec.worker_group.vm_spec.machine_type,
    }
    if ebs_configuration:
      master_instance.update({'EbsConfiguration': ebs_configuration})

    instance_groups.append(core_instances)
    instance_groups.append(master_instance)

    # Spark SQL needs to access Hive
    cmd = self.cmd_prefix + [
        'emr',
        'create-cluster',
        '--name',
        name,
        '--release-label',
        self.GetDpbVersion(),
        '--use-default-roles',
        '--instance-groups',
        json.dumps(instance_groups),
        '--application',
        'Name=Spark',
        'Name=Hadoop',
        'Name=Hive',
        '--log-uri',
        self.base_dir,
    ]

    ec2_attributes = [
        'KeyName=' + aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun(),
        'SubnetId=' + self.network.subnet.id,
        # Place all VMs in default security group for simplicity and speed of
        # provisioning
        'EmrManagedMasterSecurityGroup=' + self.security_group_id,
        'EmrManagedSlaveSecurityGroup=' + self.security_group_id,
    ]
    cmd += ['--ec2-attributes', ','.join(ec2_attributes)]

    if self.GetClusterProperties():
      cmd += [
          '--configurations',
          _GetClusterConfiguration(self.GetClusterProperties()),
      ]

    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    self.cluster_id = result['ClusterId']
    logging.info('Cluster created with id %s', self.cluster_id)
    self._AddTags(util.MakeDefaultTags())

  def _AddTags(self, tags_dict: dict[str, str]):
    tag_args = [f'{key}={value}' for key, value in tags_dict.items()]
    cmd = (
        self.cmd_prefix
        + ['emr', 'add-tags', '--resource-id', self.cluster_id, '--tags']
        + tag_args
    )
    try:
      vm_util.IssueCommand(cmd)
    except errors.VmUtil.IssueCommandError as e:
      error_message = str(e)
      if 'ThrottlingException' in error_message:
        raise errors.Benchmarks.QuotaFailure.RateLimitExceededError(
            error_message
        ) from e
      raise

  def _Delete(self):
    if self.cluster_id:
      delete_cmd = self.cmd_prefix + [
          'emr',
          'terminate-clusters',
          '--cluster-ids',
          self.cluster_id,
      ]
      vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _DeleteDependencies(self):
    super(AwsDpbEmr, self)._DeleteDependencies()
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    if not self.cluster_id:
      return False
    cmd = self.cmd_prefix + [
        'emr',
        'describe-cluster',
        '--cluster-id',
        self.cluster_id,
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    result = json.loads(stdout)
    end_datetime = (
        result.get('Cluster', {})
        .get('Status', {})
        .get('Timeline', {})
        .get('EndDateTime')
    )
    if end_datetime is not None:
      self._cluster_delete_time = end_datetime
    if result['Cluster']['Status']['State'] in INVALID_STATES:
      return False
    else:
      return True

  def _IsReady(self):
    """Check to see if the cluster is ready."""
    logging.info('Checking _Ready cluster: %s', self.cluster_id)
    cmd = self.cmd_prefix + [
        'emr',
        'describe-cluster',
        '--cluster-id',
        self.cluster_id,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    # TODO(saksena): Handle error outcomees when spinning up emr clusters
    is_ready = result['Cluster']['Status']['State'] == READY_STATE
    if is_ready:
      self._cluster_create_time, self._cluster_ready_time = (
          self._ParseClusterCreateTime(result)
      )
    return is_ready

  @classmethod
  def _ParseClusterCreateTime(
      cls, data: dict[str, Any]
  ) -> tuple[Optional[float], Optional[float]]:
    """Parses the cluster create & ready time from an API response dict."""
    try:
      creation_ts = data['Cluster']['Status']['Timeline']['CreationDateTime']
      ready_ts = data['Cluster']['Status']['Timeline']['ReadyDateTime']
      return creation_ts, ready_ts
    except (LookupError, TypeError):
      return None, None

  def _GetCompletedJob(self, job_id):
    """See base class."""
    cmd = self.cmd_prefix + [
        'emr',
        'describe-step',
        '--cluster-id',
        self.cluster_id,
        '--step-id',
        job_id,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      if 'ThrottlingException' in stderr:
        logging.warning(
            'Rate limited while polling EMR step:\n%s\nRetrying.', stderr
        )
        return None
      else:
        raise errors.VmUtil.IssueCommandError(
            f'Getting step status failed:\n{stderr}'
        )
    result = json.loads(stdout)
    state = result['Step']['Status']['State']
    if state == 'FAILED':
      raise dpb_service.JobSubmissionError(
          result['Step']['Status']['FailureDetails']
      )
    if state == 'COMPLETED':
      pending_time = result['Step']['Status']['Timeline']['CreationDateTime']
      start_time = result['Step']['Status']['Timeline']['StartDateTime']
      end_time = result['Step']['Status']['Timeline']['EndDateTime']
      return dpb_service.JobResult(
          run_time=end_time - start_time, pending_time=start_time - pending_time
      )

  def SubmitJob(
      self,
      jarfile=None,
      classname=None,
      pyspark_file=None,
      query_file=None,
      job_poll_interval=None,
      job_arguments=None,
      job_files=None,
      job_jars=None,
      job_py_files=None,
      job_stdout_file=None,
      job_type=None,
      properties=None,
  ):
    """See base class."""
    if job_arguments:
      # Escape commas in arguments
      job_arguments = (arg.replace(',', '\\,') for arg in job_arguments)

    all_properties = self.GetJobProperties()
    all_properties.update(properties or {})

    if job_type == 'hadoop':
      if not (jarfile or classname):
        raise ValueError('You must specify jarfile or classname.')
      if jarfile and classname:
        raise ValueError('You cannot specify both jarfile and classname.')
      arg_list = []
      # Order is important
      if classname:
        # EMR does not support passing classnames as jobs. Instead manually
        # invoke `hadoop CLASSNAME` using command-runner.jar
        jarfile = 'command-runner.jar'
        arg_list = ['hadoop', classname]
      # Order is important
      arg_list += ['-D{}={}'.format(k, v) for k, v in all_properties.items()]
      if job_arguments:
        arg_list += job_arguments
      arg_spec = 'Args=[' + ','.join(arg_list) + ']'
      step_list = ['Jar=' + jarfile, arg_spec]
    elif job_type == dpb_constants.SPARK_JOB_TYPE:
      arg_list = []
      if job_files:
        arg_list += ['--files', ','.join(job_files)]
      if job_py_files:
        arg_list += ['--py-files', ','.join(job_py_files)]
      if job_jars:
        arg_list += ['--jars', ','.join(job_jars)]
      for k, v in all_properties.items():
        arg_list += ['--conf', '{}={}'.format(k, v)]
      # jarfile must be last before args
      arg_list += ['--class', classname, jarfile]
      if job_arguments:
        arg_list += job_arguments
      arg_spec = '[' + ','.join(arg_list) + ']'
      step_type_spec = 'Type=Spark'
      step_list = [step_type_spec, 'Args=' + arg_spec]
    elif job_type == dpb_constants.PYSPARK_JOB_TYPE:
      arg_list = []
      if job_files:
        arg_list += ['--files', ','.join(job_files)]
      if job_jars:
        arg_list += ['--jars', ','.join(job_jars)]
      for k, v in all_properties.items():
        arg_list += ['--conf', '{}={}'.format(k, v)]
      # pyspark_file must be last before args
      arg_list += [pyspark_file]
      if job_arguments:
        arg_list += job_arguments
      arg_spec = 'Args=[{}]'.format(','.join(arg_list))
      step_list = ['Type=Spark', arg_spec]
    elif job_type == dpb_constants.SPARKSQL_JOB_TYPE:
      assert not job_arguments
      arg_list = [query_file]
      jar_spec = 'Jar="command-runner.jar"'
      for k, v in all_properties.items():
        arg_list += ['--conf', '{}={}'.format(k, v)]
      arg_spec = 'Args=[spark-sql,-f,{}]'.format(','.join(arg_list))
      step_list = [jar_spec, arg_spec]

    step_string = ','.join(step_list)

    step_cmd = self.cmd_prefix + [
        'emr',
        'add-steps',
        '--cluster-id',
        self.cluster_id,
        '--steps',
        step_string,
    ]
    stdout, _, _ = vm_util.IssueCommand(step_cmd)
    result = json.loads(stdout)
    step_id = result['StepIds'][0]
    return self._WaitForJob(step_id, EMR_TIMEOUT, job_poll_interval)

  def DistributedCopy(self, source, destination, properties=None):
    """Method to copy data using a distributed job on the cluster."""
    job_arguments = ['s3-dist-cp']
    job_arguments.append('--src={}'.format(source))
    job_arguments.append('--dest={}'.format(destination))
    return self.SubmitJob(
        'command-runner.jar',
        job_arguments=job_arguments,
        job_type=dpb_constants.HADOOP_JOB_TYPE,
    )

  def GetHdfsType(self) -> Optional[str]:
    """Gets human friendly disk type for metric metadata."""
    try:
      return disk_to_hdfs_map[self.spec.worker_group.disk_spec.disk_type]
    except KeyError:
      raise errors.Setup.InvalidSetupError(
          f'Invalid disk_type={self.spec.worker_group.disk_spec.disk_type!r} in'
          ' spec.'
      ) from None


class AwsDpbEmrServerless(
    dpb_service.DpbServiceServerlessMixin, dpb_service.BaseDpbService
):
  """Resource that allows spawning EMR Serverless Jobs.

  Pre-initialization capacity is not supported yet.

  Docs:
  https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html
  """

  CLOUD = provider_info.AWS
  SERVICE_TYPE = 'emr_serverless'

  def __init__(self, dpb_service_spec):
    # TODO(odiego): Refactor the AwsDpbEmr and AwsDpbEmrServerless into a
    # hierarchy or move common code to a parent class.
    super().__init__(dpb_service_spec)
    self.project = None
    self.cmd_prefix = list(util.AWS_PREFIX)
    if self.dpb_service_zone:
      self.region = util.GetRegionFromZone(self.dpb_service_zone)
    else:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.'
      )
    self.cmd_prefix += ['--region', self.region]
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(self.region)
    self.persistent_fs_prefix = 's3://'
    self._cluster_create_time = None
    if not self.GetDpbVersion():
      raise errors.Setup.InvalidSetupError(
          'dpb_service.version must be provided. Versions follow the format: '
          '"emr-x.y.z" and are listed at '
          'https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/'
          'release-versions.html'
      )
    self.role = FLAGS.aws_emr_serverless_role

    # Last job usage info
    self._job_run = _AwsDpbEmrServerlessJobRun()
    self._FillMetadata()

  def SubmitJob(
      self,
      jarfile=None,
      classname=None,
      pyspark_file=None,
      query_file=None,
      job_poll_interval=None,
      job_arguments=None,
      job_files=None,
      job_jars=None,
      job_py_files=None,
      job_stdout_file=None,
      job_type=None,
      properties=None,
  ):
    """See base class."""

    assert job_type

    # Set vars according to job type.
    if job_type == dpb_constants.PYSPARK_JOB_TYPE:
      application_type = 'SPARK'
      spark_props = self.GetJobProperties()
      if job_py_files:
        spark_props['spark.submit.pyFiles'] = ','.join(job_py_files)
      job_driver_dict = {
          'sparkSubmit': {
              'entryPoint': pyspark_file,
              'entryPointArguments': job_arguments,
              'sparkSubmitParameters': ' '.join(
                  f'--conf {prop}={val}' for prop, val in spark_props.items()
              ),
          }
      }
    else:
      raise NotImplementedError(
          f'Unsupported job type {job_type} for AWS EMR Serverless.'
      )

    # Create the application.
    stdout, _, _ = vm_util.IssueCommand(
        self.cmd_prefix
        + [
            'emr-serverless',
            'create-application',
            '--release-label',
            self.GetDpbVersion(),
            '--name',
            self.cluster_id,
            '--type',
            application_type,
            '--tags',
            json.dumps(util.MakeDefaultTags()),
        ]
    )
    result = json.loads(stdout)
    application_id = result['applicationId']

    @vm_util.Retry(
        poll_interval=job_poll_interval,
        fuzz=0,
        retryable_exceptions=(EMRRetryableException,),
    )
    def WaitTilApplicationReady():
      result = self._GetApplication(application_id)
      if result['application']['state'] not in ('CREATED', 'STARTED'):
        raise EMRRetryableException(
            f'Application {application_id} not ready yet.'
        )
      return result

    WaitTilApplicationReady()

    # Run the job.
    stdout, _, _ = vm_util.IssueCommand(
        self.cmd_prefix
        + [
            'emr-serverless',
            'start-job-run',
            '--application-id',
            application_id,
            '--execution-role-arn',
            self.role,
            '--job-driver',
            json.dumps(job_driver_dict),
        ]
    )
    result = json.loads(stdout)
    self._job_run = _AwsDpbEmrServerlessJobRun(
        application_id=result['applicationId'],
        job_run_id=result['jobRunId'],
        region=self.region,
    )
    return self._WaitForJob(self._job_run, EMR_TIMEOUT, job_poll_interval)

  def CalculateLastJobCosts(self) -> dpb_service.JobCosts:
    @vm_util.Retry(
        fuzz=0,
        retryable_exceptions=(EMRRetryableException,),
    )
    def WaitTilUsageMetricsAvailable():
      self._CallGetJobRunApi(self._job_run)
      if not self._job_run.HasStats():
        raise EMRRetryableException(
            'Usage metrics not ready yet for EMR Serverless '
            f'application_id={self._job_run.application_id!r} '
            f'job_run_id={self._job_run.job_run_id!r}'
        )

    if not self._job_run:
      return _AwsDpbEmrServerlessJobRun().ComputeJobRunCost()
    if not self._job_run.HasStats():
      try:
        WaitTilUsageMetricsAvailable()
      except vm_util.TimeoutExceededRetryError:
        logging.warning('Timeout exceeded for retrieving usage metrics.')
    return self._job_run.ComputeJobRunCost()

  def GetJobProperties(self) -> Dict[str, str]:
    result = {'spark.dynamicAllocation.enabled': 'FALSE'}
    if self.spec.emr_serverless_core_count:
      result['spark.executor.cores'] = self.spec.emr_serverless_core_count
      result['spark.driver.cores'] = self.spec.emr_serverless_core_count
    if self.spec.emr_serverless_memory:
      result['spark.executor.memory'] = f'{self.spec.emr_serverless_memory}G'
    if self.spec.emr_serverless_executor_count:
      result['spark.executor.instances'] = (
          self.spec.emr_serverless_executor_count
      )
    if self.spec.worker_group.disk_spec.disk_size:
      result['spark.emr-serverless.driver.disk'] = (
          f'{self.spec.worker_group.disk_spec.disk_size}G'
      )
      result['spark.emr-serverless.executor.disk'] = (
          f'{self.spec.worker_group.disk_spec.disk_size}G'
      )
    result.update(super().GetJobProperties())
    return result

  def _GetApplication(self, application_id):
    stdout, _, _ = vm_util.IssueCommand(
        self.cmd_prefix
        + [
            'emr-serverless',
            'get-application',
            '--application-id',
            application_id,
        ]
    )
    result = json.loads(stdout)
    return result

  def _ComputeJobRunCost(
      self, memory_gb_hour: float, storage_gb_hour: float, vcpu_hour: float
  ) -> dpb_service.JobCosts:
    region_prices = aws_dpb_emr_serverless_prices.EMR_SERVERLESS_PRICES.get(
        self.region, {}
    )
    memory_gb_hour_price = region_prices.get('memory_gb_hours')
    storage_gb_hour_price = region_prices.get('storage_gb_hours')
    vcpu_hour_price = region_prices.get('vcpu_hours')
    if (
        memory_gb_hour_price is None
        or storage_gb_hour_price is None
        or vcpu_hour_price is None
    ):
      return dpb_service.JobCosts()
    vcpu_cost = vcpu_hour * vcpu_hour_price
    memory_cost = memory_gb_hour * memory_gb_hour_price
    storage_cost = storage_gb_hour * storage_gb_hour_price
    return dpb_service.JobCosts(
        total_cost=vcpu_cost + memory_cost + storage_cost,
        compute_cost=vcpu_cost,
        memory_cost=memory_cost,
        storage_cost=storage_cost,
        compute_units_used=vcpu_hour,
        memory_units_used=memory_gb_hour,
        storage_units_used=storage_gb_hour,
        compute_unit_cost=vcpu_hour_price,
        memory_unit_cost=memory_gb_hour_price,
        storage_unit_cost=storage_gb_hour_price,
        compute_unit_name='vCPU*hr',
        memory_unit_name='GB*hr',
        storage_unit_name='GB*hr',
    )

  def _GetCompletedJob(self, job_run):
    """See base class."""
    return self._CallGetJobRunApi(job_run)

  def _CallGetJobRunApi(
      self, job_run: _AwsDpbEmrServerlessJobRun
  ) -> Optional[dpb_service.JobResult]:
    """Performs EMR Serverless GetJobRun API call."""
    cmd = self.cmd_prefix + [
        'emr-serverless',
        'get-job-run',
        '--application-id',
        job_run.application_id,
        '--job-run-id',
        job_run.job_run_id,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      if 'ThrottlingException' in stderr:
        logging.warning(
            'Rate limited while polling EMR JobRun:\n%s\nRetrying.', stderr
        )
        return None
      raise errors.VmUtil.IssueCommandError(
          f'Getting JobRun status failed:\n{stderr}'
      )
    result = json.loads(stdout)
    state = result['jobRun']['state']
    if state in ('FAILED', 'CANCELLED'):
      raise dpb_service.JobSubmissionError(result['jobRun'].get('stateDetails'))
    if state == 'SUCCESS':
      start_time = result['jobRun']['createdAt']
      end_time = result['jobRun']['updatedAt']
      self._job_run = self._ParseCostMetrics(result)
      return dpb_service.JobResult(run_time=end_time - start_time)

  def _FillMetadata(self) -> None:
    """Gets a dict to initialize this DPB service instance's metadata."""
    basic_data = self.metadata

    dpb_disk_size = self.spec.worker_group.disk_spec.disk_size or 'default'
    core_count = str(self.spec.emr_serverless_core_count) or 'default'
    cluster_shape = f'emr-serverless-{core_count}'
    cluster_size = str(self.spec.emr_serverless_executor_count) or 'default'

    self.metadata = {
        'dpb_service': basic_data['dpb_service'],
        'dpb_version': basic_data['dpb_version'],
        'dpb_service_version': basic_data['dpb_service_version'],
        'dpb_cluster_shape': cluster_shape,
        'dpb_cluster_size': cluster_size,
        'dpb_hdfs_type': basic_data['dpb_hdfs_type'],
        'dpb_memory_per_node': self.spec.emr_serverless_memory or 'default',
        'dpb_disk_size': dpb_disk_size,
        'dpb_service_zone': basic_data['dpb_service_zone'],
        'dpb_job_properties': basic_data['dpb_job_properties'],
    }

  def GetHdfsType(self) -> Optional[str]:
    """Gets human friendly disk type for metric metadata."""
    return 'default-disk'

  def _ParseCostMetrics(
      self, get_job_run_result: dict[Any, Any]
  ) -> _AwsDpbEmrServerlessJobRun:
    """Parses usage metrics from an EMR s8s GetJobRun API response."""
    resource_utilization = get_job_run_result.get('jobRun', {}).get(
        'totalResourceUtilization', {}
    )
    return dataclasses.replace(
        self._job_run,
        memory_gb_hour=resource_utilization.get('memoryGBHour'),
        storage_gb_hour=resource_utilization.get('storageGBHour'),
        vcpu_hour=resource_utilization.get('vCPUHour'),
    )
