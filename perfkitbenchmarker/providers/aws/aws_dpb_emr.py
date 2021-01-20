# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS's EMR service.

Clusters can be created and deleted.
"""

import json
import logging

from absl import flags
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import aws
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
flags.DEFINE_string('dpb_emr_release_label', None,
                    'DEPRECATED use dpb_service.version.')

SPARK_SAMPLE_LOCATION = 'file:///usr/lib/spark/examples/jars/spark-examples.jar'

INVALID_STATES = ['TERMINATED_WITH_ERRORS', 'TERMINATED']
READY_CHECK_SLEEP = 30
READY_CHECK_TRIES = 60
READY_STATE = 'WAITING'
JOB_WAIT_SLEEP = 30
EMR_TIMEOUT = 14400

disk_to_hdfs_map = {
    'st1': 'HDD',
    'gp2': 'SSD'
}


class EMRRetryableException(Exception):
  pass


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

  CLOUD = aws.CLOUD
  SERVICE_TYPE = 'emr'
  PERSISTENT_FS_PREFIX = 's3://'

  def __init__(self, dpb_service_spec):
    super(AwsDpbEmr, self).__init__(dpb_service_spec)
    self.dpb_service_type = AwsDpbEmr.SERVICE_TYPE
    self.project = None
    self.cmd_prefix = list(util.AWS_PREFIX)
    if self.dpb_service_zone:
      self.region = util.GetRegionFromZone(self.dpb_service_zone)
    else:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.')
    self.cmd_prefix += ['--region', self.region]
    self.network = aws_network.AwsNetwork.GetNetworkFromNetworkSpec(
        aws_network.AwsNetworkSpec(zone=self.dpb_service_zone))
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(self.region)
    self.bucket_to_delete = None
    self.dpb_version = FLAGS.dpb_emr_release_label or self.dpb_version
    if not self.dpb_version:
      raise errors.Setup.InvalidSetupError(
          'dpb_service.version must be provided.')

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused

  @property
  def security_group_id(self):
    """Returns the security group ID of this Cluster."""
    return self.network.regional_network.vpc.default_security_group_id

  def _CreateLogBucket(self):
    """Create the s3 bucket for the EMR cluster's logs."""
    log_bucket_name = 'pkb-{0}-emr'.format(FLAGS.run_uri)
    self.storage_service.MakeBucket(log_bucket_name)
    return 's3://{}'.format(log_bucket_name)

  def _DeleteLogBucket(self):
    """Delete the s3 bucket holding the EMR cluster's logs.

    This method is part of the Delete lifecycle of the resource.
    """
    # TODO(saksena): Deprecate the use of FLAGS.run_uri and plumb as argument.
    log_bucket_name = 'pkb-{0}-emr'.format(FLAGS.run_uri)
    self.storage_service.DeleteBucket(log_bucket_name)

  def _CreateDependencies(self):
    """Set up the ssh key."""
    aws_virtual_machine.AwsKeyFileManager.ImportKeyfile(self.region)

  def _Create(self):
    """Creates the cluster."""
    name = 'pkb_' + FLAGS.run_uri

    # Set up ebs details if disk_spec is present int he config
    ebs_configuration = None
    if self.spec.worker_group.disk_spec:
      # Make sure nothing we are ignoring is included in the disk spec
      assert self.spec.worker_group.disk_spec.device_path is None
      assert self.spec.worker_group.disk_spec.disk_number is None
      assert self.spec.worker_group.disk_spec.iops is None
      ebs_configuration = {'EbsBlockDeviceConfigs': [
          {'VolumeSpecification': {
              'SizeInGB': self.spec.worker_group.disk_spec.disk_size,
              'VolumeType': self.spec.worker_group.disk_spec.disk_type},
           'VolumesPerInstance': self.spec.worker_group.disk_count}]}
      self.dpb_hdfs_type = disk_to_hdfs_map[
          self.spec.worker_group.disk_spec.disk_type]

    # Create the specification for the master and the worker nodes
    instance_groups = []
    core_instances = {'InstanceCount': self.spec.worker_count,
                      'InstanceGroupType': 'CORE',
                      'InstanceType':
                          self.spec.worker_group.vm_spec.machine_type}
    if ebs_configuration:
      core_instances.update({'EbsConfiguration': ebs_configuration})

    master_instance = {'InstanceCount': 1,
                       'InstanceGroupType': 'MASTER',
                       'InstanceType':
                           self.spec.worker_group.vm_spec.machine_type}
    if ebs_configuration:
      master_instance.update({'EbsConfiguration': ebs_configuration})

    instance_groups.append(core_instances)
    instance_groups.append(master_instance)

    # Create the log bucket to hold job's log output
    # TODO(saksena): Deprecate aws_emr_loguri flag and move
    # the log bucket creation to Create dependencies.
    logs_bucket = self._CreateLogBucket()

    # Spark SQL needs to access Hive
    cmd = self.cmd_prefix + ['emr', 'create-cluster', '--name', name,
                             '--release-label', self.dpb_version,
                             '--use-default-roles',
                             '--instance-groups',
                             json.dumps(instance_groups),
                             '--application', 'Name=Spark',
                             'Name=Hadoop', 'Name=Hive',
                             '--log-uri', logs_bucket]

    ec2_attributes = [
        'KeyName=' + aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun(),
        'SubnetId=' + self.network.subnet.id,
        # Place all VMs in default security group for simplicity and speed of
        # provisioning
        'EmrManagedMasterSecurityGroup=' + self.security_group_id,
        'EmrManagedSlaveSecurityGroup=' + self.security_group_id,
    ]
    cmd += ['--ec2-attributes', ','.join(ec2_attributes)]

    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    self.cluster_id = result['ClusterId']
    logging.info('Cluster created with id %s', self.cluster_id)
    for tag_key, tag_value in util.MakeDefaultTags().items():
      self._AddTag(tag_key, tag_value)

  def _AddTag(self, key, value):
    cmd = self.cmd_prefix + ['emr', 'add-tags',
                             '--resource-id', self.cluster_id,
                             '--tag',
                             '{}={}'.format(key, value)]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    if self.cluster_id:
      delete_cmd = self.cmd_prefix + ['emr',
                                      'terminate-clusters',
                                      '--cluster-ids',
                                      self.cluster_id]
      vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _DeleteDependencies(self):
    self._DeleteLogBucket()
    aws_virtual_machine.AwsKeyFileManager.DeleteKeyfile(self.region)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    if not self.cluster_id:
      return False
    cmd = self.cmd_prefix + ['emr',
                             'describe-cluster',
                             '--cluster-id',
                             self.cluster_id]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    result = json.loads(stdout)
    if result['Cluster']['Status']['State'] in INVALID_STATES:
      return False
    else:
      return True

  def _IsReady(self):
    """Check to see if the cluster is ready."""
    logging.info('Checking _Ready cluster: %s', self.cluster_id)
    cmd = self.cmd_prefix + ['emr',
                             'describe-cluster', '--cluster-id',
                             self.cluster_id]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    # TODO(saksena): Handle error outcomees when spinning up emr clusters
    return result['Cluster']['Status']['State'] == READY_STATE

  def _GetCompletedJob(self, job_id):
    """See base class."""
    cmd = self.cmd_prefix + [
        'emr', 'describe-step', '--cluster-id', self.cluster_id, '--step-id',
        job_id
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    state = result['Step']['Status']['State']
    if state == 'FAILED':
      raise dpb_service.JobSubmissionError(
          result['Step']['Status']['FailureDetails'])
    if state == 'COMPLETED':
      pending_time = result['Step']['Status']['Timeline']['CreationDateTime']
      start_time = result['Step']['Status']['Timeline']['StartDateTime']
      end_time = result['Step']['Status']['Timeline']['EndDateTime']
      return dpb_service.JobResult(
          run_time=end_time - start_time,
          pending_time=start_time - pending_time)

  def SubmitJob(self,
                jarfile=None,
                classname=None,
                pyspark_file=None,
                query_file=None,
                job_poll_interval=5,
                job_arguments=None,
                job_files=None,
                job_jars=None,
                job_stdout_file=None,
                job_type=None,
                properties=None):
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
    elif job_type == self.SPARK_JOB_TYPE:
      arg_list = []
      if job_files:
        arg_list += ['--files', ','.join(job_files)]
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
    elif job_type == self.PYSPARK_JOB_TYPE:
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
    elif job_type == self.SPARKSQL_JOB_TYPE:
      assert not job_arguments
      arg_list = [query_file]
      jar_spec = 'Jar="command-runner.jar"'
      for k, v in all_properties.items():
        arg_list += ['--conf', '{}={}'.format(k, v)]
      arg_spec = 'Args=[spark-sql,-f,{}]'.format(','.join(arg_list))
      step_list = [jar_spec, arg_spec]

    step_string = ','.join(step_list)

    step_cmd = self.cmd_prefix + ['emr',
                                  'add-steps',
                                  '--cluster-id',
                                  self.cluster_id,
                                  '--steps',
                                  step_string]
    stdout, _, _ = vm_util.IssueCommand(step_cmd)
    result = json.loads(stdout)
    step_id = result['StepIds'][0]
    return self._WaitForJob(step_id, EMR_TIMEOUT, job_poll_interval)

  def distributed_copy(self, source_location, destination_location):
    """Method to copy data using a distributed job on the cluster."""
    @vm_util.Retry(timeout=EMR_TIMEOUT,
                   poll_interval=5, fuzz=0)
    def WaitForStep(step_id):
      result = self._IsStepDone(step_id)
      if result is None:
        raise EMRRetryableException('Step {0} not complete.'.format(step_id))
      return result

    job_arguments = ['s3-dist-cp', '--s3Endpoint=s3.amazonaws.com']
    job_arguments.append('--src={}'.format(source_location))
    job_arguments.append('--dest={}'.format(destination_location))
    arg_spec = '[' + ','.join(job_arguments) + ']'

    step_type_spec = 'Type=CUSTOM_JAR'
    step_name = 'Name="S3DistCp"'
    step_action_on_failure = 'ActionOnFailure=CONTINUE'
    jar_spec = 'Jar=command-runner.jar'

    step_list = [step_type_spec, step_name, step_action_on_failure, jar_spec]
    step_list.append('Args=' + arg_spec)
    step_string = ','.join(step_list)

    step_cmd = self.cmd_prefix + ['emr',
                                  'add-steps',
                                  '--cluster-id',
                                  self.cluster_id,
                                  '--steps',
                                  step_string]
    stdout, _, _ = vm_util.IssueCommand(step_cmd)
    result = json.loads(stdout)
    step_id = result['StepIds'][0]
    metrics = {}

    result = WaitForStep(step_id)
    pending_time = result['Step']['Status']['Timeline']['CreationDateTime']
    start_time = result['Step']['Status']['Timeline']['StartDateTime']
    end_time = result['Step']['Status']['Timeline']['EndDateTime']
    metrics[dpb_service.WAITING] = start_time - pending_time
    metrics[dpb_service.RUNTIME] = end_time - start_time
    step_state = result['Step']['Status']['State']
    metrics[dpb_service.SUCCESS] = step_state == 'COMPLETED'
    return metrics
