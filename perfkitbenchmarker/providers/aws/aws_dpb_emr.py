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

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

import aws_network
import util

GENERATE_HADOOP_JAR = ('Jar=file:///usr/lib/hadoop-mapreduce/'
                       'hadoop-mapreduce-client-jobclient.jar')

FLAGS = flags.FLAGS
flags.DEFINE_string('dpb_emr_release_label', 'emr-5.2.0',
                    'The emr version to use for the cluster.')

SPARK_SAMPLE_LOCATION = ('file:///usr/lib/spark/examples/jars/'
                         'spark-examples.jar')

INVALID_STATES = ['TERMINATED_WITH_ERRORS', 'TERMINATED']
READY_CHECK_SLEEP = 30
READY_CHECK_TRIES = 60
READY_STATE = 'WAITING'
JOB_WAIT_SLEEP = 30
EMR_TIMEOUT = 3600

MANAGER_SG = 'EmrManagedMasterSecurityGroup'
WORKER_SG = 'EmrManagedSlaveSecurityGroup'


class EMRRetryableException(Exception):
  pass


class AwsSecurityGroup(resource.BaseResource):
  """Object representing a AWS Security Group.

  A security group is created automatically when an Amazon EMR cluster
  is created.  It is not deleted automatically, and the subnet and VPN
  cannot be deleted until the security group is deleted.

  Because of this, there's no _Create method, only a _Delete and an
  _Exists method.
  """

  def __init__(self, cmd_prefix, group_id):
    super(AwsSecurityGroup, self).__init__()
    self.created = True
    self.group_id = group_id
    self.cmd_prefix = cmd_prefix

  def _Delete(self):
    cmd = self.cmd_prefix + ['ec2', 'delete-security-group',
                             '--group-id=' + self.group_id]
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    cmd = self.cmd_prefix + ['ec2', 'describe-security-groups',
                             '--group-id=' + self.group_id]
    _, _, retcode = vm_util.IssueCommand(cmd)
    # if the security group doesn't exist, the describe command gives an error.
    return retcode == 0

  def _Create(self):
    if not self.created:
      raise NotImplemented()


class AwsDpbEmr(dpb_service.BaseDpbService):
  """Object representing a AWS EMR cluster.

  Attributes:
    cluster_id: ID of the cluster.
    project: ID of the project.
  """

  CLOUD = providers.AWS
  SERVICE_TYPE = 'emr'

  def __init__(self, dpb_service_spec):
    super(AwsDpbEmr, self).__init__(dpb_service_spec)
    self.project = None
    self.cmd_prefix = util.AWS_PREFIX

    if FLAGS.zones:
      self.zone = FLAGS.zones[0]
      region = util.GetRegionFromZone(self.zone)
      self.cmd_prefix += ['--region', region]

    self.network = aws_network.AwsNetwork.GetNetwork(self)
    self.bucket_to_delete = None
    self.emr_release_label = FLAGS.dpb_emr_release_label

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused
    pass

  def _CreateLogBucket(self):
    bucket_name = 's3://pkb-{0}-emr'.format(FLAGS.run_uri)
    cmd = self.cmd_prefix + ['s3', 'mb', bucket_name]
    _, _, rc = vm_util.IssueCommand(cmd)
    if rc != 0:
      raise Exception('Error creating logs bucket')
    self.bucket_to_delete = bucket_name
    return bucket_name

  def _Create(self):
    """Creates the cluster."""
    name = 'pkb_' + FLAGS.run_uri

    # Set up ebs details if disk_spec is present int he config
    ebs_configuration = None
    if self.spec.worker_group.disk_spec:
      # Make sure nothing we are ignoring is included in the disk spec
      assert self.spec.worker_group.disk_spec.device_path is None
      assert self.spec.worker_group.disk_spec.disk_number is None
      assert self.spec.worker_group.disk_spec.mount_point is None
      assert self.spec.worker_group.disk_spec.iops is None
      ebs_configuration = {'EbsBlockDeviceConfigs': [
          {'VolumeSpecification': {
              'SizeInGB': self.spec.worker_group.disk_spec.disk_size,
              'VolumeType': self.spec.worker_group.disk_spec.disk_type},
              'VolumesPerInstance':
                  self.spec.worker_group.disk_spec.num_striped_disks}]}

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
    logs_bucket = FLAGS.aws_emr_loguri or self._CreateLogBucket()

    cmd = self.cmd_prefix + ['emr', 'create-cluster', '--name', name,
                             '--release-label', self.emr_release_label,
                             '--use-default-roles',
                             '--instance-groups',
                             json.dumps(instance_groups),
                             '--application', 'Name=Spark',
                             'Name=Hadoop',
                             '--log-uri', logs_bucket]
    if self.network:
      cmd += ['--ec2-attributes', 'SubnetId=' + self.network.subnet.id]

    stdout, stderr, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    self.cluster_id = result['ClusterId']
    logging.info('Cluster created with id %s', self.cluster_id)

  def _DeleteSecurityGroups(self):
    """Delete the security groups associated with this cluster."""
    cmd = self.cmd_prefix + ['emr', 'describe-cluster',
                             '--cluster-id', self.cluster_id]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    cluster_desc = json.loads(stdout)
    sec_object = cluster_desc['Cluster']['Ec2InstanceAttributes']
    manager_sg = sec_object[MANAGER_SG]
    worker_sg = sec_object[WORKER_SG]

    # the manager group and the worker group reference each other, so neither
    # can be deleted.  First we delete the references to the manager group in
    # the worker group.  Then we delete the manager group, and then, finally the
    # worker group.

    # remove all references to the manager group from the worker group.
    for proto, port in [('tcp', '0-65535'), ('udp', '0-65535'), ('icmp', '-1')]:
      for group1, group2 in [(worker_sg, manager_sg), (manager_sg, worker_sg)]:
        cmd = self.cmd_prefix + ['ec2', 'revoke-security-group-ingress',
                                 '--group-id=' + group1,
                                 '--source-group=' + group2,
                                 '--protocol=' + proto,
                                 '--port=' + port]
        vm_util.IssueCommand(cmd)

    # Now we need to delete the manager, then the worker.
    for group in manager_sg, worker_sg:
      sec_group = AwsSecurityGroup(self.cmd_prefix, group)
      sec_group.Delete()


  def _Delete(self):
    delete_cmd = self.cmd_prefix + ['emr',
                                    'terminate-clusters',
                                    '--cluster-ids',
                                    self.cluster_id]
    vm_util.IssueCommand(delete_cmd)

  def _DeleteDependencies(self):
    if self.network:
      self._DeleteSecurityGroups()
    if self.bucket_to_delete:
      bucket_del_cmd = self.cmd_prefix + ['s3', 'rb', '--force',
                                          self.bucket_to_delete]
      vm_util.IssueCommand(bucket_del_cmd)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = self.cmd_prefix + ['emr',
                             'describe-cluster',
                             '--cluster-id',
                             self.cluster_id]
    stdout, _, rc = vm_util.IssueCommand(cmd)
    if rc != 0:
      return False
    result = json.loads(stdout)
    if result['Cluster']['Status']['State'] in INVALID_STATES:
      return False
    else:
      return True

  def _IsReady(self):
    """Check to see if the cluster is ready."""
    logging.info('Checking _Ready cluster:', self.cluster_id)
    cmd = self.cmd_prefix + ['emr',
                             'describe-cluster', '--cluster-id',
                             self.cluster_id]
    stdout, _, rc = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    # TODO(saksena): Handle error outcomees when spinning up emr clusters
    return result['Cluster']['Status']['State'] == READY_STATE


  def _IsStepDone(self, step_id):
    """Determine whether the step is done.

    Args:
      step_id: The step id to query.
    Returns:
      A dictionary describing the step if the step the step is complete,
          None otherwise.
    """

    cmd = self.cmd_prefix + ['emr', 'describe-step', '--cluster-id',
                             self.cluster_id, '--step-id', step_id]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    state = result['Step']['Status']['State']
    if state == 'COMPLETED' or state == 'FAILED':
      return result
    else:
      return None

  def SubmitJob(self, jarfile, classname, job_poll_interval=5,
                job_arguments=None, job_stdout_file=None,
                job_type=None):
    """See base class."""
    @vm_util.Retry(timeout=EMR_TIMEOUT,
                   poll_interval=job_poll_interval, fuzz=0)
    def WaitForStep(step_id):
      result = self._IsStepDone(step_id)
      if result is None:
        raise EMRRetryableException('Step {0} not complete.'.format(step_id))
      return result

    if job_type == 'hadoop':
      step_type_spec = 'Type=CUSTOM_JAR'
      jar_spec = 'Jar=' + jarfile

      # How will we handle a class name ????
      step_list = [step_type_spec, jar_spec]

      if job_arguments:
        arg_spec = '[' + ','.join(job_arguments) + ']'
        step_list.append('Args=' + arg_spec)
    else:
      # assumption: spark job will always have a jar and a class
      arg_list = ['--class', classname, jarfile]
      if job_arguments:
          arg_list += job_arguments
      arg_spec = '[' + ','.join(arg_list) + ']'
      step_type_spec = 'Type=Spark'
      step_list = [step_type_spec, 'Args=' + arg_spec]

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

  def SetClusterProperty(self):
    pass

  def CreateBucket(self, source_bucket):
    mb_cmd = self.cmd_prefix + ['s3', 'mb', source_bucket]
    stdout, _, _ = vm_util.IssueCommand(mb_cmd)

  def generate_data(self, source_dir, udpate_default_fs, num_files,
                    size_file):
    """Method to generate data using a distributed job on the cluster."""
    @vm_util.Retry(timeout=EMR_TIMEOUT,
                   poll_interval=5, fuzz=0)
    def WaitForStep(step_id):
      result = self._IsStepDone(step_id)
      if result is None:
          raise EMRRetryableException('Step {0} not complete.'.format(step_id))
      return result

    job_arguments = ['TestDFSIO']
    if udpate_default_fs:
      job_arguments.append('-Dfs.default.name={}'.format(source_dir))
    job_arguments.append('-Dtest.build.data={}'.format(source_dir))
    job_arguments.extend(['-write', '-nrFiles', str(num_files), '-fileSize',
                          str(size_file)])
    arg_spec = '[' + ','.join(job_arguments) + ']'

    step_type_spec = 'Type=CUSTOM_JAR'
    step_name = 'Name="TestDFSIO"'
    step_action_on_failure = 'ActionOnFailure=CONTINUE'
    jar_spec = GENERATE_HADOOP_JAR

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

    result = WaitForStep(step_id)
    step_state = result['Step']['Status']['State']
    if step_state != 'COMPLETED':
      return {dpb_service.SUCCESS: False}
    else:
      return {dpb_service.SUCCESS: True}

  def read_data(self, source_dir, udpate_default_fs, num_files, size_file):
    """Method to read data using a distributed job on the cluster."""
    @vm_util.Retry(timeout=EMR_TIMEOUT,
                   poll_interval=5, fuzz=0)
    def WaitForStep(step_id):
      result = self._IsStepDone(step_id)
      if result is None:
        raise EMRRetryableException('Step {0} not complete.'.format(step_id))
      return result

    job_arguments = ['TestDFSIO']
    if udpate_default_fs:
      job_arguments.append('-Dfs.default.name={}'.format(source_dir))
    job_arguments.append('-Dtest.build.data={}'.format(source_dir))
    job_arguments.extend(['-read', '-nrFiles', str(num_files), '-fileSize',
                          str(size_file)])
    arg_spec = '[' + ','.join(job_arguments) + ']'

    step_type_spec = 'Type=CUSTOM_JAR'
    step_name = 'Name="TestDFSIO"'
    step_action_on_failure = 'ActionOnFailure=CONTINUE'
    jar_spec = GENERATE_HADOOP_JAR

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

    result = WaitForStep(step_id)
    step_state = result['Step']['Status']['State']
    if step_state != 'COMPLETED':
      return {dpb_service.SUCCESS: False}
    else:
      return {dpb_service.SUCCESS: True}


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

  def cleanup_data(self, base_dir, udpate_default_fs):
    """Method to cleanup data using a distributed job on the cluster."""
    @vm_util.Retry(timeout=EMR_TIMEOUT,
                   poll_interval=5, fuzz=0)
    def WaitForStep(step_id):
      result = self._IsStepDone(step_id)
      if result is None:
          raise EMRRetryableException('Step {0} not complete.'.format(step_id))
      return result

    job_arguments = ['TestDFSIO']
    if udpate_default_fs:
      job_arguments.append('-Dfs.default.name={}'.format(base_dir))
    job_arguments.append('-Dtest.build.data={}'.format(base_dir))
    job_arguments.append('-clean')
    arg_spec = '[' + ','.join(job_arguments) + ']'

    step_type_spec = 'Type=CUSTOM_JAR'
    step_name = 'Name="TestDFSIO"'
    step_action_on_failure = 'ActionOnFailure=CONTINUE'
    jar_spec = GENERATE_HADOOP_JAR

    # How will we handle a class name ????
    step_list = [step_type_spec, step_name, step_action_on_failure, jar_spec
                 ]
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

    result = WaitForStep(step_id)
    step_state = result['Step']['Status']['State']
    if step_state != 'COMPLETED':
      return {dpb_service.SUCCESS: False}
    else:
      rb_step_cmd = self.cmd_prefix + ['s3', 'rb', base_dir, '--force']
      stdout, _, _ = vm_util.IssueCommand(rb_step_cmd)
      return {dpb_service.SUCCESS: True}

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(AwsDpbEmr, self).GetMetadata()
    basic_data['dpb_service'] = 'emr_{}'.format(self.emr_release_label)
    return basic_data
