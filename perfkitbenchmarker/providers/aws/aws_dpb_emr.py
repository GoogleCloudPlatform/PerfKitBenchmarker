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

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

import util

FLAGS = flags.FLAGS

SPARK_SAMPLE_LOCATION = ('file:///usr/lib/spark/examples/jars/'
                         'spark-examples.jar')

INVALID_STATES = ['TERMINATED_WITH_ERRORS', 'TERMINATED']
READY_CHECK_SLEEP = 30
READY_CHECK_TRIES = 60
READY_STATE = 'WAITING'
JOB_WAIT_SLEEP = 30


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

        # TODO(saksena): Move this to a configuration value, potentially
        # related to providers' cluster support details
        RELEASE_LABEL = 'emr-5.2.0'

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
                                 '--release-label', RELEASE_LABEL,
                                 '--use-default-roles',
                                 '--instance-groups',
                                 json.dumps(instance_groups),
                                 '--application', 'Name=Spark',
                                 'Name=Hadoop',
                                 '--log-uri', logs_bucket]
        stdout, stderr, _ = vm_util.IssueCommand(cmd)
        result = json.loads(stdout)
        self.cluster_id = result['ClusterId']
        print 'Cluster created with id %s', self.cluster_id


    def _Delete(self):
        delete_cmd = self.cmd_prefix + ['emr',
                                        'terminate-clusters',
                                        '--cluster-ids',
                                        self.cluster_id]
        vm_util.IssueCommand(delete_cmd)


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
        print 'Checking _Ready cluster:', self.cluster_id
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
        if state == "COMPLETED" or state == "FAILED":
            return result
        else:
            return None

    def SubmitJob(self, jarfile, classname, job_poll_interval=5,
                  job_arguments=None, job_stdout_file=None,
                  job_type=None):
        """See base class."""
        @vm_util.Retry(timeout=600,
                       poll_interval=job_poll_interval, fuzz=0)
        def WaitForStep(step_id):
            result = self._IsStepDone(step_id)
            if result is None:
                raise Exception('Step {0} not complete.'.format(step_id))
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
        metrics[dpb_service.SUCCESS] = step_state == "COMPLETED"
        return metrics

    def SetClusterProperty(self):
        pass
