# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing implementation of AWS Batch jobs.

More details at:
- AWS Batch home: https://aws.amazon.com/batch/
- AWS Batch API reference:
  https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""

import json
import time
from typing import Any, Dict, List, Optional

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util as aws_util
from perfkitbenchmarker.resources import base_job
from perfkitbenchmarker.resources import jobs_setter

FLAGS = flags.FLAGS

_FARGATE = 'FARGATE'
_EC2 = 'EC2'

_JOB_SPEC = jobs_setter.BaseJobSpec


class AwsBatchJobsSpec(_JOB_SPEC):
  """Specification for AWS Batch jobs."""

  SERVICE = 'AwsBatchJob'
  CLOUD = 'AWS'
  DEPENDENCIES = frozenset(['network'])


class AwsBatchComputeEnvironment(resource.BaseResource):
  """Manages the AWS Batch Compute Environment.

  A Compute Environment (CE) defines the compute resources (Fargate vCPUs,
  subnets, security groups) used to run containerized jobs.
  """

  def __init__(
      self,
      name: str,
      network: aws_network.AwsNetwork,
      account: str,
      region: str,
  ):
    super().__init__()
    self.name = name
    self.network = network
    self.account = account
    self.region = region
    self.arn: Optional[str] = None

  def _Create(self) -> None:
    assert (
        self.network.subnet is not None
    ), 'Subnet must be created before creating Compute Environment.'
    compute_resources = {
        'subnets': [self.network.subnet.id],
        'securityGroupIds': [
            self.network.regional_network.vpc.default_security_group_id
        ],
        'maxvCpus': 256,
        'type': _FARGATE,
    }
    service_role = f'arn:aws:iam::{self.account}:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch'

    cmd = [
        'aws',
        'batch',
        'create-compute-environment',
        '--compute-environment-name',
        self.name,
        '--type',
        'MANAGED',
        '--state',
        'ENABLED',
        '--compute-resources',
        json.dumps(compute_resources),
        '--service-role',
        service_role,
        '--region',
        self.region,
    ]

    stdout, _, _ = vm_util.IssueCommand(cmd)
    data = json.loads(stdout)
    self.arn = data.get('computeEnvironmentArn')

  @vm_util.Retry(
      poll_interval=10,
      fuzz=0,
      timeout=300,
      retryable_exceptions=(errors.Resource.RetryableDeletionError,),
  )
  def _WaitForDisabled(self) -> None:
    cmd = [
        'aws',
        'batch',
        'describe-compute-environments',
        '--compute-environments',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    envs = json.loads(stdout).get('computeEnvironments', [])
    if envs and not (
        envs[0].get('status') == 'VALID' and envs[0].get('state') == 'DISABLED'
    ):
      raise errors.Resource.RetryableDeletionError()

  def _Delete(self) -> None:
    if not self._Exists():
      return

    update_cmd = [
        'aws',
        'batch',
        'update-compute-environment',
        '--compute-environment',
        self.name,
        '--state',
        'DISABLED',
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(update_cmd, raise_on_failure=False)

    self._WaitForDisabled()

    delete_cmd = [
        'aws',
        'batch',
        'delete-compute-environment',
        '--compute-environment',
        self.name,
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(delete_cmd)

  @vm_util.Retry(
      poll_interval=10,
      fuzz=0,
      timeout=300,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitUntilRunning(self) -> None:
    cmd = [
        'aws',
        'batch',
        'describe-compute-environments',
        '--compute-environments',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    envs = json.loads(stdout).get('computeEnvironments', [])
    if envs[0].get('status') != 'VALID':
      raise errors.Resource.RetryableCreationError()

  def _Exists(self) -> bool:
    cmd = [
        'aws',
        'batch',
        'describe-compute-environments',
        '--compute-environments',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    envs = json.loads(stdout).get('computeEnvironments', [])
    return bool(envs)


class AwsBatchJobQueue(resource.BaseResource):
  """Manages the AWS Batch Job Queue.

  Jobs are submitted to a Job Queue (JQ) and are scheduled onto the
  associated Compute Environment.
  """

  def __init__(
      self, name: str, compute_env: AwsBatchComputeEnvironment, region: str
  ):
    super().__init__()
    self.name = name
    self.compute_env = compute_env
    self.region = region
    self.arn: Optional[str] = None

  def _CreateDependencies(self) -> None:
    self.compute_env.Create()

  def _Create(self) -> None:
    compute_env_order = [{
        'order': 1,
        'computeEnvironment': self.compute_env.arn,
    }]

    cmd = [
        'aws',
        'batch',
        'create-job-queue',
        '--job-queue-name',
        self.name,
        '--state',
        'ENABLED',
        '--priority',
        '1',
        '--compute-environment-order',
        json.dumps(compute_env_order),
        '--region',
        self.region,
    ]

    stdout, _, _ = vm_util.IssueCommand(cmd)
    data = json.loads(stdout)
    self.arn = data.get('jobQueueArn')

  @vm_util.Retry(
      poll_interval=10,
      fuzz=0,
      timeout=300,
      retryable_exceptions=(errors.Resource.RetryableDeletionError,),
  )
  def _WaitForDisabled(self) -> None:
    cmd = [
        'aws',
        'batch',
        'describe-job-queues',
        '--job-queues',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    queues = json.loads(stdout).get('jobQueues', [])
    if queues and not (
        queues[0].get('status') == 'VALID'
        and queues[0].get('state') == 'DISABLED'
    ):
      raise errors.Resource.RetryableDeletionError()

  def _Delete(self) -> None:
    if not self._Exists():
      return

    update_cmd = [
        'aws',
        'batch',
        'update-job-queue',
        '--job-queue',
        self.name,
        '--state',
        'DISABLED',
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(update_cmd, raise_on_failure=False)

    self._WaitForDisabled()

    delete_cmd = [
        'aws',
        'batch',
        'delete-job-queue',
        '--job-queue',
        self.name,
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(delete_cmd)

  @vm_util.Retry(
      poll_interval=10,
      fuzz=0,
      timeout=300,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitUntilRunning(self) -> None:
    cmd = [
        'aws',
        'batch',
        'describe-job-queues',
        '--job-queues',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    queues = json.loads(stdout).get('jobQueues', [])
    if queues[0].get('status') != 'VALID':
      raise errors.Resource.RetryableCreationError()

  def _Exists(self) -> bool:
    cmd = [
        'aws',
        'batch',
        'describe-job-queues',
        '--job-queues',
        self.name,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    queues = json.loads(stdout).get('jobQueues', [])
    return bool(queues)


class AwsBatchJobDefinition(resource.BaseResource):
  """Manages the AWS Batch Job Definition.

  A Job Definition (JD) specifies how a job is to be run, including the
  container image, resource requirements (vCPU, memory), execution roles,
  and platform capabilities.
  """

  def __init__(
      self,
      name: str,
      account: str,
      region: str,
      compute_type: str,
      image: Optional[str] = None,
      execution_role: Optional[str] = None,
  ):
    super().__init__()
    self.name = name
    self.account = account
    self.region = region
    self.compute_type = compute_type
    self.image = image
    self.execution_role = execution_role
    self.arn: Optional[str] = None

  def _Create(self) -> None:

    container_properties = {
        'image': self.image,
        # We must specify resource requirements for the container.
        # Unlike GCP Cloud Batch (which only configures the VM machine type and
        # allows the container to dynamically share all resources), AWS Batch
        # (via ECS) requires explicit container-level resource declarations
        # for scheduling.
        # To ensure the job can be scheduled on a default
        # instance (2 vCPUs, 8 GiB memory), these requirements must be strictly
        # less than the VM's physical capacity to account for ECS agent and OS
        # overhead.
        'resourceRequirements': [
            {'value': '1', 'type': 'VCPU'},
            {'value': '2048', 'type': 'MEMORY'},
        ],
    }

    if self.compute_type == _FARGATE:
      execution_role = self.execution_role or (
          f'arn:aws:iam::{self.account}:role/ecsTaskExecutionRole'
      )
      container_properties.update({
          'executionRoleArn': execution_role,
          'fargatePlatformConfiguration': {
              'platformVersion': 'LATEST',
          },
          'networkConfiguration': {
              'assignPublicIp': 'ENABLED',
          },
      })
    else:
      raise errors.Resource.CreationError(
          f'Unsupported compute type: {self.compute_type}'
      )

    cmd = [
        'aws',
        'batch',
        'register-job-definition',
        '--job-definition-name',
        self.name,
        '--type',
        'container',
        '--container-properties',
        json.dumps(container_properties),
        '--platform-capabilities',
        json.dumps([self.compute_type]),
        '--region',
        self.region,
    ]

    stdout, _, _ = vm_util.IssueCommand(cmd)
    data = json.loads(stdout)
    self.arn = data.get('jobDefinitionArn')

  def _Delete(self) -> None:
    if not self._Exists():
      return

    cmd = [
        'aws',
        'batch',
        'deregister-job-definition',
        '--job-definition',
        self.arn,
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self) -> bool:
    cmd = [
        'aws',
        'batch',
        'describe-job-definitions',
        '--job-definition-name',
        self.name,
        '--status',
        'ACTIVE',
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    defs = json.loads(stdout).get('jobDefinitions', [])
    return bool(defs)


class AwsBatchJob(base_job.BaseJob):
  """Orchestrates the execution of a job on AWS Batch.

  This class manages the high-level lifecycle of the job execution,
  delegating the creation and deletion of the required infrastructure
  (Compute Environment, Job Queue, Job Definition) to their respective
  resource classes.
  """

  SERVICE = 'AwsBatchJob'

  def __init__(self, aws_batch_job_spec, container_registry):
    super().__init__(aws_batch_job_spec, container_registry)
    self.region = aws_batch_job_spec.job_region
    self.zone = FLAGS.zone[0] if FLAGS.zone else f'{self.region}a'
    self.account = aws_util.GetAccount()
    self.compute_type = FLAGS.aws_batch_compute_type.upper()

    network_spec = aws_network.AwsNetworkSpec(zone=self.zone)
    self.network = aws_network.AwsNetwork.GetNetworkFromNetworkSpec(
        network_spec
    )

    self.compute_env = AwsBatchComputeEnvironment(
        name=f'{self.name}-ce',
        network=self.network,
        account=self.account,
        region=self.region,
    )

    self.job_queue = AwsBatchJobQueue(
        name=f'{self.name}-jq',
        compute_env=self.compute_env,
        region=self.region,
    )

    self.job_definition = AwsBatchJobDefinition(
        name=self.name,
        account=self.account,
        region=self.region,
        compute_type=self.compute_type,
        execution_role=FLAGS.aws_batch_execution_role,
    )

    self.job_id: Optional[str] = None
    self.start_timestamp: Optional[float] = None

  def _CreateDependencies(self) -> None:
    """Provisions the prerequisite network, job queue, and job definition."""
    super()._CreateDependencies()
    self.job_definition.image = self.container_image

    self.job_queue.Create()
    self.job_definition.Create()

  def _Create(self) -> None:
    pass

  def _Delete(self) -> None:
    self.job_definition.Delete()
    self.job_queue.Delete()
    self.compute_env.Delete()

  def Execute(self) -> None:
    cmd = [
        'aws',
        'batch',
        'submit-job',
        '--job-name',
        self.name,
        '--job-queue',
        self.job_queue.arn,
        '--job-definition',
        self.job_definition.arn,
        '--region',
        self.region,
    ]

    stdout, _, _ = vm_util.IssueCommand(cmd)
    data = json.loads(stdout)
    self.job_id = data.get('jobId')
    self.submit_timestamp = time.time()

    self._WaitForJobCompletion()

  @vm_util.Retry(
      poll_interval=10,
      timeout=600,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitForJobCompletion(self) -> None:
    """Polls the job status until it reaches a terminal state.

    Raises:
      errors.Resource.CreationError: If the job fails.
      errors.Resource.RetryableCreationError: If the job is still running,
        triggering a retry.
    """
    cmd = [
        'aws',
        'batch',
        'describe-jobs',
        '--jobs',
        self.job_id,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    job_data = json.loads(stdout)['jobs'][0]
    status = job_data.get('status')

    if status == 'SUCCEEDED':
      self.start_timestamp = job_data.get('startedAt', 0) / 1000.0
      return
    elif status == 'FAILED':
      raise errors.Resource.CreationError(
          f'Job failed: {job_data.get("statusReason")}'
      )
    else:
      raise errors.Resource.RetryableCreationError(
          f'Job is in status: {status}'
      )

  def GetMonitoringLink(self) -> str:
    return (
        f'https://{self.region}.console.aws.amazon.com/batch/'
        f'home?region={self.region}#jobs/detail/{self.job_id}'
    )

  def GetResourceMetadata(self) -> Dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'job_compute_type': self.compute_type,
    })
    return metadata

  def GetSamples(self) -> List[sample.Sample]:
    samples = super().GetSamples()
    samples.extend(self.compute_env.GetSamples())
    samples.extend(self.job_queue.GetSamples())
    samples.extend(self.job_definition.GetSamples())
    return samples
