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
"""Run YCSB benchmark against AWS DynamoDB.

This benchmark does not provision VMs for the corresponding DynamboDB database.
The only VM group is client group that sends requests to specifiedDB.
TODO: add DAX option.
TODO: add global table option.
"""

from collections.abc import Collection, Iterable, MutableMapping
import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import aws_credentials
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.aws import aws_dynamodb

_INITIAL_WRITES = flags.DEFINE_integer(
    'aws_dynamodb_ycsb_provision_wcu',
    10000,
    'The provisioned WCU to use during the load phase.',
)
flags.register_validator(
    'aws_dynamodb_ycsb_provision_wcu',
    lambda wcu: wcu >= 1,
    message='WCU must be >=1 to load successfully.',
)
_CONSISTENT_READS = flags.DEFINE_boolean(
    'aws_dynamodb_ycsb_consistentReads',
    False,
    'Consistent reads cost 2x eventual reads. '
    "'false' is default which is eventual",
)
_MAX_CONNECTIONS = flags.DEFINE_integer(
    'aws_dynamodb_connectMax',
    50,
    'Maximum number of concurrent dynamodb connections. Defaults to 50.',
)
_RAMP_UP = flags.DEFINE_boolean(
    'aws_dynamodb_ycsb_ramp_up',
    False,
    'If true, runs YCSB with a target throughput equal to the provisioned qps '
    'of the instance and increments until a max throughput is found.',
)
_PROVISIONED_QPS = flags.DEFINE_boolean(
    'aws_dynamodb_ycsb_provisioned_qps',
    False,
    'If true, runs YCSB with a target throughput equal to the provisioned qps '
    'of the instance and returns the result.',
)
_CLI_PROFILE = flags.DEFINE_string(
    'aws_dynamodb_ycsb_cli_profile',
    None,
    'Local AWS CLI profile to use with YCSB. Must be long term crendentials. '
    '"default" will work with basic CLI set up. '
    'Using an IAM user that only has DynamoDB access limits the scope of '
    'credentials copied into the VM.',
)
_RESTORE_VM_COUNT = flags.DEFINE_integer(
    'aws_dynamodb_ycsb_restore_vm_count',
    None,
    'Number of VMs to use for the benchmark if the restore flag is set. This is'
    ' used mostly as an override and should not generally be used manually. Use'
    ' --ycsb_client_vms instead to set the VMs for a normal run.',
)
FLAGS = flags.FLAGS

_TARGET_QPS_INCREMENT = 1000
_QPS_THRESHOLD = 100

BENCHMARK_NAME = 'aws_dynamodb_ycsb'
BENCHMARK_CONFIG = """
aws_dynamodb_ycsb:
  description: >
      Run YCSB against AWS DynamoDB.
      Configure the number of VMs via --ycsb_client_vms.
  non_relational_db:
    service_type: dynamodb
    zone: us-east-1a
    enable_freeze_restore: True
  vm_groups:
    default:
      os_type: ubuntu2204  # Python 2
      vm_spec: *default_single_core
      vm_count: 1
  flags:
    openjdk_version: 8"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  # Override the VM count for restore runs (i.e. when some do not need multiple
  # VMs for loading).
  if FLAGS['restore'].present and _RESTORE_VM_COUNT.value is not None:
    config['vm_groups']['default']['vm_count'] = _RESTORE_VM_COUNT.value
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config
  if not _CLI_PROFILE.value:
    raise errors.Config.MissingOption(
        '--aws_dynamodb_ycsb_cli_profile must be explicitly passed. "default" '
        'will use the default CLI profile and works with basic long lived '
        'credentials. Using DynamoDB specific credentials limits what is '
        'copied into the VM.'
    )
  ycsb.CheckPrerequisites()


def _GetYcsbArgs(
    client: virtual_machine.VirtualMachine,
    instance: aws_dynamodb.AwsDynamoDBInstance,
) -> dict[str, Any]:
  """Returns args to pass to YCSB."""
  run_kwargs = {
      'dynamodb.awsCredentialsFile': GetRemoteCredentialsFullPath(client),
      'dynamodb.primaryKey': instance.primary_key,
      'dynamodb.endpoint': instance.GetEndPoint(),
      'dynamodb.region': instance.region,
      'table': instance.table_name,
  }
  if FLAGS.aws_dynamodb_use_sort:
    run_kwargs.update({
        'dynamodb.primaryKeyType': 'HASH_AND_RANGE',
        'aws_dynamodb_connectMax': _MAX_CONNECTIONS.value,
        'dynamodb.hashKeyName': instance.primary_key,
        'dynamodb.primaryKey': instance.sort_key,
    })
  if _CONSISTENT_READS.value:
    run_kwargs['dynamodb.consistentReads'] = 'true'
  return run_kwargs


def Prepare(benchmark_spec):
  """Install YCSB on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  vms = benchmark_spec.vms
  # Install required packages.
  background_tasks.RunThreaded(_Install, vms)
  benchmark_spec.executor = ycsb.YCSBExecutor('dynamodb')

  instance: aws_dynamodb.AwsDynamoDBInstance = benchmark_spec.non_relational_db

  # Restored instances have already been loaded with data.
  if instance.restored:
    return

  load_kwargs = _GetYcsbArgs(vms[0], instance)
  if FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = FLAGS.ycsb_preload_threads
  # More WCU results in a faster load stage.
  if instance.wcu < _INITIAL_WRITES.value and not instance.IsServerless():
    instance.SetThroughput(wcu=_INITIAL_WRITES.value)
  benchmark_spec.executor.Load(vms, load_kwargs=load_kwargs)
  # Reset the WCU to the initial level.
  if not instance.IsServerless():
    instance.SetThroughput()


def Run(benchmark_spec):
  """Run YCSB on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  instance: aws_dynamodb.AwsDynamoDBInstance = benchmark_spec.non_relational_db

  run_kwargs = _GetYcsbArgs(vms[0], instance)
  samples = []
  if _RAMP_UP.value:
    samples += RampUpRun(benchmark_spec.executor, instance, vms, run_kwargs)
  elif _PROVISIONED_QPS.value:
    samples += ExactThroughputRun(
        benchmark_spec.executor, instance, vms, run_kwargs
    )
  else:
    samples += list(benchmark_spec.executor.Run(vms, run_kwargs=run_kwargs))
  benchmark_metadata = {
      'ycsb_client_vms': len(vms),
      'aws_dynamodb_consistentReads': _CONSISTENT_READS.value,
      'aws_dynamodb_connectMax': _MAX_CONNECTIONS.value,
  }
  for s in samples:
    s.metadata.update(instance.GetResourceMetadata())
    s.metadata.update(benchmark_metadata)
  return samples


def _ExtractThroughput(samples: Iterable[sample.Sample]) -> float:
  for result in samples:
    if result.metric == 'overall Throughput':
      return result.value
  return 0.0


def _GetTargetQps(db: aws_dynamodb.AwsDynamoDBInstance) -> int:
  """Gets the target QPS for eventual and strong reads."""
  read_multiplier = 1 if _CONSISTENT_READS.value else 2
  rcu = 0 if db.rcu <= 25 else db.rcu * read_multiplier
  wcu = 0 if db.wcu <= 25 else db.wcu
  return rcu + wcu


def RampUpRun(
    executor: ycsb.YCSBExecutor,
    db: aws_dynamodb.AwsDynamoDBInstance,
    vms: Collection[virtual_machine.VirtualMachine],
    run_kwargs: MutableMapping[str, Any],
) -> list[sample.Sample]:
  """Runs YCSB starting from provisioned QPS until max throughput is found."""
  # Database is already provisioned with the correct QPS.
  qps = _GetTargetQps(db)
  max_throughput = 0
  while True:
    run_kwargs['target'] = qps
    run_samples = executor.Run(vms, run_kwargs=run_kwargs)
    throughput = _ExtractThroughput(run_samples)
    logging.info(
        'Run had throughput target %s and measured throughput %s.',
        qps,
        throughput,
    )

    if throughput < max_throughput + _QPS_THRESHOLD:
      logging.info('Found maximum throughput %s.', throughput)
      for s in run_samples:
        s.metadata['qps_threshold'] = _QPS_THRESHOLD
        s.metadata['ramp_up'] = True
      return run_samples

    max_throughput = throughput
    qps += _TARGET_QPS_INCREMENT


def ExactThroughputRun(
    executor: ycsb.YCSBExecutor,
    db: aws_dynamodb.AwsDynamoDBInstance,
    vms: Collection[virtual_machine.VirtualMachine],
    run_kwargs: MutableMapping[str, Any],
) -> list[sample.Sample]:
  """Runs YCSB at provisioned QPS."""
  run_kwargs['target'] = _GetTargetQps(db)
  run_samples = executor.Run(vms, run_kwargs=run_kwargs)
  for s in run_samples:
    s.metadata['provisioned_qps'] = True
  return run_samples


def Cleanup(benchmark_spec):
  """Cleanup YCSB on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec


AWS_CREDENTIALS_FILE = 'aws_credentials.properties'


def GetRemoteCredentialsFullPath(vm):
  """Returns the full path for the generated AWS credentials file."""
  result, _ = vm.RemoteCommand(f'echo ~/{AWS_CREDENTIALS_FILE}')
  return result.strip()


def GenerateCredentials(vm) -> None:
  """Generates AWS credentials properties file and pushes it to the VM."""
  key_id, secret = aws_credentials.GetCredentials(profile=_CLI_PROFILE.value)

  with vm_util.NamedTemporaryFile(prefix=AWS_CREDENTIALS_FILE, mode='w') as f:
    f.write(f"""
accessKey={key_id}
secretKey={secret}
""")
    f.close()
    vm.PushFile(f.name, GetRemoteCredentialsFullPath(vm))


def _Install(vm):
  """Install YCSB on client 'vm'."""
  vm.Install('ycsb')
  GenerateCredentials(vm)
