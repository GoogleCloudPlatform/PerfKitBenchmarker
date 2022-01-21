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

import os
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.aws import aws_dynamodb

_INITIAL_WRITES = flags.DEFINE_integer(
    'aws_dynamodb_ycsb_provision_wcu', 10000,
    'The provisioned WCU to use during the load phase.')
flags.register_validator(
    'aws_dynamodb_ycsb_provision_wcu',
    lambda wcu: wcu >= 1,
    message='WCU must be >=1 to load successfully.')
_CONSISTENT_READS = flags.DEFINE_boolean(
    'aws_dynamodb_ycsb_consistentReads', False,
    'Consistent reads cost 2x eventual reads. '
    "'false' is default which is eventual")
_MAX_CONNECTIONS = flags.DEFINE_integer(
    'aws_dynamodb_connectMax', 50,
    'Maximum number of concurrent dynamodb connections. '
    'Defaults to 50.')
FLAGS = flags.FLAGS

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
      vm_spec: *default_single_core
      vm_count: 1"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config
  ycsb.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Install YCSB on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  vms = benchmark_spec.vms
  # Install required packages.
  vm_util.RunThreaded(_Install, vms)
  benchmark_spec.executor = ycsb.YCSBExecutor('dynamodb')


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

  run_kwargs = {
      'dynamodb.awsCredentialsFile': GetRemoteVMCredentialsFullPath(vms[0]),
      'dynamodb.primaryKey': instance.primary_key,
      'dynamodb.endpoint': instance.GetEndPoint(),
      'table': 'pkb-{0}'.format(FLAGS.run_uri),
  }
  if FLAGS.aws_dynamodb_use_sort:
    run_kwargs.update({
        'dynamodb.primaryKeyType': 'HASH_AND_RANGE',
        'aws_dynamodb_connectMax': _MAX_CONNECTIONS.value,
        'dynamodb.hashKeyName': instance.primary_key,
        'dynamodb.primaryKey': instance.sort_key
    })
  if _CONSISTENT_READS.value:
    run_kwargs.update({'dynamodb.consistentReads': 'true'})
  load_kwargs = run_kwargs.copy()
  if FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = FLAGS.ycsb_preload_threads
  # More WCU results in a faster load stage.
  instance.SetThroughput(wcu=_INITIAL_WRITES.value)
  samples = list(benchmark_spec.executor.Load(vms, load_kwargs=load_kwargs))
  # Reset the WCU to the initial level.
  instance.SetThroughput()
  samples += list(benchmark_spec.executor.Run(vms, run_kwargs=run_kwargs))
  benchmark_metadata = {
      'ycsb_client_vms': len(vms),
      'aws_dynamodb_consistentReads': _CONSISTENT_READS.value,
      'aws_dynamodb_connectMax': _MAX_CONNECTIONS.value,
  }
  for sample in samples:
    sample.metadata.update(instance.GetResourceMetadata())
    sample.metadata.update(benchmark_metadata)
  return samples


def Cleanup(benchmark_spec):
  """Cleanup YCSB on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec


def GetRemoteVMCredentialsFullPath(vm):
  """Returns the full path for first AWS credentials file found."""
  home_dir, _ = vm.RemoteCommand('echo ~')
  search_path = os.path.join(
      home_dir.rstrip('\n'), FLAGS.aws_credentials_remote_path)
  result, _ = vm.RemoteCommand('grep -irl "key" {0}'.format(search_path))
  return result.strip('\n').split('\n')[0]


def _Install(vm):
  """Install YCSB on client 'vm'."""
  vm.Install('ycsb')
  # copy AWS creds
  vm.Install('aws_credentials')
  # aws credentials file format to ycsb recognized format
  vm.RemoteCommand('sed -i "s/aws_access_key_id/accessKey/g" {0}'.format(
      GetRemoteVMCredentialsFullPath(vm)))
  vm.RemoteCommand('sed -i "s/aws_secret_access_key/secretKey/g" {0}'.format(
      GetRemoteVMCredentialsFullPath(vm)))
