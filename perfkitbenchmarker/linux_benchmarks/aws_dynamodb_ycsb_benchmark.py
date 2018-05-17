# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
Before running this benchmark, you have to manually create `usertable` with primaryKey 'user02'.
TODO: add RANGE option.
TODO: add DAX option.
TODO: add global table option.
"""

import logging
import posixpath
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.aws import aws_dynamodb

BENCHMARK_NAME = 'aws_dynamodb_ycsb'
BENCHMARK_CONFIG = """
aws_dynamodb_ycsb:
  description: >
      Run YCSB against AWS DynamoDB.
      Configure the number of VMs via --ycsb_client_vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1"""

YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'dynamodb-binding', 'lib')
AWS_CREDENTIAL_DIR = '/tmp/AWSCredentials.properties'
FLAGS = flags.FLAGS

flags.DEFINE_string('aws_dynamodb_ycsb_awscredentials_properties',
                    './AWSCredentials.properties',
                    'The AWS credential location. Defaults to PKB top folder')
flags.DEFINE_string('aws_dynamodb_ycsb_dynamodb_primarykey',
                    'user01',
                    'The primaryKey of dynamodb table.')
flags.DEFINE_string('aws_dynamodb_ycsb_dynamodb_region',
                    'us-east-1',
                    'The AWS dynamodb region to connect to.')
flags.DEFINE_string('aws_dynamodb_ycsb_readproportion',
                    '0.5',
                    'The read proportion, '
                    'default is 0.5 in workloada and 0.95 in YCSB.')
flags.DEFINE_string('aws_dynamodb_ycsb_updateproportion',
                    '0.5',
                    'The update proportion, '
                    'default is 0.5 in workloada and 0.05 in YCSB')
flags.DEFINE_string('aws_dynamodb_ycsb_table',
                    'ycsb',
                    'The dynamodb table name precursor.')
flags.DEFINE_enum('aws_dynamodb_ycsb_consistentReads',
                  None, ['false', 'true'],
                  "Consistent reads cost 2x eventual reads. "
                  "'false' is default which is eventual")
flags.DEFINE_string('aws_dynamodb_ycsb_capacity',
                    'ReadCapacityUnits=5,WriteCapacityUnits=5',
                    'Set RCU/WCU for dynamodb table')


def GetConfig(user_config):
    config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
    if FLAGS['ycsb_client_vms'].present:
        config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
    return config


def CheckPrerequisites(benchmark_config):
    """Verifies that the required resources are present.
    Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
    """
    ycsb.CheckPrerequisites()


def Prepare(benchmark_spec):
    """Install YCSB on the target vm.
    Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
    """
    benchmark_spec.always_call_cleanup = True
    benchmark_spec.dynamodb_instance = aws_dynamodb.AwsDynamoDBInstance(
        region=FLAGS.aws_dynamodb_ycsb_dynamodb_region,
        table_name=FLAGS.aws_dynamodb_ycsb_table + "_" + FLAGS.run_uri,
        primary_key=FLAGS.aws_dynamodb_ycsb_dynamodb_primarykey,
        throughput=FLAGS.aws_dynamodb_ycsb_capacity)
    if benchmark_spec.dynamodb_instance._Exists():
      logging.warning('DynamoDB table %s exists, delete it first.' %
                      FLAGS.aws_dynamodb_ycsb_table + "_" + FLAGS.run_uri)
    benchmark_spec.dynamodb_instance.Delete()
    benchmark_spec.dynamodb_instance.Create()
    if not benchmark_spec.dynamodb_instance._Exists():
      logging.warning('Failed to create DynamoDB table.')
    benchmark_spec.dynamodb_instance.Delete()
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

    run_kwargs = {
        'dynamodb.awsCredentialsFile': AWS_CREDENTIAL_DIR,
        'dynamodb.primaryKey': FLAGS.aws_dynamodb_ycsb_dynamodb_primarykey,
        'dynamodb.endpoint': 'http://dynamodb.' +
                             FLAGS.aws_dynamodb_ycsb_dynamodb_region +
                             '.amazonaws.com',
        'readproportion': FLAGS.aws_dynamodb_ycsb_readproportion,
        'updateproportion': FLAGS.aws_dynamodb_ycsb_updateproportion,
        'table': FLAGS.aws_dynamodb_ycsb_table + "_" + FLAGS.run_uri,
        'dynamodb.consistentReads': FLAGS.aws_dynamodb_ycsb_consistentReads,
    }
    load_kwargs = run_kwargs.copy()
    if FLAGS['ycsb_preload_threads'].present:
        load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
    samples = list(benchmark_spec.executor.LoadAndRun(
        vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))
    return samples


def Cleanup(benchmark_spec):
    """Cleanup YCSB on the target vm.
    Args:
    benchmark_spec: The benchmark specification. Contains all data that is
    required to run the benchmark.
    """
    benchmark_spec.dynamodb_instance.Delete()


def _Install(vm):
    """Install YCSB on client 'vm'."""
    vm.Install('ycsb')
    # copy AWS creds
    vm.RemoteCopy(FLAGS.aws_dynamodb_ycsb_awscredentials_properties, AWS_CREDENTIAL_DIR)
