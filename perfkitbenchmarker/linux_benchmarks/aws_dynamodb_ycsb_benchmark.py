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

Before running this benchmark, you have to manually create `usertable` with primaryKey 'user01'.
TODO: add DynamoDB provider.

"""

import logging
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb


BENCHMARK_NAME = 'aws_dynamodb_ycsb'
BENCHMARK_CONFIG = """
aws_dynamodb_ycsb:
  description: >
      Run YCSB against AWS DynamoDB.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1"""

YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'dynamodb-binding', 'lib')
AWS_CREDENTIAL_DIR = '/tmp/AWSCredentials.properties'

FLAGS = flags.FLAGS
flags.DEFINE_string('aws_dynamodb_ycsb_awscredentials_properties',
                    None,
                    'The AWS credential location.')
flags.DEFINE_string('aws_dynamodb_ycsb_dynamodb_primarykey',
                    None,
                    'The primaryKey of usertable.')
flags.DEFINE_string('aws_dynamodb_ycsb_dynamodb_endpoint',
                    None,
                    'The aws endpoint to connect to.')
flags.DEFINE_string('aws_dynamodb_ycsb_requestdistribution',
                    'uniform',
                    'Type of request distribution.')


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
    if not FLAGS.aws_dynamodb_ycsb_awscredentials_properties:
        raise ValueError('"aws_dynamodb_ycsb_awscredentials_properties" must be set')
    if not FLAGS.aws_dynamodb_ycsb_dynamodb_primarykey:
        raise ValueError('"aws_dynamodb_ycsb_dynamodb_primarykey" must be set')
    if not FLAGS.aws_dynamodb_ycsb_dynamodb_endpoint:
        raise ValueError('"aws_dynamodb_ycsb_dynamodb_endpoint" must be set ')


def Prepare(benchmark_spec):
    """Install YCSB on the target vm.

    Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
    """
    benchmark_spec.always_call_cleanup = True
    vms = benchmark_spec.vms

    # Install required packages and copy credential files.
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
        'dynamodb.endpoint': FLAGS.aws_dynamodb_ycsb_dynamodb_endpoint,
        'requestdistribution': FLAGS.aws_dynamodb_ycsb_requestdistribution,
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
    logging.warning(
        "Manual cleanup only currently.")


def _Install(vm):
    """Install YCSB on client 'vm'."""
    vm.Install('ycsb')

    # copy AWS creds
    vm.RemoteCopy(FLAGS.aws_dynamodb_ycsb_awscredentials_properties, AWS_CREDENTIAL_DIR)
