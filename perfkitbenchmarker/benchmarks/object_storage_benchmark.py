#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
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

"""Using storage tools from providers to upload/download files in directory.

Naming Conventions (X refers to cloud providers):
PrepareX: Prepare vm with necessary storage tools from cloud providers.
RunX: Run upload/download on vm using storage tools from cloud providers.
CleanupX: Cleanup storage tools on vm.
Documentation: https://goto.google.com/perfkitbenchmarker-storage
"""

import os
import re

import gflags as flags
from perfkitbenchmarker import benchmark_spec as benchmark_spec_class
from perfkitbenchmarker import errors
from perfkitbenchmarker import perfkitbenchmarker_lib

flags.DEFINE_enum('storage', benchmark_spec_class.GCP,
                  [benchmark_spec_class.GCP, benchmark_spec_class.AWS,
                   benchmark_spec_class.AZURE],
                  'storage provider (GCP/AZURE/AWS) to use.')

flags.DEFINE_string('object_storage_credential_file', None,
                    'Directory of credential file.')

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'object_storage_benchmark',
                  'description':
                  'Benchmark upload/download to Cloud Storage using CLI tools.',
                  'scratch_disk': False,
                  'num_machines': 1}

AWS_CREDENTIAL_LOCATION = '.aws'
GCE_CREDENTIAL_LOCATION = '.config/gcloud'
AZURE_CREDENTIAL_LOCATION = '.azure'

OBJECT_STORAGE_CREDENTIAL_DEFAULT_LOCATION = {
    benchmark_spec_class.GCP: '~/' + GCE_CREDENTIAL_LOCATION,
    benchmark_spec_class.AWS: '~/' + AWS_CREDENTIAL_LOCATION,
    benchmark_spec_class.AZURE: '~/' + AZURE_CREDENTIAL_LOCATION}

NODE_URL = 'git://github.com/ry/node.git'
NODE_COMMIT = 'v0.10.30'

CONF_DIR = 'data/'

DATA_FILE = 'cloud-storage-workload.sh'
# size of all data
DATA_SIZE_IN_MB = 256.1


def GetInfo():
  return BENCHMARK_INFO


class S3StorageBenchmark(object):
  """S3 version of storage benchmark."""

  def Prepare(self, vm):
    """Prepare vm with AWS s3 tool and create a bucket using vm.

    Documentation: http://aws.amazon.com/cli/
    Args:
      vm: The vm being used to run the benchmark.
    """
    vm.InstallPackage('python-setuptools')
    vm.RemoteCommand('sudo easy_install -U pip')
    vm.RemoteCommand('sudo pip install awscli')
    vm.PushFile(FLAGS.object_storage_credential_file, AWS_CREDENTIAL_LOCATION)
    vm.RemoteCommand(
        'aws s3 mb s3://pkb%s --region=us-east-1' %
        FLAGS.run_uri)

  def Run(self, vm, result):
    """Run upload/download on vm with s3 tool.

    Args:
      vm: The vm being used to run the benchmark.
      result: The result variable to store resutls.
    """
    vm.RemoteCommand('aws s3 rm s3://pkb%s --recursive'
                     % FLAGS.run_uri, ignore_failure=True)
    _, res = vm.RemoteCommand('time aws s3 sync /run/data/ '
                              's3://pkb%s/' % FLAGS.run_uri)
    print res
    time_used = perfkitbenchmarker_lib.ParseTimeCommandResult(res)
    result[0][1] = DATA_SIZE_IN_MB / time_used
    vm.RemoteCommand('rm /run/data/*')
    _, res = vm.RemoteCommand('time aws s3 sync '
                              's3://pkb%s/ /run/data/'
                              % FLAGS.run_uri)
    print res
    time_used = perfkitbenchmarker_lib.ParseTimeCommandResult(res)
    result[1][1] = DATA_SIZE_IN_MB / time_used

  def Cleanup(self, vm):
    """Clean up S3 bucket and uninstall packages on vm.

    Args:
      vm: The vm needs cleanup.
    """
    vm.RemoteCommand('aws s3 rm s3://pkb%s --recursive'
                     % FLAGS.run_uri, ignore_failure=True)
    vm.RemoteCommand('aws s3 rb s3://pkb%s' % FLAGS.run_uri)
    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall awscli')
    vm.RemoteCommand('sudo easy_install -m pip')


class AzureBlobStorageBenchmark(object):
  """Azure Blob version of storage benchmark."""

  def Prepare(self, vm):
    """Prepare vm with Azure CLI tool and create a storage container using vm.

    Documentation: http://azure.microsoft.com/en-us/documentation/articles/
      xplat-cli/
    Args:
      vm: The vm being used to run the benchmark.
    """
    vm.InstallPackage(' '.join(['g++', 'curl', 'libssl-dev', 'apache2-utils',
                                'make', 'git-core']))
    vm.RemoteCommand('git clone %s' % NODE_URL)
    vm.RemoteCommand('cd node; git checkout -q %s' % NODE_COMMIT)
    vm.RemoteCommand('cd node; ./configure; make; sudo make install')
    vm.RemoteCommand('sudo npm install azure-cli -g')
    vm.PushFile(FLAGS.object_storage_credential_file, AZURE_CREDENTIAL_LOCATION)
    vm.RemoteCommand(
        'azure storage account create -l \'East US\' ''"pkb%s"' %
        (FLAGS.run_uri), ignore_failure=True)
    output, _ = (
        vm.RemoteCommand(
            'azure storage account keys list pkb%s' %
            (FLAGS.run_uri)))
    key = re.findall(r'Primary (.+)', output)
    vm.azure_command_suffix = (
        ' -a pkb%s -k %s' % (FLAGS.run_uri, key[0]))
    vm.RemoteCommand(
        'azure storage container create pkb%s %s' %
        (FLAGS.run_uri, vm.azure_command_suffix))
    vm.RemoteCommand('azure storage blob list pkb%s %s' % (
        FLAGS.run_uri, vm.azure_command_suffix))

  def Run(self, vm, result):
    """Run upload/download on vm with azure CLI tool.

    Args:
      vm: The vm being used to run the benchmark.
      result: The result variable to store results.
    """
    vm.RemoteCommand('for i in {0..99}; do azure storage blob delete '
                     'pkb%s file-$i.dat %s; done' %
                     (FLAGS.run_uri, vm.azure_command_suffix),
                     ignore_failure=True)
    _, res = vm.RemoteCommand('time for i in {0..99}; do azure storage blob '
                              'upload /run/data/file-$i.dat'
                              ' pkb%s %s; done' %
                              (FLAGS.run_uri, vm.azure_command_suffix))
    print res
    time_used = perfkitbenchmarker_lib.ParseTimeCommandResult(res)
    result[0][1] = DATA_SIZE_IN_MB / time_used
    vm.RemoteCommand('rm /run/data/*')
    _, res = vm.RemoteCommand('time for i in {0..99}; do azure storage blob '
                              'download pkb%s '
                              'file-$i.dat /run/data/file-$i.dat %s; done' %
                              (FLAGS.run_uri, vm.azure_command_suffix))
    print res
    time_used = perfkitbenchmarker_lib.ParseTimeCommandResult(res)
    result[1][1] = DATA_SIZE_IN_MB / time_used

  def Cleanup(self, vm):
    """Clean up Azure storage container and uninstall packages on vm.

    Args:
      vm: The vm needs cleanup.
    """
    vm.RemoteCommand(
        'for i in {0..99}; do azure storage blob delete pkb%s '
        'file-$i.dat %s; done' %
        (FLAGS.run_uri, vm.azure_command_suffix))
    vm.RemoteCommand(
        'azure storage container delete -q pkb%s %s' %
        (FLAGS.run_uri, vm.azure_command_suffix))
    vm.RemoteCommand('azure storage account delete -q pkb%s' %
                     FLAGS.run_uri)
    vm.RemoteCommand('sudo npm uninstall azure-cli -g')
    vm.RemoteCommand('cd node; sudo make clean')
    vm.RemoteCommand('rm -rf node')
    vm.UninstallPackage(' '.join(['g++', 'curl', 'libssl-dev', 'apache2-utils',
                                  'make', 'git-core']))


class GoogleCloudStorageBenchmark(object):
  """Google Cloud Storage version of storage benchmark."""

  def Prepare(self, vm):
    """Prepare vm with gsutil tool and create a bucket using vm.

    Args:
      vm: The vm being used to run the benchmark.
    """
    vm.RemoteCommand(
        'wget '
        'https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz')
    vm.RemoteCommand('tar xvf google-cloud-sdk.tar.gz')
    vm.RemoteCommand('bash ./google-cloud-sdk/install.sh '
                     '--disable-installation-options '
                     '--usage-report=false '
                     '--rc-path=.bash_profile '
                     '--path-update=true '
                     '--bash-completion=true')
    try:
      vm.RemoteCommand('mkdir .config')
    except errors.VmUtil.SshConnectionError:
      # If ran on existing machines, .config folder may already exists.
      pass
    vm.PushFile(FLAGS.object_storage_credential_file, '.config/')
    vm.gsutil_path, _ = vm.RemoteCommand('which gsutil', login_shell=True)
    vm.gsutil_path = vm.gsutil_path.split()[0]
    vm.RemoteCommand('%s mb gs://pkb%s' %
                     (vm.gsutil_path, FLAGS.run_uri))

  def Run(self, vm, result):
    """Run upload/download on vm with gsutil tool.

    Args:
      vm: The vm being used to run the benchmark.
      result:  The result variable to store results.
    """
    vm.RemoteCommand('%s rm gs://pkb%s/*' %
                     (vm.gsutil_path, FLAGS.run_uri), ignore_failure=True)
    _, res = vm.RemoteCommand('time %s -m cp /run/data/* '
                              'gs://pkb%s/' % (vm.gsutil_path, FLAGS.run_uri))

    print res
    time_used = perfkitbenchmarker_lib.ParseTimeCommandResult(res)
    result[0][1] = DATA_SIZE_IN_MB / time_used
    vm.RemoteCommand('rm /run/data/*')
    _, res = vm.RemoteCommand('time %s -m cp '
                              'gs://pkb%s/* '
                              '/run/data/' % (vm.gsutil_path, FLAGS.run_uri))
    print res
    time_used = perfkitbenchmarker_lib.ParseTimeCommandResult(res)
    result[1][1] = DATA_SIZE_IN_MB / time_used

  def Cleanup(self, vm):
    """Clean up Google Cloud Storage bucket and uninstall packages on vm.

    Args:
      vm: The vm needs cleanup.
    """
    vm.RemoteCommand('%s rm gs://pkb%s/*' %
                     (vm.gsutil_path, FLAGS.run_uri))
    vm.RemoteCommand('%s rb gs://pkb%s' %
                     (vm.gsutil_path, FLAGS.run_uri))


OBJECT_STORAGE_BENCHMARK_DICTIONARY = {
    benchmark_spec_class.GCP: GoogleCloudStorageBenchmark(),
    benchmark_spec_class.AWS: S3StorageBenchmark(),
    benchmark_spec_class.AZURE: AzureBlobStorageBenchmark()}


def Prepare(benchmark_spec):
  """Prepare vm with cloud provider tool and prepare vm with data file.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  if not FLAGS.object_storage_credential_file:
    FLAGS.object_storage_credential_file = (
        OBJECT_STORAGE_CREDENTIAL_DEFAULT_LOCATION[
            FLAGS.storage])
  FLAGS.object_storage_credential_file = os.path.expanduser(
      FLAGS.object_storage_credential_file)
  if not (
      os.path.isfile(FLAGS.object_storage_credential_file) or os.path.isdir(
          FLAGS.object_storage_credential_file)):
    raise errors.Benchmarks.MissingObjectCredentialException(
        'Credential cannot be found in %s',
        FLAGS.object_storage_credential_file)
  OBJECT_STORAGE_BENCHMARK_DICTIONARY[FLAGS.storage].Prepare(vms[0])
  # Prepare data on vm, add permission to /run/folder
  vms[0].RemoteCommand('sudo chmod 777 /run/')
  file_path = CONF_DIR + DATA_FILE
  vms[0].PushFile(file_path, '/run/')


def Run(benchmark_spec):
  """Run storage benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    Total throughput in the form of tuple. The tuple contains
        the sample metric (string), value (float), unit (string).
  """
  value = 0.0
  unit = 'MB/sec'
  metadata = {'storage provider': FLAGS.storage}
  results = [['storage upload', value, unit, metadata],
             ['storage download', value, unit, metadata]]
  vms = benchmark_spec.vms
  vms[0].RemoteCommand('cd /run/; bash cloud-storage-workload.sh')
  OBJECT_STORAGE_BENCHMARK_DICTIONARY[FLAGS.storage].Run(vms[0], results)
  print results
  return results


def Cleanup(benchmark_spec):
  """Clean up storage bucket/container and clean up vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[0].RemoteCommand('rm -rf /run/data/')
  OBJECT_STORAGE_BENCHMARK_DICTIONARY[FLAGS.storage].Cleanup(vms[0])
