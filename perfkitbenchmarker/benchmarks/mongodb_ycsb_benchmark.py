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

"""Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki
"""

import os

from perfkitbenchmarker import flags
from perfkitbenchmarker.packages import ycsb

FLAGS = flags.FLAGS


BENCHMARK_INFO = {'name': 'mongodb_ycsb',
                  'description': 'Run YCSB against MongoDB.',
                  'scratch_disk': True,
                  'num_machines': 2}

RESULT_REGEX = r'\[(\w+)\], (\w+)\(([\w/]+)\), ([-+]?[0-9]*\.?[0-9]+)'
OPERATIONS_REGEX = r'\[(\w+)\], Operations, ([-+]?[0-9]*\.?[0-9]+)'


def GetInfo():
  return BENCHMARK_INFO


def _GetDataDir(vm):
  return os.path.join(vm.GetScratchDir(), 'mongodb-data')


def Prepare(benchmark_spec):
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  assert len(vms) == BENCHMARK_INFO['num_machines']
  mongo_vm = vms[0]

  # Install mongodb on the 1st machine.
  mongo_vm.Install('mongodb_server')
  data_dir = _GetDataDir(mongo_vm)
  mongo_vm.RemoteCommand('mkdir {0} && chmod a+rwx {0}'.format(data_dir))
  mongo_vm.RemoteCommand(
      "sudo sed -i -e '/bind_ip/ s/^/#/; s,^dbpath=.*,dbpath=%s,' %s" %
      (data_dir,
       mongo_vm.GetPathToConfig('mongodb_server')))
  mongo_vm.RemoteCommand('sudo service %s restart' %
                         mongo_vm.GetServiceName('mongodb_server'))

  # Setup YCSB load generator on the 2nd machine.
  ycsb_vm = vms[1]
  ycsb_vm.Install('ycsb')


def Run(benchmark_spec):
  """Run run YCSB with workloada against MongoDB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  vm = vms[1]

  executor = ycsb.YCSBExecutor('mongodb')
  kwargs = {
      'mongodb.url': 'mongodb://%s:27017' % vms[0].internal_ip,
      'mongodb.writeConcern': 'normal'}
  samples = list(executor.LoadAndRun([vm], load_kwargs=kwargs,
                                     run_kwargs=kwargs))
  return samples


def Cleanup(benchmark_spec):
  """Remove MongoDB and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  mongo_vm = vms[0]
  mongo_vm.RemoteCommand('sudo service %s stop' %
                         mongo_vm.GetServiceName('mongodb_server'))
  mongo_vm.RemoteCommand('rm -rf %s' % _GetDataDir(mongo_vm))
