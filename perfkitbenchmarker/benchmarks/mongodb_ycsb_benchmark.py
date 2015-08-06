# Copyright 2015 Google Inc. All rights reserved.
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

import functools
import os

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import ycsb

# See http://api.mongodb.org/java/2.13/com/mongodb/WriteConcern.html
flags.DEFINE_string('mongodb_writeconcern', 'safe',
                    'MongoDB write concern.')

FLAGS = flags.FLAGS


BENCHMARK_INFO = {'name': 'mongodb_ycsb',
                  'description': 'Run YCSB against MongoDB.',
                  'scratch_disk': True,
                  'num_machines': 2}


def GetInfo():
  return BENCHMARK_INFO


def _GetDataDir(vm):
  return os.path.join(vm.GetScratchDir(), 'mongodb-data')


def _PrepareServer(vm):
  """Installs MongoDB on the server."""
  vm.Install('mongodb_server')
  data_dir = _GetDataDir(vm)
  vm.RemoteCommand('mkdir {0} && chmod a+rwx {0}'.format(data_dir))
  vm.RemoteCommand(
      "sudo sed -i -e '/bind_ip/ s/^/#/; s,^dbpath=.*,dbpath=%s,' %s" %
      (data_dir,
       vm.GetPathToConfig('mongodb_server')))
  vm.RemoteCommand('sudo service %s restart' %
                   vm.GetServiceName('mongodb_server'))


def _PrepareClient(vm):
  """Install YCSB on the client VM."""
  vm.Install('ycsb')


def Prepare(benchmark_spec):
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  assert len(vms) == BENCHMARK_INFO['num_machines']
  mongo_vm, ycsb_vm = vms[:2]

  vm_util.RunThreaded((lambda f: f()),
                      [functools.partial(_PrepareServer, mongo_vm),
                       functools.partial(_PrepareClient, ycsb_vm)])


def Run(benchmark_spec):
  """Run YCSB with against MongoDB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[1]

  executor = ycsb.YCSBExecutor('mongodb')
  kwargs = {
      'mongodb.url': 'mongodb://%s:27017' % vms[0].internal_ip,
      'mongodb.writeConcern': FLAGS.mongodb_writeconcern}
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
