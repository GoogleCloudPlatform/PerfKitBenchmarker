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

"""Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki
"""

import logging

REQUIRED_PACKAGES_LOAD_GEN = 'git maven openjdk-7-jdk'
YCSB_CMD = ('cd YCSB; ./bin/ycsb %s mongodb -s -P workloads/workloada '
            '-threads 10 -p mongodb.url=mongodb://%s:27017')

YCSB_URL = 'git://github.com/brianfrankcooper/YCSB.git'
YCSB_COMMIT = '5659fc582c8280e1431ebcfa0891979f806c70ed'

BENCHMARK_INFO = {'name': 'mongodb',
                  'description': 'Run YCSB against MongoDB.',
                  'scratch_disk': False,
                  'num_machines': 2}


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  assert len(vms) == BENCHMARK_INFO['num_machines']
  vm = vms[0]
  # Install mongodb on the 1st machine.
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackage('mongodb-server')
  vm.RemoteCommand('sudo sed -i \'/bind_ip/ s/^/#/\' /etc/mongodb.conf')
  vm.RemoteCommand('sudo service mongodb restart')
  # Setup YCSB load generator on the 2nd machine.
  vm = vms[1]
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackage(REQUIRED_PACKAGES_LOAD_GEN)
  vm.RemoteCommand('git clone %s' % YCSB_URL)
  vm.RemoteCommand('cd YCSB; '
                   'git checkout -q %s' % YCSB_COMMIT)
  # TODO(user): remove this and update the commit referenced above
  #    when https://github.com/brianfrankcooper/YCSB/issues/181 is fixed.
  vm.RemoteCommand('cd YCSB; '
                   'sed -i -e '
                   "'s,<module>mapkeeper</module>,<!--&-->,' pom.xml")
  # TODO(user): This build requires a lot of IO, investigate moving TCSB on
  #    a data drive.
  vm.RemoteCommand('cd YCSB; mvn clean package')
  vm.RemoteCommand(YCSB_CMD % ('load', vms[0].internal_ip))


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
  # TODO(user): We are running workloada with default parameters.
  #    This does not seem like a rigorous benchmark.
  logging.info('MongoDb Results:')
  vm.RemoteCommand(
      YCSB_CMD % ('run', vms[0].internal_ip), should_log=True)
  return []


def Cleanup(benchmark_spec):
  """Remove MongoDb and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  vm.UninstallPackage('mongodb-server')
  vm = vms[1]
  vm.RemoteCommand('rm -rf YCSB')
  vm.UninstallPackage(REQUIRED_PACKAGES_LOAD_GEN)
