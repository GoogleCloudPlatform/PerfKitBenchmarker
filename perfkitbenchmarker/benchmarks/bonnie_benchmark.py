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

"""Runs plain vanilla bonnie++."""

import logging

from perfkitbenchmarker import flags

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'bonnie++',
                  'description': 'Runs Bonnie++.',
                  'scratch_disk': True,
                  'num_machines': 1}


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install Bonnie++ on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Bonnie++ prepare on %s', vm)
  vm.Install('bonnieplusplus')


def Run(benchmark_spec):
  """Run Bonnie++ on the target vm.

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
  vm = vms[0]
  logging.info('Bonnie++ running on %s', vm)
  bonnie_command = ('/usr/sbin/bonnie++ -d %s -s %d -n 100 -f' %
                    (vm.GetScratchDir(),
                     2 * vm.total_memory_kb / 1024))
  logging.info('Bonnie++ Results:')
  vm.RemoteCommand(bonnie_command, should_log=True)
  # TODO(user): The hard work! Parsing this output. For now just print out.
  return []


def Cleanup(benchmark_spec):
  """Cleanup Bonnie++ on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
