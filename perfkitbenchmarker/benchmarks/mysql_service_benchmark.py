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

"""MySQL Service Benchmarks.

This is a set of benchmarks that measures performance of MySQL DataBases.

Currently it focuses on managed MySQL services.

"""

import logging

from perfkitbenchmarker import benchmark_spec as benchmark_spec_class
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'mysql_service',
                  'description': 'MySQL service benchmarks.',
                  'scratch_disk': False,
                  'num_machines': 1}


def GetInfo():
  return BENCHMARK_INFO


# TODO: Add Azure based MySQL benchmarks. We need to install mysql instances
# by ourselves on Azure because it does not support a managed MySQL service.


class RDSMySQLBenchmark(object):
  """MySQL benchmark based on the RDS service on AWS."""

  def Prepare(self, vm):
    logging.info('Preparing MySQL Service benchmarks for RDS.')

  def Run(self, vm, metadata):
    results = []
    return results

  def Cleanup(self, vm):
    """Clean up RDS instances.
    """
    pass


class GoogleCloudSQLBenchmark(object):
  """MySQL benchmark based on the Google Cloud SQL service."""

  def Prepare(self, vm):
    logging.info('Preparing MySQL Service benchmarks for Google Cloud SQL.')


  def Run(self, vm, metadata):
    results = []
    return results

  def Cleanup(self, vm):
    pass

MYSQL_SERVICE_BENCHMARK_DICTIONARY = {
    benchmark_spec_class.GCP: GoogleCloudSQLBenchmark(),
    benchmark_spec_class.AWS: RDSMySQLBenchmark()}


def Prepare(benchmark_spec):
  """Prepare the MySQL DB Instances, configures it.
     Prepare the client test VM, installs SysBench, configures it.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.cloud].Prepare(vms[0])


def Run(benchmark_spec):
  """Run the MySQL Service benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    Results.
  """
  logging.info('Start benchmarking MySQL Service, '
               'Cloud Provider is %s.', FLAGS.cloud)
  vms = benchmark_spec.vms
  metadata = {}
  results = MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.storage].Run(vms[0],
                                                                  metadata)
  print results
  return results


def Cleanup(benchmark_spec):
  """Clean up MySQL Service benchmark related states on server and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.storage].Cleanup(vms[0])
