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

"""Runs CloudSuite Web Search benchmark.

Docs:
http://parsa.epfl.ch/cloudsuite/

Runs CloudSuite Web Search to collect the statistics that show
the operations completed per second and the minimum, maximum,
average, 90th, and 99th response times.
"""

import logging
import posixpath

from perfkitbenchmarker import flags

from perfkitbenchmarker.packages.cloudsuite_web_search \
    import CLOUDSUITE_WEB_SEARCH_DIR

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'cloudsuite_web_search',
                  'description': 'Run CloudSuite Web Search',
                  'scratch_disk': True,
                  'num_machines': 2}

APACHE_NUTCH_TAR_URL = ('/home/vstefano/test1/apache-nutch-1.10-src.tar.gz')
NUTCH_SITE_URL = ('/home/vstefano/test1/apache-nutch-1.10/conf/nutch-site.xml')
CRAWLED_URL = ('/home/vstefano/test1/hadoop-2.7.1/crawl7')
NUTCH_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'apache-nutch-1.10')


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install cloudsuite web search and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms:
    vm.Install('cloudsuite_web_search')
  vms[0].RemoteCommand('tar -C {3} -xzf {4} && '
                       'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-'
                       '1.7.0.9.x86_64 && '
                       'cd {0} && '
                       'cp {1} conf/ && '
                       'ant && '
                       'cd runtime/local && '
                       'bin/nutch index {2}/crawldb/ -linkdb '
                       '{2}/linkdb/ -dir {2}/segments/'.format(
                           NUTCH_HOME_DIR, NUTCH_SITE_URL, CRAWLED_URL,
                           CLOUDSUITE_WEB_SEARCH_DIR, APACHE_NUTCH_TAR_URL))


def Run(benchmark_spec):
  """Run cloudsuite web search on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  results = []

  logging.info('CloudSuite Web Search Results:')

  return results


def Cleanup(benchmark_spec):
  """Cleanup CloudSuite Web Search on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
