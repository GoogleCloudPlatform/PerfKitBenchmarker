# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run wrk against a simple Tomcat web server.

This is close to HTTP-RR:

  * Connections are reused.
  * The server does very little work.

Doubles connections up to a fixed count, reports single connection latency and
maximum error-free throughput.

`wrk` is a scalable web load generator.
`tomcat` is a popular Java web server.
"""

import functools
import logging
import operator
import urlparse

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import tomcat
from perfkitbenchmarker.linux_packages import wrk


flags.DEFINE_integer('tomcat_wrk_test_length', 120,
                     'Length of time, in seconds, to run wrk for each '
                     'connction count', lower_bound=1)
flags.DEFINE_integer('tomcat_wrk_max_connections', 128,
                     'Maximum number of simultaneous connections to attempt',
                     lower_bound=1)
flags.DEFINE_boolean('tomcat_wrk_report_all_samples', False,
                     'If true, report throughput/latency at all connection '
                     'counts. If false (the default), report only the '
                     'connection counts with lowest p50 latency and highest '
                     'throughput.')

# Stop when >= 1% of requests have errors
MAX_ERROR_RATE = 0.01

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'tomcat_wrk'
BENCHMARK_CONFIG = """
tomcat_wrk:
  description: Run wrk against tomcat.
  vm_groups:
    server:
      vm_spec: *default_single_core
    client:
      vm_spec: *default_single_core
"""

MAX_OPEN_FILES = 65536
WARM_UP_DURATION = 30
# Target: simple sample page that generates an SVG.
SAMPLE_PAGE_PATH = 'examples/jsp/jsp2/jspx/textRotate.jspx?name=JSPX'
NOFILE_LIMIT_CONF = '/etc/security/limits.d/pkb-tomcat.conf'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _IncreaseMaxOpenFiles(vm):
  vm.RemoteCommand(('echo "{0} soft nofile {1}\n{0} hard nofile {1}" | '
                    'sudo tee {2}').format(vm.user_name, MAX_OPEN_FILES,
                                           NOFILE_LIMIT_CONF))


def _RemoveOpenFileLimit(vm):
  vm.RemoteCommand('sudo rm -f {0}'.format(NOFILE_LIMIT_CONF))


def _PrepareServer(vm):
  """Installs tomcat on the server."""
  vm.Install('tomcat')
  _IncreaseMaxOpenFiles(vm)
  tomcat.Start(vm)


def _PrepareClient(vm):
  """Install wrk on the client VM."""
  _IncreaseMaxOpenFiles(vm)
  vm.Install('curl')
  vm.Install('wrk')


def Prepare(benchmark_spec):
  """Install tomcat on one VM and wrk on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  tomcat_vm = benchmark_spec.vm_groups['server'][0]
  wrk_vm = benchmark_spec.vm_groups['client'][0]

  tomcat_vm.AllowPort(tomcat.TOMCAT_HTTP_PORT)

  vm_util.RunThreaded((lambda f: f()),
                      [functools.partial(_PrepareServer, tomcat_vm),
                       functools.partial(_PrepareClient, wrk_vm)])


def Run(benchmark_spec):
  """Run wrk against tomcat.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  tomcat_vm = benchmark_spec.vm_groups['server'][0]
  wrk_vm = benchmark_spec.vm_groups['client'][0]

  samples = []
  errors = 0
  connections = 1
  duration = FLAGS.tomcat_wrk_test_length
  max_connections = FLAGS.tomcat_wrk_max_connections

  target = urlparse.urljoin('http://{0}:{1}'.format(tomcat_vm.ip_address,
                                                    tomcat.TOMCAT_HTTP_PORT),
                            SAMPLE_PAGE_PATH)

  logging.info('Warming up for %ds', WARM_UP_DURATION)
  list(wrk.Run(wrk_vm, connections=1, target=target, duration=WARM_UP_DURATION))

  all_by_metric = []

  while connections <= max_connections:
    run_samples = list(wrk.Run(wrk_vm, connections=connections, target=target,
                               duration=duration))

    by_metric = {i.metric: i for i in run_samples}
    errors = by_metric['errors'].value
    requests = by_metric['requests'].value
    throughput = by_metric['throughput'].value
    if requests < 1:
      logging.warn('No requests issued for %d connections.',
                   connections)
      error_rate = 1.0
    else:
      error_rate = float(errors) / requests

    if error_rate <= MAX_ERROR_RATE:
      all_by_metric.append(by_metric)
    else:
      logging.warn('Error rate exceeded maximum (%g > %g)', error_rate,
                   MAX_ERROR_RATE)

    logging.info('Ran with %d connections; %.2f%% errors, %.2f req/s',
                 connections, error_rate, throughput)

    # Retry with double the connections
    connections *= 2

  if not all_by_metric:
    raise ValueError('No requests succeeded.')

  # Annotate the sample with the best throughput
  max_throughput = max(all_by_metric, key=lambda x: x['throughput'].value)
  for sample in max_throughput.itervalues():
    sample.metadata.update(best_throughput=True)

  # ...and best 50th percentile latency
  min_p50 = min(all_by_metric, key=lambda x: x['p50 latency'].value)
  for sample in min_p50.itervalues():
    sample.metadata.update(best_p50=True)

  sort_key = operator.attrgetter('metric')
  if FLAGS.tomcat_wrk_report_all_samples:
    samples = [sample for d in all_by_metric
               for sample in sorted(d.itervalues(), key=sort_key)]
  else:
    samples = (sorted(min_p50.itervalues(), key=sort_key) +
               sorted(max_throughput.itervalues(), key=sort_key))

  for sample in samples:
    sample.metadata.update(ip_type='external', runtime_in_seconds=duration)

  return samples


def Cleanup(benchmark_spec):
  """Remove tomcat and wrk.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  tomcat_vm = benchmark_spec.vm_groups['server'][0]
  tomcat.Stop(tomcat_vm)
  vm_util.RunThreaded(_RemoveOpenFileLimit, benchmark_spec.vms)
