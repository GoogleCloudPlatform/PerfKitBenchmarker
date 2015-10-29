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

"""Run wrk against a simple Nginx web server.

`wrk` is a scalable web load generator.
`Nginx` is a popular static web server and reverse proxy.
"""

import functools
import logging
import urlparse

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import nginx
from perfkitbenchmarker.packages import wrk


flags.DEFINE_integer('nginx_wrk_test_length', 60,
                     'Length of time, in seconds, to run wrk at each '
                     'connction count', lower_bound=1)
flags.DEFINE_float('nginx_wrk_max_p50_ratio', 5.0,
                   'Double concurrent connections until '
                   'p50 / (1 connection p50) > nginx_wrk_max_p50_ratio')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'nginx_wrk'
BENCHMARK_CONFIG = """
nginx_wrk:
  description: Run wrk against Nginx.
  vm_groups:
    server:
      vm_spec: *default_single_core
    client:
      vm_spec: *default_single_core
"""

HTTP_PORT = 80
MAX_CONNECTIONS = 512
SAMPLE_PAGE_URL = ('https://ajax.googleapis.com/ajax/libs/angularjs/1.4.2/'
                   'angular.min.js')
SAMPLE_PAGE_FILE_NAME = 'angular.min.js'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _PrepareServer(vm):
  """Installs Nginx on the server."""
  vm.Install('nginx')
  # TODO: choose a more reasonable page to get.
  vm.RemoteCommand('curl -L {} | sudo tee /usr/share/nginx/html/{}'.format(
      SAMPLE_PAGE_URL, SAMPLE_PAGE_FILE_NAME))
  nginx.Start(vm)


def _PrepareClient(vm):
  """Install wrk on the client VM."""
  vm.Install('wrk')


def Prepare(benchmark_spec):
  """Install Nginx on one VM and wrk on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  nginx_vm = benchmark_spec.vm_groups['server'][0]
  wrk_vm = benchmark_spec.vm_groups['client'][0]

  nginx_vm.AllowPort(HTTP_PORT)

  vm_util.RunThreaded((lambda f: f()),
                      [functools.partial(_PrepareServer, nginx_vm),
                       functools.partial(_PrepareClient, wrk_vm)])


def Run(benchmark_spec):
  """Run wrk against Nginx.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  nginx_vm = benchmark_spec.vm_groups['server'][0]
  wrk_vm = benchmark_spec.vm_groups['client'][0]
  samples = []
  errors = 0
  connections = 1
  baseline_p50 = None
  p50_multiplier = FLAGS.nginx_wrk_max_p50_ratio
  duration = FLAGS.nginx_wrk_test_length

  target = urlparse.urljoin('http://' + nginx_vm.ip_address,
                            SAMPLE_PAGE_FILE_NAME)

  while connections <= MAX_CONNECTIONS:
    run_samples = list(wrk.Run(wrk_vm, connections=connections, target=target,
                               duration=duration))
    samples.extend(run_samples)

    errors = next(i.value for i in run_samples if i.metric == 'errors')
    p50 = next(i.value for i in run_samples
               if i.metric == 'p50 latency')
    if baseline_p50 is None:
      baseline_p50 = p50

    logging.info('Ran with %d connections; %d errors, %fms p50 '
                 '(baseline: %fms)',
                 connections, errors, p50, baseline_p50)

    # Check stopping conditions.
    if errors > 0:
      logging.info('%d requests had errors. Terminating.', errors)
      break
    if p50 / baseline_p50 > p50_multiplier:
      logging.info('p50 %fms is %fx baseline (%fms). Terminating.',
                   p50, p50 / baseline_p50, baseline_p50)
      break

    # Retry with double the connections
    connections *= 2

  for sample in samples:
    sample.metadata.update(ip_type='external', runtime_in_seconds=duration)

  return samples


def Cleanup(benchmark_spec):
  """Remove Nginx and wrk.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  nginx_vm = benchmark_spec.vm_groups['server'][0]
  nginx.Stop(nginx_vm)
