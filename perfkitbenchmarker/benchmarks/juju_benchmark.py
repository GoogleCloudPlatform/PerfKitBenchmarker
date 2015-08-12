# Copyright 2015 Canonical, Ltd. All rights reserved.
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

"""
A proxy benchmark to run benchmark(s) against a juju environment
"""
import logging
import yaml

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.juju import utils as juju_utils

flags.DEFINE_string('action', '',
                    'Specify the charm and action to run, i.e. mongodb:stress')

flags.DEFINE_string('action_params', '',
                    'The parameters, if any, to pass to the action.')

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'juju'
BENCHMARK_CONFIG = """
juju:
  description: Juju meta-benchmark
  vm_groups:
    dummy:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      vm_count: 1
"""


def GetConfig(user_config):
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
    """Verifies that the required resources are present.

    Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
    """
    pass


def Prepare(benchmark_spec):
    """Deploy the environment per --model.

    Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
    """
    juju_utils.deploy(FLAGS.model, FLAGS.controller)
    pass


def RunTestOnLoader(vm, data_node_ips):
    """Run Cassandra-stress test on loader node.

    Args:
    vm: The target vm.
    data_node_ips: List of IP addresses for all data nodes.
    """
    pass


def WaitForLoaderToFinish(vm):
    """Watch loader node and wait for it to finish test.

    Args:
    vm: The target vm.
    """
    pass


def CollectResults(benchmark_spec, juju_action_fetch):
    """Collect and parse test results.

    Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

    Returns:
    A list of sample.Sample objects.
    """
    logging.info('Gathering results.')

    results = []
    if juju_action_fetch:
        result = yaml.load(juju_action_fetch)
        for key in result['results']['results']:
            try:
                results.append(
                    sample.Sample(
                        key,
                        float(result['results']['results'][key]['value']),
                        result['results']['results'][key]['units']
                    )
                )
            except ValueError:
                logging.warn("Non-numeric value skipped: %s." %
                             result['results']['results'][key]['value'])
    else:
        logging.warn("No results found.")
    return results


def Run(benchmark_spec):
    """Run Cassandra on target vms.

    Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

    Returns:
    A list of sample.Sample objects.
    """
    if FLAGS.action:
        # TODO: Handle exceptions thrown by run_command

        # Find the unit to run the benchmark against
        service = FLAGS.action[:FLAGS.action.index(':')]
        idx = service.find('/')
        if idx > 0:
            # This is a unit
            unit = service
        else:
            # Get the lowest unit
            unit = juju_utils.get_first_unit_name(service, FLAGS.controller)
            unitno = unit[unit.index('/') + 1:]
            unit = "%s/%s" % (service, unitno)

    action = FLAGS.action[FLAGS.action.index(':') + 1:]

    cmd = "juju action do %s %s %s" % (unit, action, FLAGS.action_params)
    logging.info('Running juju benchmark: %s' % cmd)
    output = juju_utils.run_command(cmd, controller=FLAGS.controller)

    # Get the Action UUID
    ACTION_UUID = output[output.index(':') + 2:]

    cmd = 'juju action fetch --wait 0 %s' % ACTION_UUID
    juju_action_fetch = juju_utils.run_command(
        cmd,
        controller=FLAGS.controller
    )

    return CollectResults(benchmark_spec, juju_action_fetch)


def Cleanup(benchmark_spec):
    """Cleanup function.

    Args:
    benchmark_spec: The benchmark specification. Contains all data
    that is required to run the benchmark.
    """
    logging.info("Cleanup called.")
    juju_utils.destroy_environment(FLAGS.controller)
