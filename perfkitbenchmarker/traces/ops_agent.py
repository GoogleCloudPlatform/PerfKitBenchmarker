# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License);
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
"""Runs Google Cloud Ops Agent on VMs.

The metrics and logging output of Google Cloud Ops Agent publishes to Cloud
Monitoring and Cloud Logging.
"""

import logging
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker.traces import base_collector

flags.DEFINE_boolean('ops_agent', False, 'Run Ops Agent on VMs.')
flags.DEFINE_string(
    'ops_agent_debian_rapture_repo',
    'google-cloud-ops-agent-${CODENAME}-${REPO_SUFFIX:-all}',
    'The Cloud Rapture repo name used by ops agent installation bash script Debian distribution.'
)
flags.DEFINE_string(
    'ops_agent_rpm_rapture_repo',
    r'google-cloud-ops-agent-${CODENAME}-\$basearch-${REPO_SUFFIX:-all}',
    'The Cloud Rapture repo name used by ops agent installation bash script for RPM distribution .'
)
flags.DEFINE_string(
    'ops_agent_suse_rapture_repo',
    r'google-cloud-ops-agent-${CODENAME}-\$basearch-${REPO_SUFFIX:-all}',
    'The Cloud Rapture repo name used by ops agent installation bash script for SUSE distribution.'
)
flags.DEFINE_string('ops_agent_config_file', '',
                    'Path of the configuration file for Ops Agent.')

OPS_AGENT_BASH_SCRIPT_PATH = './ops_agent/add-google-cloud-ops-agent-repo.sh'
FLAGS = flags.FLAGS


class _OpsAgentCollector(base_collector.BaseCollector):
  """Ops Agent collector.

  Installs Ops Agent and runs it on the VMs.
  """

  def _CollectorName(self):
    """See base class."""
    return 'ops_agent'

  def _InstallCollector(self, vm):
    """See base class."""
    vm.RemoteCommand('sudo rm -rf ops_agent && mkdir ops_agent')

    # Renders the cloud rapture repo placeholders with actual values.
    vm.RenderTemplate(
        data.ResourcePath(OPS_AGENT_BASH_SCRIPT_PATH),
        OPS_AGENT_BASH_SCRIPT_PATH,
        context={
            'DEBIAN_REPO_NAME': str(FLAGS.ops_agent_debian_rapture_repo),
            'RPM_REPO_NAME': str(FLAGS.ops_agent_rpm_rapture_repo),
            'SUSE_REPO_NAME': str(FLAGS.ops_agent_suse_rapture_repo),
        })

    vm.RobustRemoteCommand(
        f'sudo bash {OPS_AGENT_BASH_SCRIPT_PATH} --also-install')

    if FLAGS.ops_agent_config_file:
      vm.PushFile(FLAGS.ops_agent_config_file, '/tmp/config.yaml')
      vm.RemoteCommand(
          'sudo cp /tmp/config.yaml /etc/google-cloud-ops-agent/config.yaml')

  def _CollectorRunCommand(self, vm, collector_file):
    """See base class."""
    return f'sudo service google-cloud-ops-agent restart > {collector_file} 2>&1 & echo $!'


def Register(parsed_flags):
  """Registers the ops agent collector if FLAGS.ops_agent is set."""

  if not parsed_flags.ops_agent:
    return
  logging.debug('Registering ops_agent collector.')

  collector = _OpsAgentCollector()
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
