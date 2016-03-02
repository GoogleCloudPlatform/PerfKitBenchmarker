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

from perfkitbenchmarker import flags

flags.DEFINE_string('rack_path',
                    default='rack',
                    help='The path for the rack CLI binary.')

flags.DEFINE_string('rackspace_region', default='IAD',
                    help='A string indicating which Rackspace region to use.')

flags.DEFINE_string('rack_profile', default=None,
                    help='A string indicating which RackCLI profile to use. '
                         'If none is specified default profile is used '
                         '(see https://developer.rackspace.com/docs/'
                         'rack-cli/configuration/#config-file)')

flags.DEFINE_boolean('rackspace_boot_from_cbs_volume', 'False',
                     'When flag is included the instance will use a remote disk'
                     ' as its boot disk, if machine_type supports it.')

flags.DEFINE_boolean(
    'rackspace_use_security_group', False,
    '(EXPERIMENTAL) A boolean indicating whether or not to create a security'
    ' group for the new instance. Applies default security group rules'
    ' (e.g. allow ingress TCP, and UDP traffic through port 22). If no security'
    ' group is used, all incoming and outgoing traffic through TCP, UDP and'
    ' ICMP is allowed, this is the default.')

flags.DEFINE_string('rackspace_network_id', None,
                    '(EXPERIMENTAL) The ID of an already '
                    'created network to use instead of creating a new one. '
                    'Must have a subnet already associated with the network.')

flags.DEFINE_list('additional_rackspace_flags',
                  [],
                  'Additional global flags to pass to every '
                  'RackCLI command. See "rack --help" for more.')
