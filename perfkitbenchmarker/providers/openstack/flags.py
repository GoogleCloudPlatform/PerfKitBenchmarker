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

flags.DEFINE_string('openstack_cli_path',
                    default='openstack',
                    help='The path to the OpenStack CLI binary.')

flags.DEFINE_list('openstack_additional_flags',
                  default=[],
                  help='Additional comma separated flags to pass to every '
                       'OpenStack CLI command. See "openstack --help" for '
                       'more.')

flags.DEFINE_string('openstack_public_network', None,
                    '(DEPRECATED: Use openstack_floating_ip_pool) '
                    'Name of OpenStack public network.')

flags.DEFINE_string('openstack_private_network', 'private',
                    '(DEPRECATED: Use openstack_network) '
                    'Name of OpenStack private network.')

flags.DEFINE_string('openstack_network', 'private',
                    'Name of OpenStack network. This network provides '
                    'automatically allocated fixed-IP addresses to attached '
                    'instances. Typically, this network is used for internal '
                    'communication between instances. '
                    'If openstack_floating_ip_pool is not '
                    'set then this network will be used to communicate with '
                    'the instance.')

flags.DEFINE_string('openstack_floating_ip_pool', None,
                    'Name of OpenStack floating IP-address pool. If set, '
                    'a floating-ip address from this pool will be associated'
                    'to each instance and will be used for communicating '
                    'with it. To use this flag, an internally routable network '
                    'must also be specified via the openstack_network flag.')

flags.DEFINE_boolean('openstack_config_drive', False,
                     'Add possibilities to get metadata from external drive')

flags.DEFINE_boolean('openstack_boot_from_volume', False,
                     'Boot from volume instead of an image')

flags.DEFINE_integer('openstack_volume_size', None,
                     '(DEPRECATED: Use data_disk_size) '
                     'Size of the volume (GB).')

flags.DEFINE_string('openstack_volume_type',
                    default=None,
                    help='Optional Cinder volume type to use.')

flags.DEFINE_string('openstack_image_username', 'ubuntu',
                    'Ssh username for cloud image')

NONE = 'None'
flags.DEFINE_enum('openstack_scheduler_policy', NONE,
                  [NONE, 'affinity', 'anti-affinity'],
                  'Add possibility to use affinity or anti-affinity '
                  'policy in scheduling process')
