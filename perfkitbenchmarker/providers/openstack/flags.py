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

import os

from perfkitbenchmarker import flags

flags.DEFINE_string('openstack_auth_url',
                    os.environ.get('OS_AUTH_URL', 'http://localhost:5000'),
                    ('Url for Keystone authentication service, defaults to '
                     '$OS_AUTH_URL. Required for discovery of other OpenStack '
                     'service URLs.'))

flags.DEFINE_string('openstack_username',
                    os.getenv('OS_USERNAME', 'admin'),
                    'OpenStack login username, defaults to $OS_USERNAME.')

flags.DEFINE_string('openstack_tenant',
                    os.getenv('OS_TENANT_NAME', 'admin'),
                    'OpenStack tenant name, defaults to $OS_TENANT_NAME.')

flags.DEFINE_string('openstack_password_file',
                    os.getenv('OPENSTACK_PASSWORD_FILE',
                              '~/.config/openstack-password.txt'),
                    'Path to file containing the openstack password, '
                    'defaults to $OPENSTACK_PASSWORD_FILE. Alternatively, '
                    'setting the password itself in $OS_PASSWORD is also '
                    'supported.')

flags.DEFINE_string('openstack_nova_endpoint_type',
                    os.getenv('NOVA_ENDPOINT_TYPE', 'publicURL'),
                    'OpenStack Nova endpoint type, '
                    'defaults to $NOVA_ENDPOINT_TYPE.')

flags.DEFINE_string('openstack_public_network', None,
                    'Name of OpenStack public network')

flags.DEFINE_string('openstack_private_network', 'private',
                    'Name of OpenStack private network')

flags.DEFINE_boolean('openstack_config_drive', False,
                     'Add possibilities to get metadata from external drive')

flags.DEFINE_boolean('openstack_boot_from_volume', False,
                     'Boot from volume instead of an image')

flags.DEFINE_integer('openstack_volume_size', None,
                     'Size of the volume (GB)')

flags.DEFINE_string('openstack_image_username', 'ubuntu',
                    'Ssh username for cloud image')

NONE = 'None'
flags.DEFINE_enum('openstack_scheduler_policy', NONE,
                  [NONE, 'affinity', 'anti-affinity'],
                  'Add possibility to use affinity or anti-affinity '
                  'policy in scheduling process')
