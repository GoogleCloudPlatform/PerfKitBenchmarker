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

flags.DEFINE_string('acloud_auth_url',
                    os.environ.get('OS_AUTH_URL', 'http://localhost:5000'),
                    ('Url for Keystone authentication service, defaults to '
                     '$OS_AUTH_URL. Required for discovery of other Acloud '
                     'service URLs.'))

flags.DEFINE_string('acloud_username',
                    os.getenv('OS_USERNAME', 'admin'),
                    'Acloud login username, defaults to $OS_USERNAME.')

flags.DEFINE_string('acloud_tenant',
                    os.getenv('OS_TENANT_NAME', 'admin'),
                    'Acloud tenant name, defaults to $OS_TENANT_NAME.')

flags.DEFINE_string('acloud_password',
                    os.getenv('OS_PASSWORD', '1'),
                    'setting the password.')
flags.DEFINE_string('acloud_password_file',
                    os.getenv('OS_PASSWORD_FILE',
                              '~/.config/acloud-password.txt'),
                    'Path to file containing the acloud password, '
                    'defaults to $ACLOUD_PASSWORD_FILE. Alternatively, '
                    'setting the password itself in $OS_PASSWORD is also '
                    'supported.')

flags.DEFINE_string('acloud_nova_endpoint_type',
                    os.getenv('NOVA_ENDPOINT_TYPE', 'publicURL'),
                    'Acloud Nova endpoint type, '
                    'defaults to $NOVA_ENDPOINT_TYPE.')

flags.DEFINE_string('acloud_public_network', None,
                    'Name of Acloud public network')

flags.DEFINE_string('acloud_private_network', 'private',
                    'Name of Acloud private network')

flags.DEFINE_boolean('acloud_config_drive', False,
                     'Add possibilities to get metadata from external drive')

flags.DEFINE_boolean('acloud_boot_from_volume', False,
                     'Boot from volume instead of an image')

flags.DEFINE_integer('acloud_volume_size', None,
                     'Size of the volume (GB)')

NONE = 'None'
flags.DEFINE_enum('acloud_scheduler_policy', NONE,
                  [NONE, 'affinity', 'anti-affinity'],
                  'Add possibility to use affinity or anti-affinity '
                  'policy in scheduling process')
