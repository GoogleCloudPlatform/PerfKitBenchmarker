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

flags.DEFINE_boolean('boot_from_cbs_volume', 'False',
                     'When flag is included the instance will use a remote disk'
                     'as its boot disk, if machine_type supports it.')

flags.DEFINE_string('nova_path',
                    'nova',
                    'The path for the rackspace-novaclient tool.')

flags.DEFINE_string('neutron_path',
                    'neutron',
                    'The path for the rackspace-neutronclient tool.')

flags.DEFINE_list('additional_rackspace_flags',
                  [],
                  'Additional flags to pass to Rackspace.')

flags.DEFINE_boolean(
    'use_security_group', False,
    'A boolean indicating if whether or not to create a security group for the'
    ' new instance. Applies default security group rules'
    ' (e.g. allow ingress TCP, and UDP traffic through port 22). If no security'
    ' group is used, all incoming and outgoing traffic through TCP, UDP and'
    ' ICMP is allowed, this is the default.')

flags.DEFINE_boolean(
    'rackspace_apply_onmetal_ssd_tuning', default=False,
    help='Apply Rackspace recommended tuning to PCIe-based flash storage '
         'included with OnMetal IO instances. See: '
         'http://www.rackspace.com/knowledge_center/article/'
         'configure-flash-drives-in-high-io-instances-as-data-drives')

flags.DEFINE_string(
    'image_id', None, 'The ID of the image from which you want to'
    ' create the volume.')
