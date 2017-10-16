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

flags.DEFINE_string('ali_user_name', 'ubuntu',
                    'This determines the user name that Perfkit will '
                    'attempt to use. This must be changed in order to '
                    'use any image other than ubuntu.')
flags.DEFINE_integer('ali_bandwidth_in', 100, 'Inbound Bandwidth')
flags.DEFINE_integer('ali_bandwidth_out', 100, 'Outbound Bandwidth')
flags.DEFINE_string('ali_io_optimized', None,
                    'IO optimized for disk in AliCloud. The default is '
                    'None which means no IO optimized '
                    '"optimized" means use IO optimized. If you '
                    'choose optimized, you must specify the system disk type')
flags.DEFINE_string('ali_system_disk_type', 'cloud',
                    'System disk catogory for AliCloud. The default is '
                    '"cloud" for General cloud disk, '
                    '"cloud_ssd" for cloud ssd disk, '
                    '"cloud_efficiency" for efficiency cloud disk, '
                    '"ephemeral_ssd" for local ssd disk')
flags.DEFINE_boolean('ali_use_vpc', True,
                     'Use VPC to create networks')
flags.DEFINE_integer('ali_eip_address_bandwidth', 5,
                     'The rate limit of the EIP in Mbps.')
