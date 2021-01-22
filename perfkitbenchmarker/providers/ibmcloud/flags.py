# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing flags applicable across benchmark run on IBM Cloud."""

from absl import flags

flags.DEFINE_string('ibmcloud_azone', None,
                    'IBMCloud internal DC name')

flags.DEFINE_integer('ibmcloud_volume_iops', 20000,
                     'Desired volume IOPS.')

flags.DEFINE_integer('ibmcloud_volume_bandwidth', None,
                     'Desired volume bandwidth in Mbps.')

flags.DEFINE_boolean('ibmcloud_volume_encrypted', False,
                     'Enable encryption on volume creates.')

flags.DEFINE_string('ibmcloud_image_username', 'root',
                    'Ssh username for cloud image.')

flags.DEFINE_integer('ibmcloud_polling_delay', 2,
                     'Delay between polling attempts in seconds.')

flags.DEFINE_integer('ibmcloud_timeout', 600,
                     'timeout in secs.')

flags.DEFINE_integer('ibmcloud_boot_disk_size', 10,
                     'boot volume disk size.')

flags.DEFINE_boolean('ibmcloud_debug', False,
                     'debug flag.')

flags.DEFINE_boolean('ibmcloud_resources_keep', False,
                     'keep resources.')

flags.DEFINE_string('ibmcloud_volume_profile', 'custom',
                    'volume profile')

flags.DEFINE_string('ibmcloud_bootvol_encryption_key', None,
                    'boot volume encryption key crn')

flags.DEFINE_string('ibmcloud_datavol_encryption_key', None,
                    'data volume encryption key crn')

flags.DEFINE_string('ibmcloud_vpcid', None,
                    'IBM Cloud vpc id')

flags.DEFINE_string('ibmcloud_subnet', None,
                    'primary subnet id')

flags.DEFINE_string('ibmcloud_networks', None,
                    'additional network ids, comma separated')

flags.DEFINE_string('ibmcloud_prefix', 'perfkit',
                    'resource name prefix')

flags.DEFINE_string('ibmcloud_rgid', None,
                    'Resource Group id for the account.')

flags.DEFINE_integer('ibmcloud_boot_volume_iops', 3000,
                     'boot voume iops')

flags.DEFINE_integer('ibmcloud_boot_volume_size', 0,
                     'boot voume size in GB')

flags.DEFINE_string('ibmcloud_pub_keyid', None,
                    'rias public sshkey id')

flags.DEFINE_integer('ibmcloud_network_mtu', 9000,
                     'MTU size on network interfaces.')

flags.DEFINE_integer('ibmcloud_subnets_extra', 0,
                     'extra subnets to lookup')

flags.DEFINE_integer('ibmcloud_vdisks_extra', 0,
                     'extra disks to create')

flags.DEFINE_string('ibmcloud_image_info', None,
                    'image info in json formatted file')

flags.DEFINE_boolean('ibmcloud_encrypted_image', False,
                     'encrypted image.')
