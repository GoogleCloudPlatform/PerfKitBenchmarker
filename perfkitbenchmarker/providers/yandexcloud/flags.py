# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing flags applicable across benchmark run on Yandex.Cloud."""

from perfkitbenchmarker import flags

flags.DEFINE_string('yc_path', 'yc', 'The path for the yc utility.')
flags.DEFINE_list('additional_yc_flags', [],
                  'Additional flags to pass to yc.')
flags.DEFINE_boolean('yc_preemptible_vms', False, 'If true, use preemptible '
                     'VMs on YC.')
flags.DEFINE_string('yc_folder_id', None, 'YC folder ID under which '
                    'to create the virtual machines')
flags.DEFINE_string(
    'yc_image_family', None, 'The family of the image that the boot disk will be '
    'initialized with. The --image flag will take priority over this flag. See:'
    ' https://cloud.yandex.ru/docs/compute/operations/vm-create/create-linux-vm')
flags.DEFINE_string(
    'yc_image_folder_id', None, 'The folder against which all image references will'
    ' be resolved. See: '
    'https://cloud.yandex.ru/docs/compute/operations/disk-create/empty')
flags.DEFINE_string(
    'yc_network_name', None, 'The name of an already created '
    'network to use instead of creating a new one.')
flags.DEFINE_string(
    'yc_subnet_zone', None, 'Zone to create subnet in '
    'instead of automatically creating one in every zone.')
flags.DEFINE_string(
    'yc_subnet_addr', '10.128.0.0/20', 'Address range to the '
    'subnet, given in CDR notation.')
flags.DEFINE_integer(
    'yc_core_fraction', None, 'If provided, specifies baseline performance for a core in percent')
flags.DEFINE_enum(
    'yc_platform_id', None, [
        'standard-v1', 'standard-v2',
    ], 'Specifies platform ID for the instance.')
flags.DEFINE_multi_string(
    'yc_instance_metadata_from_file', [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata-from-file" flag of the yc cli (with the colon replaced by '
    'the equal sign). Multiple key-value pairs may be specified by separating '
    'each pair by commas. This option can be repeated multiple times. For '
    'information about YC instance metadata, see: --metadata-from-file from '
    '`yc help compute instances create`.')
flags.DEFINE_multi_string(
    'yc_instance_metadata', [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata" flag of the yc cli (with the colon replaced by the equal '
    'sign). Multiple key-value pairs may be specified by separating each pair '
    'by commas. This option can be repeated multiple times. For information '
    'about YC instance metadata, see: --metadata from '
    '`yc help compute instances create`.')
flags.DEFINE_integer('yc_boot_disk_size', None,
                     'The boot disk size in GB for YC VMs.')
flags.DEFINE_enum('yc_boot_disk_type', None, ['network-hdd', 'network-ssd'],
                  'The boot disk type for YC VMs.')

flags.DEFINE_string('yc_preprovisioned_data_bucket', None,
                    'YC bucket where pre-provisioned data has been copied.')
