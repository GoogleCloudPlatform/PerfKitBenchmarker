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

flags.DEFINE_string('gcloud_path',
                    'gcloud',
                    'The path for the gcloud utility.')
flags.DEFINE_list('additional_gcloud_flags', [],
                  'Additional flags to pass to gcloud.')
flags.DEFINE_integer('gce_num_local_ssds', 0,
                     'The number of ssds that should be added to the VM. Note '
                     'that this is currently only supported in certain zones '
                     '(see https://cloud.google.com/compute/docs/local-ssd).')
flags.DEFINE_string('gcloud_scopes', None, 'If set, space-separated list of '
                    'scopes to apply to every created machine')
flags.DEFINE_boolean('gce_migrate_on_maintenance', True, 'If true, allow VM '
                     'migration on GCE host maintenance.')
flags.DEFINE_boolean('gce_preemptible_vms', False, 'If true, use preemptible '
                     'VMs on GCE.')
flags.DEFINE_string(
    'image_project', None, 'The project against which all image references will'
    ' be resolved. See: '
    'https://cloud.google.com/sdk/gcloud/reference/compute/disks/create')
flags.DEFINE_string('gce_network_name', None, 'The name of an already created '
                    'network to use instead of creating a new one.')
flags.DEFINE_string('gce_subnet_region', None, 'Region to create subnet in '
                    'instead of automatically creating one in every region.')
flags.DEFINE_string('gce_subnet_addr', '10.128.0.0/20', 'Address range to the '
                    'subnet, given in CDR notation. Not used unless '
                    '--gce_subnet_region is given.')
flags.DEFINE_multistring(
    'gcp_instance_metadata_from_file',
    [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata-from-file" flag of the gcloud cli (with the colon replaced by '
    'the equal sign). Multiple key-value pairs may be specified by separating '
    'each pair by commas. This option can be repeated multiple times. For '
    'information about GCP instance metadata, see: --metadata-from-file from '
    '`gcloud help compute instances create`.')
flags.DEFINE_multistring(
    'gcp_instance_metadata',
    [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata" flag of the gcloud cli (with the colon replaced by the equal '
    'sign). Multiple key-value pairs may be specified by separating each pair '
    'by commas. This option can be repeated multiple times. For information '
    'about GCP instance metadata, see: --metadata from '
    '`gcloud help compute instances create`.')
flags.DEFINE_integer('gce_boot_disk_size', None,
                     'The boot disk size in GB for GCP VMs.')
flags.DEFINE_enum('gce_boot_disk_type', None, ['pd-standard', 'pd-ssd'],
                  'The boot disk type for GCP VMs.')
