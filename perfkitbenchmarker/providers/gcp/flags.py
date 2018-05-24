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
"""Module containing flags applicable across benchmark run on GCP."""

from perfkitbenchmarker import flags

# Sentinel value for unspecified platform.
GCP_MIN_CPU_PLATFORM_NONE = 'none'
flags.DEFINE_string('gce_vpn',
                    'true',
                    'Run Benchmark through VPN')

flags.DEFINE_string('gcloud_path', 'gcloud', 'The path for the gcloud utility.')
flags.DEFINE_list('additional_gcloud_flags', [],
                  'Additional flags to pass to gcloud.')
flags.DEFINE_integer(
    'gce_num_local_ssds', 0,
    'The number of ssds that should be added to the VM. Note '
    'that this is currently only supported in certain zones '
    '(see https://cloud.google.com/compute/docs/local-ssd).')
flags.DEFINE_string(
    'gcloud_scopes', None, 'If set, space-separated list of '
    'scopes to apply to every created machine')
flags.DEFINE_boolean('gce_migrate_on_maintenance', True, 'If true, allow VM '
                     'migration on GCE host maintenance.')
flags.DEFINE_boolean('gce_preemptible_vms', False, 'If true, use preemptible '
                     'VMs on GCE.')
flags.DEFINE_string(
    'image_family', None, 'The family of the image that the boot disk will be '
    'initialized with. The --image flag will take priority over this flag. See:'
    ' https://cloud.google.com/sdk/gcloud/reference/compute/instances/create')
flags.DEFINE_string(
    'image_project', None, 'The project against which all image references will'
    ' be resolved. See: '
    'https://cloud.google.com/sdk/gcloud/reference/compute/disks/create')
flags.DEFINE_string(
    'gce_network_name', None, 'The name of an already created '
    'network to use instead of creating a new one.')
flags.DEFINE_string(
    'gce_subnet_region', None, 'Region to create subnet in '
    'instead of automatically creating one in every region.')
flags.DEFINE_string(
    'gce_subnet_addr', '10.128.0.0/20', 'Address range to the '
    'subnet, given in CDR notation. Not used unless '
    '--gce_subnet_region is given.')
flags.DEFINE_string(
    'gce_remote_access_firewall_rule', None, 'The name of an '
    'already created firewall rule which allows remote access '
    'instead of creating a new one.')
flags.DEFINE_multi_string(
    'gcp_instance_metadata_from_file', [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata-from-file" flag of the gcloud cli (with the colon replaced by '
    'the equal sign). Multiple key-value pairs may be specified by separating '
    'each pair by commas. This option can be repeated multiple times. For '
    'information about GCP instance metadata, see: --metadata-from-file from '
    '`gcloud help compute instances create`.')
flags.DEFINE_multi_string(
    'gcp_instance_metadata', [],
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
flags.DEFINE_enum('gce_ssd_interface', 'SCSI', ['SCSI', 'NVME'],
                  'The ssd interface for GCE local SSD.')
flags.DEFINE_integer(
    'gcp_num_vms_per_host', None,
    'The number of VMs per dedicated host. If None, VMs will be packed on a '
    'single host until no more can be packed at which point a new host will '
    'be created.')
flags.DEFINE_string('gcp_node_type', None,
                    'The node type of all sole tenant hosts that get created.')
flags.DEFINE_enum(
    'gcp_min_cpu_platform', None, [
        GCP_MIN_CPU_PLATFORM_NONE, 'sandybridge', 'ivybridge', 'haswell',
        'broadwell', 'skylake'
    ], 'When specified, the VM will have either the specified '
    'architecture or a newer one. Architecture availability is zone dependent.')
flags.DEFINE_string(
    'gce_accelerator_type_override', None,
    'When specified, override the accelerator_type string passed to the gcloud '
    'compute instance create command.')
flags.DEFINE_string('gcp_preprovisioned_data_bucket', None,
                    'GCS bucket where pre-provisioned data has been copied.')
flags.DEFINE_integer('gcp_redis_gb', 5, 'Size of redis cluster in gb')
flags.DEFINE_string('gcp_service_account', None, 'Service account to use for '
                    'authorization.')
flags.DEFINE_string(
    'gcp_service_account_key_file', None,
    'Local path to file that contains a private authorization '
    'key, used to activate gcloud.')
flags.DEFINE_list('gce_tags', None, 'List of --tags when creating a VM')
flags.DEFINE_boolean('gke_enable_alpha', False,
                     'Whether to enable alpha kubernetes clusters.')
flags.DEFINE_string('gcp_dataproc_subnet', None,
                    'Specifies the subnet that the cluster will be part of.')
flags.DEFINE_multi_string('gcp_dataproc_property', [],
                          'Specifies configuration properties for installed '
                          'packages, such as Hadoop and Spark. Properties are '
                          'mapped to configuration files by specifying a prefix'
                          ', such as "core:io.serializations". '
                          'See https://cloud.google.com/dataproc/docs/concepts/'
                          'configuring-clusters/cluster-properties '
                          'for details.')
flags.DEFINE_string('gcp_dataproc_image', None,
                    'Specifies the custom image URI or the custom image name '
                    'that will be used to create a cluster.')
flags.DEFINE_boolean('gcp_internal_ip', False,
                     'Use internal ips for ssh or scp commands. gcloud beta'
                     'components must be installed to use this flag.')
flags.DEFINE_enum('gce_network_tier', 'premium', ['premium', 'standard'],
                  'Network tier to use for all GCE VMs. Note that standard '
                  'networking is only available in certain regions. See '
                  'https://cloud.google.com/network-tiers/docs/overview')
flags.DEFINE_boolean(
    'gce_shielded_secure_boot', False,
    'Whether the image uses the shielded VM feature')
