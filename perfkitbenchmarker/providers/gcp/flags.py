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

from absl import flags

# Sentinel value for unspecified platform.
GCP_MIN_CPU_PLATFORM_NONE = 'none'

flags.DEFINE_string('gcloud_path', 'gcloud', 'The path for the gcloud utility.')
flags.DEFINE_list(
    'additional_gcloud_flags', [], 'Additional flags to pass to gcloud.'
)
flags.DEFINE_integer(
    'gce_num_local_ssds',
    0,
    'The number of ssds that should be added to the VM. Note '
    'that this is currently only supported in certain zones '
    '(see https://cloud.google.com/compute/docs/local-ssd), and '
    'only applies for vms that can have a variable number of local SSDs.',
)
flags.DEFINE_string(
    'gcloud_scopes',
    None,
    'If set, space-separated list of scopes to apply to every created machine',
)
flags.DEFINE_boolean(
    'gce_migrate_on_maintenance',
    True,
    'If true, allow VM migration on GCE host maintenance.',
)
flags.DEFINE_boolean(
    'gce_automatic_restart', False, 'If true, allow VM to restart when crashes.'
)
flags.DEFINE_boolean(
    'gce_preemptible_vms', False, 'If true, use preemptible VMs on GCE.'
)
flags.DEFINE_string(
    'image_family',
    None,
    'The family of the image that the boot disk will be '
    'initialized with. The --image flag will take priority over this flag. See:'
    ' https://cloud.google.com/sdk/gcloud/reference/compute/instances/create',
)
flags.DEFINE_string(
    'image_project',
    None,
    'The project against which all image references will'
    ' be resolved. See: '
    'https://cloud.google.com/sdk/gcloud/reference/compute/disks/create',
)
GCE_CONFIDENTIAL_COMPUTE = flags.DEFINE_boolean(
    'gce_confidential_compute',
    False,
    'Whether or not we create a Confidential VM Instance',
)
GCE_CONFIDENTIAL_COMPUTE_TYPE = flags.DEFINE_string(
    'gce_confidential_compute_type',
    'sev',
    'Type of Confidential VM Instance'
)
GCE_NETWORK_NAMES = flags.DEFINE_list(
    'gce_network_name',
    [],
    'The name of an already created '
    'network to use instead of creating a new one.',
)
GCE_NETWORK_TYPE = flags.DEFINE_enum(
    'gce_network_type',
    None,
    [
        'auto',
        'custom',
        'legacy',
    ],
    'The subnet mode of the network (i.e. auto, custom, legacy)',
)
GCE_SUBNET_NAMES = flags.DEFINE_list(
    'gce_subnet_name',
    [],
    'The name of an already created '
    'subnet to use instead of creating a new one.',
)
flags.DEFINE_string(
    'gce_subnet_region',
    None,
    'Region to create subnet in '
    'instead of automatically creating one in every region.',
)
flags.DEFINE_string(
    'gce_subnet_addr',
    '10.128.0.0/20',
    'Address range to the '
    'subnet, given in CDR notation. Not used unless '
    '--gce_subnet_region is given.',
)
GCE_AVAILABILITY_DOMAIN_COUNT = flags.DEFINE_integer(
    'gce_availability_domain_count',
    0,
    'Number of fault domains to create for availability-domain placement group',
    lower_bound=0,
    upper_bound=8,
)

GCE_PLACEMENT_GROUP_MAX_DISTANCE = flags.DEFINE_integer(
    'gce_placement_group_max_distance',
    None,
    'Number of max logical switches between VMs.',
    lower_bound=0,
)
flags.DEFINE_string(
    'gce_remote_access_firewall_rule',
    None,
    'The name of an '
    'already created firewall rule which allows remote access '
    'instead of creating a new one.',
)
flags.DEFINE_multi_string(
    'gcp_instance_metadata_from_file',
    [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata-from-file" flag of the gcloud cli (with the colon replaced by '
    'the equal sign). Multiple key-value pairs may be specified by separating '
    'each pair by commas. This option can be repeated multiple times. For '
    'information about GCP instance metadata, see: --metadata-from-file from '
    '`gcloud help compute instances create`.',
)
flags.DEFINE_multi_string(
    'gcp_instance_metadata',
    [],
    'A colon separated key-value pair that will be added to the '
    '"--metadata" flag of the gcloud cli (with the colon replaced by the equal '
    'sign). Multiple key-value pairs may be specified by separating each pair '
    'by commas. This option can be repeated multiple times. For information '
    'about GCP instance metadata, see: --metadata from '
    '`gcloud help compute instances create`.',
)
flags.DEFINE_integer(
    'gce_boot_disk_size', None, 'The boot disk size in GB for GCP VMs.'
)
flags.DEFINE_enum(
    'gce_boot_disk_type',
    None,
    [
        'pd-standard',
        'pd-ssd',
        'pd-balanced',
        'hyperdisk-balanced',
    ],
    'The boot disk type for GCP VMs.',
)
flags.DEFINE_integer(
    'gce_boot_disk_iops', None, 'The boot disk iops for GCP VMs.'
)
flags.DEFINE_integer(
    'gce_boot_disk_throughput', None, 'The boot disk throughput for GCP VMs.'
)
flags.DEFINE_enum(
    'gce_ssd_interface',
    'SCSI',
    ['SCSI', 'NVME'],
    'The ssd interface for GCE local SSD.',
)
flags.DEFINE_list(
    'gce_nic_types',
    ['GVNIC'],
    'The virtual NIC type of GCE VMs. Each nic_type should map to each subnet'
    ' in gce_subnet_name based on order. All machine types currently support'
    ' GVNIC.',
)
GCE_NIC_RECORD_VERSION = flags.DEFINE_boolean(
    'gce_nic_record_version',
    False,
    'If True, records the NIC version for supported NICs (currently GVNIC).',
)
GCE_NIC_QUEUE_COUNTS = flags.DEFINE_list(
    'gce_nic_queue_counts', ['default'], 'The queue count of each NIC.'
)
EGRESS_BANDWIDTH_TIER = flags.DEFINE_enum(
    'gce_egress_bandwidth_tier',
    None,
    ['TIER_1'],
    'Egress bandwidth tier of the GCE VMs.',
)
GCE_CREATE_LOG_HTTP = flags.DEFINE_boolean(
    'gce_create_log_http',
    False,
    'If True, pass --log-http to gcloud compute instance create.',
)

flags.DEFINE_string(
    'gcp_node_type',
    None,
    'The node type of all sole tenant hosts that get created.',
)
flags.DEFINE_enum(
    'gcp_min_cpu_platform',
    None,
    [
        GCP_MIN_CPU_PLATFORM_NONE,
        'sandybridge',
        'ivybridge',
        'haswell',
        'broadwell',
        'skylake',
        'cascadelake',
        'milan',
        'icelake',
    ],
    'When specified, the VM will have either the specified '
    'architecture or a newer one. Architecture availability is zone dependent.',
)
flags.DEFINE_string(
    'gce_accelerator_type_override',
    None,
    'When specified, override the accelerator_type string passed to the gcloud '
    'compute instance create command.',
)
flags.DEFINE_string(
    'gcp_preprovisioned_data_bucket',
    None,
    'GCS bucket where pre-provisioned data has been copied.',
)
REDIS_GB = flags.DEFINE_integer(
    'gcp_redis_gb',
    5,
    'Size of redis instance in gb. Ignored if --managed_memory_store_cluster is'
    ' True.',
)
REDIS_NODE_TYPE = flags.DEFINE_enum(
    'gcp_redis_node_type',
    'redis-standard-small',
    [
        'shared-core-nano',
        'standard-small',
        'highmem-medium',
        'highmem-xlarge',
        'redis-shared-core-nano',
        'redis-standard-small',
        'redis-highmem-medium',
        'redis-highmem-xlarge',
    ],
    'Node type of redis instance. Only used if --managed_memory_store_cluster'
    ' is True.',
)
REDIS_ZONE_DISTRIBUTION = flags.DEFINE_enum(
    'gcp_redis_zone_distribution',
    'single-zone',
    ['single-zone', 'multi-zone'],
    'Zones to distribute shards between. Only used if'
    ' --managed_memory_store_cluster is True.',
)
flags.DEFINE_string(
    'gcp_service_account', None, 'Service account to use for authorization.'
)
flags.DEFINE_string(
    'gcp_service_account_key_file',
    None,
    'Local path to file that contains a private authorization '
    'key, used to activate gcloud.',
)
flags.DEFINE_string(
    'gke_node_system_config',
    None,
    'Local path to yaml file that contains node system configuration.',
)
flags.DEFINE_list('gce_tags', None, 'List of --tags when creating a VM')
flags.DEFINE_boolean(
    'gke_enable_alpha', False, 'Whether to enable alpha kubernetes clusters.'
)
flags.DEFINE_boolean(
    'gke_enable_gvnic',
    True,
    'Whether to use google virtual network interface on GKE nodes.',
)
GKE_NCCL_FAST_SOCKET = flags.DEFINE_boolean(
    'gke_enable_nccl_fast_socket',
    False,
    'Whether to enable NCCL fast socket on GKE.',
)
flags.DEFINE_string(
    'gcp_dataproc_subnet',
    None,
    'Specifies the subnet that the cluster will be part of.',
)
flags.DEFINE_multi_string(
    'gcp_dataproc_property',
    [],
    'Specifies configuration properties for installed '
    'packages, such as Hadoop and Spark. Properties are '
    'mapped to configuration files by specifying a prefix'
    ', such as "core:io.serializations". '
    'See https://cloud.google.com/dataproc/docs/concepts/'
    'configuring-clusters/cluster-properties '
    'for details.',
)
flags.DEFINE_string(
    'gcp_dataproc_image',
    None,
    'Specifies the custom image URI or the custom image name '
    'that will be used to create a cluster.',
)
flags.DEFINE_boolean(
    'gcp_internal_ip',
    False,
    'Use internal ips for ssh or scp commands. gcloud beta'
    'components must be installed to use this flag.',
)
flags.DEFINE_enum(
    'gce_network_tier',
    'premium',
    ['premium', 'standard'],
    'Network tier to use for all GCE VMs. Note that standard '
    'networking is only available in certain regions. See '
    'https://cloud.google.com/network-tiers/docs/overview',
)
flags.DEFINE_boolean(
    'gce_shielded_secure_boot',
    False,
    'Whether the image uses the shielded VM feature',
)
flags.DEFINE_boolean(
    'gce_firewall_rules_clean_all',
    False,
    'Determines whether all the gce firewall rules should be '
    'cleaned up before deleting the network. If firewall '
    'rules are added manually, PKB will not know about all of '
    'them. However, they must be deleted in order to '
    'successfully delete the PKB-created network.',
)
flags.DEFINE_enum(
    'bq_client_interface',
    'CLI',
    [
        'CLI',
        'JAVA',
        'SIMBA_JDBC_1_2_4_1007',
        'SIMBA_JDBC_1_3_3_1004',
        'SIMBA_JDBC_1_5_0_1001',
        'SIMBA_JDBC_1_5_2_1005',
        'PYTHON',
    ],
    'The Runtime Interface used when interacting with BigQuery.',
)
flags.DEFINE_string(
    'gcp_preemptible_status_bucket',
    None,
    'The GCS bucket to store the preemptible status when running on GCP.',
)
GCP_CREATE_DISKS_WITH_VM = flags.DEFINE_boolean(
    'gcp_create_disks_with_vm',
    True,
    'Whether to create PD disks at VM creation time. Defaults to True.',
)
CLOUD_REDIS_API_OVERRIDE = flags.DEFINE_string(
    'gcp_cloud_redis_api_override',
    default='https://redis.googleapis.com/',
    help='Cloud redis API endpoint override. Defaults to prod.',
)
CLOUD_VALKEY_API_OVERRIDE = flags.DEFINE_string(
    'gcp_cloud_valkey_api_override',
    default='https://memorystore.googleapis.com/',
    help='Cloud valkey API endpoint override. Defaults to prod.',
)
GKE_API_OVERRIDE = flags.DEFINE_string(
    'gke_api_override',
    default=None,
    help='GKE API endpoint override. Defaults to unset (prod).',
)
RETRY_GCE_SUBNETWORK_NOT_READY = flags.DEFINE_boolean(
    'retry_gce_subnetwork_not_ready',
    True,
    'Retry Subnetwork not ready when provisioning resources.',
)

# Flags required by dataflow_template provider
flags.DEFINE_string(
    'dpb_dataflow_template_gcs_location',
    None,
    'GCS URI path for pre-built Dataflow template to run.'
    'Template must be available before running your pipeline.'
    'e.g. gs://dataflow-templates/latest/PubSub_To_BigQuery',
)
flags.DEFINE_string(
    'dpb_dataflow_template_input_subscription',
    None,
    'Cloud Pub/Sub subscription ID for Dataflow template to '
    'ingest data from. Data must be pre-populated in '
    'subscription before running your pipeline.'
    'e.g. projects/<project>/subscriptions/<subscription>',
)
flags.DEFINE_string(
    'dpb_dataflow_template_output_ptransform',
    None,
    'Pipeline PTransform from which to retrieve Dataflow '
    'output throughput. Value depends on Dataflow template '
    'used. e.g. WriteSuccessfulRecords/StreamingInserts/'
    'StreamingWriteTables/StreamingWrite',
)
flags.DEFINE_list(
    'dpb_dataflow_template_additional_args',
    [],
    'Additional arguments which should be passed to job.',
)

# Flags for BigQuery flex slot allocation
flags.DEFINE_integer(
    'bq_slot_allocation_num', None, 'Number of flex slots to allocate.'
)
flags.DEFINE_string(
    'bq_slot_allocation_region', None, 'Region to allocate flex slots in.'
)
flags.DEFINE_string(
    'bq_slot_allocation_project', None, 'Project to allocate flex slots in.'
)

LM_NOTIFICATION_METADATA_NAME = flags.DEFINE_string(
    'lm_notification_metadata_name',
    'instance/maintenance-event',
    'Lm notification metadata name to listen on.',
)
LM_NOTIFICATION_TIMEOUT = flags.DEFINE_integer(
    'lm_notification_timeout', 120, 'Timeout for LM notification.'
)
flags.DEFINE_list(
    'data_disk_zones',
    [],
    'The zone of the GCP data disk. This is used to provision regional pd with '
    'multiple zones.',
)

SPARK_BIGQUERY_CONNECTOR = flags.DEFINE_string(
    'spark_bigquery_connector',
    None,
    'The Spark BigQuery Connector jar to pass to the Spark Job',
)

AI_USE_SDK = flags.DEFINE_bool(
    'use_ai_sdk',
    False,
    'If True, use the AI python SDK to perform operations. Otherwise, use'
    ' gcloud commands.',
)

AI_BUCKET_URI = flags.DEFINE_string(
    'ai_bucket_uri',
    None,
    'If set, use this pre-existing bucket for model upload. Otherwise will'
    ' create a bucket & copy needed information to it at runtime. Should not'
    ' have a gs:// prefix.',
)


def _ValidatePreemptFlags(flags_dict):
  if flags_dict['gce_preemptible_vms']:
    return bool(flags_dict['gcp_preemptible_status_bucket'])
  return True


flags.register_multi_flags_validator(
    ['gce_preemptible_vms', 'gcp_preemptible_status_bucket'],
    _ValidatePreemptFlags,
    'When gce_preemptible_vms is specified, '
    'gcp_preemptible_status_bucket must be specified.',
)


def _ValidateNetworkFlags(flags_dict):
  return len(flags_dict['gce_nic_types']) == len(
      flags_dict['gce_nic_queue_counts']
  ) and all(
      nic_type
      in [
          'GVNIC',
          'VIRTIO_NET',
          'IDPF',
      ]
      for nic_type in flags_dict['gce_nic_types']
  )


flags.register_multi_flags_validator(
    [
        'gce_nic_types',
        'gce_nic_queue_counts',
    ],
    _ValidateNetworkFlags,
    'gce_nic_types and gce_nic_queue_counts must be the same length.',
)
