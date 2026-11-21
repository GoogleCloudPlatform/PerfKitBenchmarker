# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing flags applicable across benchmark run on OCI."""

from absl import flags

VOLUME_ATTACHMENT_TYPES = ['paravirtualized', 'iscsi']

OCI_PROFILE = flags.DEFINE_string(
    'oci_profile',
    'DEFAULT',
    'Profile in the OCI CLI config file (~/.oci/config) to authenticate with.',
)
OCI_COMPARTMENT_ID = flags.DEFINE_string(
    'oci_compartment_id',
    None,
    (
        'OCID of the compartment in which to create resources. If unset, the '
        'compartment-id default from ~/.oci/oci_cli_rc is used.'
    ),
)
OCI_AVAILABILITY_DOMAIN = flags.DEFINE_string(
    'oci_availability_domain',
    None,
    (
        'Availability domain to launch VMs in, e.g. "Uocm:US-ASHBURN-AD-1". '
        'Overrides any "-ad-N" suffix in --zone. Defaults to the first '
        'availability domain in the region.'
    ),
)
OCI_FAULT_DOMAIN = flags.DEFINE_string(
    'oci_fault_domain',
    None,
    (
        'Fault domain to launch VMs in, e.g. "FAULT-DOMAIN-1". If unset, OCI '
        'chooses one.'
    ),
)
OCI_OCPUS = flags.DEFINE_integer(
    'oci_ocpus',
    None,
    (
        'Number of OCPUs for Flex shapes. Defaults to 1 for Flex shapes and is '
        'ignored for fixed shapes.'
    ),
)
OCI_MEMORY_IN_GBS = flags.DEFINE_integer(
    'oci_memory_in_gbs',
    None,
    (
        'Memory in GB for Flex shapes. If unset, OCI uses the default '
        'memory-per-OCPU ratio for the shape.'
    ),
)
OCI_PREEMPTIBLE = flags.DEFINE_boolean(
    'oci_preemptible',
    False,
    'Whether to launch VMs as preemptible instances.',
)
OCI_NETWORK_NAME = flags.DEFINE_string(
    'oci_network_name',
    None,
    (
        'Display name of an existing VCN to use instead of creating a new one. '
        'The first subnet in the VCN is used and its security list is not '
        'modified.'
    ),
)
OCI_VCN_CIDR = flags.DEFINE_string(
    'oci_vcn_cidr', '10.0.0.0/16', 'CIDR block for VCNs created by PKB.'
)
OCI_VOLUME_ATTACHMENT_TYPE = flags.DEFINE_enum(
    'oci_volume_attachment_type',
    'paravirtualized',
    VOLUME_ATTACHMENT_TYPES,
    'How block volumes are attached to VMs.',
)
OCI_VPUS_PER_GB = flags.DEFINE_integer(
    'oci_vpus_per_gb',
    None,
    (
        'Volume performance units per GB for block volumes. Overrides the value'
        ' implied by --data_disk_type (lower_cost=0, balanced=10,'
        ' higher_performance=20, ultra_high_performance=30). Valid values are 0'
        ' and 10-120 in multiples of 10.'
    ),
)
