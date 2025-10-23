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
"""Module containing flags applicable across benchmark run on Azure."""

from absl import flags


NONE = 'None'
READ_ONLY = 'ReadOnly'
READ_WRITE = 'ReadWrite'
flags.DEFINE_enum(
    'azure_host_caching',
    NONE,
    [NONE, READ_ONLY, READ_WRITE],
    'The type of host caching to use on Azure data disks.',
)
# Azure Storage Account types. See
# http://azure.microsoft.com/en-us/pricing/details/storage/ for more information
# about the different types.
LRS = 'Standard_LRS'
ULRS = 'UltraSSD_LRS'
PLRS = 'Premium_LRS'
PZRS = 'Premium_ZRS'
ZRS = 'Standard_ZRS'
GRS = 'Standard_GRS'
GZRS = 'Standard_GZRS'
RAGRS = 'Standard_RAGRS'
RAGZRS = 'Standard_RAGZRS'

STORAGE_V2 = 'StorageV2'
# These are deprecated in favor of StorageV2
STORAGE = 'Storage'
BLOB_STORAGE = 'BlobStorage'
BLOCK_BLOB_STORAGE = 'BlockBlobStorage'
VALID_TIERS = ['Basic', 'Standard', 'Premium']

# Azure redis cache tiers. See
# https://docs.microsoft.com/en-us/azure/redis-cache/cache-faq for information.
VALID_CACHE_SIZES = [
    'C0',
    'C1',
    'C2',
    'C3',
    'C4',
    'C5',
    'C6',
    'P1',
    'P2',
    'P3',
    'P4',
    'P5',
]

flags.DEFINE_enum(
    'azure_storage_type',
    LRS,
    [LRS, PLRS, ULRS, ZRS, GRS, RAGRS],
    'The type of storage account to create. See '
    'http://azure.microsoft.com/en-us/pricing/details/storage/ for more '
    'information. To use remote ssd scratch disks, you must use Premium_LRS. '
    'If you use Premium_LRS, you must use the DS series of machines, or else '
    'VM creation will fail.',
)

AZURE_BLOB_STORAGE_ACCOUNT_KIND = flags.DEFINE_enum(
    'azure_blob_account_kind',
    STORAGE_V2,
    [STORAGE_V2, BLOCK_BLOB_STORAGE, BLOB_STORAGE, STORAGE],
    'Type of Azure storage account to create for blob storage. Defaults to'
    ' StorageV2. BlockBlobStorage is required for Premium block blobs. See'
    ' https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#types-of-storage-accounts'
    ' for more information.',
)
AZURE_BLOB_STORAGE_TYPE = flags.DEFINE_enum(
    'azure_blob_storage_type',
    ZRS,
    [ZRS, LRS, PLRS, PZRS, GRS, RAGRS, GZRS, RAGZRS],
    'Azure storage Account SKU to use for blob storage. Defaults to '
    "'Standard_ZRS' unlike --azure_storage_type which defaults to "
    "'Standard_LRS'. This is consistent with other clouds that default to "
    'zonal disks, but regional object storage buckets.',
)
flags.DEFINE_string(
    'azure_preprovisioned_data_account',
    None,
    'Azure storage account containing Azure blob containers. Containing '
    'preprovisioned data. This does not align with GCS or S3 where packages '
    'are in the object path instead of the container/bucket name.',
)
flags.DEFINE_string(
    'azure_preprovisioned_data_subscription',
    None,
    'Azure subscription id or name for the preprovisioned data account. If not '
    'provided, Azure will look in the default subscription. Even though '
    'storage accounts are globally unique, Azure requires specifying the '
    'subscription to access.',
)

flags.DEFINE_boolean(
    'azure_accelerated_networking',
    False,
    'Enable Azure Accelerated Networking. See '
    'https://docs.microsoft.com/en-us/azure/virtual-network/'
    'create-vm-accelerated-networking-cli'
    'for more information.',
)
AZURE_NIC_COUNT = flags.DEFINE_integer(
    'azure_nic_count',
    1,
    'Number of NICs to attach to the VM. Defaults to 1. Flag azure_subnet_id'
    ' must be None if greater than 1.',
)
AZURE_SUBNET_ID = flags.DEFINE_string(
    'azure_subnet_id',
    None,
    'The id of an already created subnet to use instead of creating a new one.',
)

flags.DEFINE_enum(
    'azure_tier',
    'Basic',
    VALID_TIERS,
    'Performance tier to use for the machine type. Defaults to Basic.',
)

flags.DEFINE_integer(
    'azure_compute_units',
    None,
    'Number of compute units to allocate for the machine type',
)

REDIS_SIZE = flags.DEFINE_enum(
    'azure_redis_size',
    'C3',
    VALID_CACHE_SIZES,
    'Azure redis cache size to use.',
)

flags.DEFINE_boolean(
    'azure_low_priority_vms',
    False,
    'Whether to set the priority to low for Azure VMs',
)

flags.DEFINE_boolean(
    'bootstrap_azure_service_principal',
    True,
    'Whether to use the current service principal credentials '
    'when passing a service principal to a service. This has '
    'no effect if the logged in user is not a service '
    'principal. This is useful, because service principals '
    "usually lack the 'User Authentication Admin' role that "
    'allows creation of new service principals.',
)
flags.DEFINE_enum(
    'sqldatawarehouse_client_interface',
    'CLI',
    ['CLI', 'JDBC'],
    'The Runtime Interface used when interacting with Synapse.',
)
flags.DEFINE_string('query_timeout', '600', 'Query timeout in seconds.')
AZURE_ATTACH_DISK_WITH_CREATE = flags.DEFINE_boolean(
    'azure_attach_disk_with_create',
    True,
    'Whether to create PD disks at VM creation time. Defaults to True.',
)
AZURE_SECURE_BOOT = flags.DEFINE_bool(
    'azure_secure_boot',
    None,
    'Enable/Disable secure boot of the VM to allow unsigned operating systems'
    ' and drivers. Defaults to None to fallback to Azure default behavior',
)
flags.DEFINE_boolean(
    'azure_aks_auto_node_provisioning',
    False,
    'Enable automatic node provisioning for Azure AKS clusters. '
    'This is automatically set to True for the provision_node_pools benchmark.',
)
