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


NONE = 'None'
READ_ONLY = 'ReadOnly'
READ_WRITE = 'ReadWrite'
flags.DEFINE_enum(
    'azure_host_caching', NONE,
    [NONE, READ_ONLY, READ_WRITE],
    'The type of host caching to use on Azure data disks.')
# Azure Storage Account types. See
# http://azure.microsoft.com/en-us/pricing/details/storage/ for more information
# about the different types.
LRS = 'Standard_LRS'
PLRS = 'Premium_LRS'
ZRS = 'Standard_ZRS'
GRS = 'Standard_GRS'
RAGRS = 'Standard_RAGRS'

STORAGE = 'Storage'
BLOB_STORAGE = 'BlobStorage'

flags.DEFINE_enum(
    'azure_storage_type', LRS,
    [LRS, PLRS, ZRS, GRS, RAGRS],
    'The type of storage account to create. See '
    'http://azure.microsoft.com/en-us/pricing/details/storage/ for more '
    'information. To use remote ssd scratch disks, you must use Premium_LRS. '
    'If you use Premium_LRS, you must use the DS series of machines, or else '
    'VM creation will fail.')

flags.DEFINE_enum(
    'azure_blob_account_kind', BLOB_STORAGE,
    [STORAGE, BLOB_STORAGE],
    'The type of storage account to use for blob storage. Choosing Storage '
    'will let you use ZRS storage. Choosing BlobStorage will give you access '
    'to Hot and Cold storage tiers.')
