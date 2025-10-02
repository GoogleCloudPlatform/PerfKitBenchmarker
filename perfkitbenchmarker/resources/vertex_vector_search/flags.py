# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Flags for Vertex Vector Search."""

from absl import flags
STANDARD = 'n2d-standard-32'

VVS_LOCATION = flags.DEFINE_string(
    'vvs_location',
    None,
    'Location of the Vertex Vector Search index.',
)

VVS_DATASET_BUCKET_URL = flags.DEFINE_string(
    'vvs_dataset_bucket_url',
    None,
    'GCS bucket URL of the Vertex Vector Search dataset.',
)

VVS_VPC_NETWORK = flags.DEFINE_string(
    'vvs_vpc_network',
    None,
    'VPC network of the Vertex Vector Search index.',
)

VVS_MACHINE_TYPE = flags.DEFINE_string(
    'vvs_machine_type',
    'n2d-standard-32',
    'Machine type of the Vertex Vector Search index.',
)

VVS_MIN_REPLICA_COUNT = flags.DEFINE_integer(
    'vvs_min_replica_count',
    2,
    'Minimum number of replicas of the Vertex Vector Search index.',
)

VVS_MAX_REPLICA_COUNT = flags.DEFINE_integer(
    'vvs_max_replica_count',
    2,
    'Maximum number of replicas of the Vertex Vector Search index.',
)

VVS_BASE_URL = flags.DEFINE_string(
    'vvs_base_url',
    None,
    'Base URL of the Vertex Vector Search index.',
)
