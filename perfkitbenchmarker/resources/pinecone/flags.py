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
"""Flags for pinecone."""

from absl import flags

POD = 'pod'
SERVERLESS = 'serverless'

flags.DEFINE_enum(
    'pinecone_server_type',
    POD,
    [POD, SERVERLESS],
    'Server type to use for pinecone.',
)

flags.DEFINE_string(
    'pinecone_api_key',
    '',
    'API key to use for pinecone.',
)

flags.DEFINE_string(
    'pinecone_server_environment',
    'us-west1-gcp',
    'Serverless environment to use for pinecone.',
)

flags.DEFINE_string(
    'pinecone_server_pod_type',
    'p1.x1',
    'Pod type to use for pinecone.',
)


flags.DEFINE_integer(
    'pinecone_server_replicas',
    1,
    'Replicas to use for pinecone.',
)

flags.DEFINE_integer(
    'pinecone_server_shards',
    1,
    'Number of shards to use for pinecone.',
)
