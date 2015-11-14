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

import os

from perfkitbenchmarker import flags

flags.DEFINE_string('CS_API_URL',
                    os.environ.get('CS_API_URL'),
                    'API endpoint for Cloudstack.')

flags.DEFINE_string('CS_API_KEY',
                    os.environ.get('CS_API_KEY'),
                    'Key for API authentication')

flags.DEFINE_string('CS_API_SECRET',
                    os.environ.get('CS_API_SECRET'),
                    'Secret for API authentication')

flags.DEFINE_string('cs_network_offering',
                    'DefaultIsolatedNetworkOfferingForVpcNetworksNoLB',
                    'Name of the network offering')

flags.DEFINE_string('cs_vpc_offering',
                    'Default VPC offering',
                    'Name of the VPC offering')

flags.DEFINE_boolean('cs_use_vpc', True,
                     'Use VPC to create networks')
