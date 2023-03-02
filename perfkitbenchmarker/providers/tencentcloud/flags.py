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
"""Module containing flags applicable across benchmark run on Tencent Cloud."""

from absl import flags

flags.DEFINE_string('tencent_path', 'tccli', 'The path for the Tencent Cloud utility.')
flags.DEFINE_string('default_region', 'ap-guangzhou-3',
                    'The path for the Tencent Cloud utility.')
flags.DEFINE_string('unfold', '--cli-unfold-argument',
                    'Unfold complicated arguments, using . to access values of '
                    'complicated arguments')
flags.DEFINE_integer('bandwith_out', 100,
                    'The maximum outbound bandwidth of the public network, in Mbps. '
                    'The default value is 100 Mbps. ')
flags.DEFINE_boolean('assign_public_ip', True,
                    'If the public network bandwidth is greater than 0 Mbps, you can '
                    'choose whether to assign a public IP. If the public network '
                    'bandwidth is 0 Mbps, you will not be able to assign a public IP.')

