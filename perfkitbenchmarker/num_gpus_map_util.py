# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""A map of machine types to their number of K80 GPUs"""

gpus_per_vm = {
    'n1-standard-4-k80x1': 1,
    'n1-standard-8-k80x1': 1,
    'n1-standard-8-k80x2': 2,
    'n1-standard-16-k80x2': 2,
    'n1-standard-16-k80x4': 4,
    'n1-standard-32-k80x4': 4,
    'n1-standard-32-k80x8': 8,
    'p2.xlarge': 1,
    'p2.8xlarge': 8,
    'p2.16xlarge': 16,
    'Standard_NC6': 1,
    'Standard_NC12': 2,
    'Standard_NC24': 4
}
