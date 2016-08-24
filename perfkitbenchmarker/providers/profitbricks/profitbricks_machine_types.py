# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
FLAVORS = [
    {
        'name': 'Micro',
        'ram': 1024,
        'cores': 1
    },
    {
        'name': 'Small',
        'ram': 2048,
        'cores': 1
    },
    {
        'name': 'Medium',
        'ram': 4096,
        'cores': 2
    },
    {
        'name': 'Large',
        'ram': 7168,
        'cores': 4
    },
    {
        'name': 'ExtraLarge',
        'ram': 14336,
        'cores': 8
    },
    {
        'name': 'MemoryIntensiveSmall',
        'ram': 16384,
        'cores': 2
    },
    {
        'name': 'MemoryIntensiveMedium',
        'ram': 28672,
        'cores': 2
    },
    {
        'name': 'MemoryIntensiveLarge',
        'ram': 57344,
        'cores': 2
    },
]
