# Copyright 2014 Google Inc. All rights reserved.
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
"""Utilities for working with Centurylink Cloud resources."""

import os
import random
import string


def AutoGenerateServerName():
    """Returns an auto-generated server name"""

    rand_alpha_key = ''

    for i in range(0, 3):
        rand_alpha_key += random.choice(string.ascii_uppercase)

    rand_int_key = random.randint(100, 999)
    server_name = '%s%s' % (rand_alpha_key, rand_int_key)
    return server_name


def GetDefaultCenturylinkEnv():
    """Returns a dictionary of environment variables and other locally set \
    variables for using Centurylink Cloud SDK"""

    server_name = AutoGenerateServerName()

    env = {
        'USERNAME': os.getenv('CLC_USERNAME'),
        'PASSWORD': os.getenv('CLC_PASSWORD'),
        'GROUP': os.getenv('CLC_GROUP'),
        'VM_NAME': server_name,
        'VM_DESC': 'This server is created as a part of CLC hook development \
                    for PerfKitBenchmarker',
        'CPU': os.getenv('CLC_CPU'),
        'RAM': os.getenv('CLC_RAM'),
    }

    return env
