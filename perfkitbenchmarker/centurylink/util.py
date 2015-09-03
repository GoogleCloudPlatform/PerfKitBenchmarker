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

    randAlfaKey = ''

    for i in range(0, 3):
        randAlfaKey += random.choice(string.ascii_uppercase)

    randIntKey = random.randint(100, 999)
    serverName = '%s%s' % (randAlfaKey, randIntKey)
    return serverName


def ParseCLIResponse(output):
    """Returns Server Name from the returned CLC CLI formatted table."""

    res = output.split('|')
    return res[3].strip()


def GetDefaultCenturylinkEnv():
    """Returns a dictionary of environment variables and other locally set \
    variables for using Centurylink Cloud SDK"""

    serverName = AutoGenerateServerName()

    env = {
        'API_KEY': os.getenv('CLC_API_KEY'),
        'API_PASSWD': os.getenv('CLC_API_PASSWD'),
        'GROUP': os.getenv('CLC_GROUP'),
        'LOCATION': os.getenv('CLC_LOCATION'),
        'NETWORK': os.getenv('CLC_NETWORK'),
        'VM_NAME': serverName,
        'VM_DESC': 'This server is created as a part of CLC hook development \
                    for PerfKitBenchmarker',
        'BACKUP_LEVEL': 'Standard',
        'CPU': 1,
        'RAM': 1,
    }

    return env
