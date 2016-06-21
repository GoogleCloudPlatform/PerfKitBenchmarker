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

import os

from perfkitbenchmarker import flags


# Locations
US_LAS = 'us/las'
DE_FKB = 'de/fkb'
DE_FRA = 'de/fra'

# Zones
ZONE_1 = 'ZONE_1'
ZONE_2 = 'ZONE_2'

flags.DEFINE_string('profitbricks_config',
                    os.getenv('PROFITBRICKS_CONFIG',
                              '~/.config/profitbricks-auth.cfg'),
                    ('Path to config file containing your email and password. '
                     'Can also be set via $PROFITBRICKS_CONFIG environment '
                     "variable.\n(File format: email:password)"))

flags.DEFINE_enum('location',
                  US_LAS,
                  [US_LAS, DE_FKB, DE_FRA],
                  ('Location of data center to be provisioned (us/las, '
                   'de/fkb, de/fra)'))

flags.DEFINE_integer('profitbricks_ram',
                     None,
                     ('Amount of RAM for the new server in multiples '
                      'of 256 MB.'))

flags.DEFINE_integer('profitbricks_cores',
                     None,
                     ('Number of cores for the new server.'))

flags.DEFINE_string('profitbricks_disk_type',
                    'HDD',
                    ('Choose between HDD or SSD disk types.'))

flags.DEFINE_integer('profitbricks_disk_size',
                     20,
                     ('Choose the disk size in GB.'))

flags.DEFINE_enum('zone',
                  ZONE_1,
                  [ZONE_1, ZONE_2],
                  ('Choose availability ZONE_1 or ZONE_2.'))
