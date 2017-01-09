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

# Boot Volume Types
HDD = 'HDD'
SSD = 'SSD'

# Availability zones for volumes
AUTO = 'AUTO'
ZONE_1 = 'ZONE_1'
ZONE_2 = 'ZONE_2'
ZONE_3 = 'ZONE_3'

flags.DEFINE_string('profitbricks_config',
                    os.getenv('PROFITBRICKS_CONFIG',
                              '~/.config/profitbricks-auth.cfg'),
                    ('Path to config file containing your email and password. '
                     'Can also be set via $PROFITBRICKS_CONFIG environment '
                     'variable.\n(File format: email:password)'))

flags.DEFINE_enum('profitbricks_location',
                  US_LAS,
                  [US_LAS, DE_FKB, DE_FRA],
                  ('Location of data center to be provisioned (us/las, '
                   'de/fkb, de/fra)'))

flags.DEFINE_enum('profitbricks_boot_volume_type',
                  HDD,
                  [HDD, SSD],
                  ('Choose between HDD or SSD boot volume types.'))

flags.DEFINE_integer('profitbricks_boot_volume_size',
                     10,
                     ('Choose the boot volume size in GB.'))

flags.DEFINE_enum('availability_zone',
                  AUTO,
                  [AUTO, ZONE_1, ZONE_2, ZONE_2],
                  ('Direct a storage volume to be created in one of three '
                   'zones per data center (AUTO, '
                   'ZONE_1, ZONE_2, ZONE_3)'))
