# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

import gflags as flags  # NOQA
import gflags_validators as flags_validators  # NOQA
import pint

# Pint recommends one global UnitRegistry for the entire program, so
# we create it here.
UNIT_REGISTRY = pint.UnitRegistry()

# Pint 0.6 uses 'Bo' as the abbreviation for a byte. We want to use
# 'B', like the rest of the world.
UNIT_REGISTRY.define('byte = 8 * bit = B')

# Apparently the prefix kilo- is supposed to be abbreviated with a
# lower-case k. However, everyone uses the upper-case K, and would be
# very surprised to find out that 'KB' is not a valid unit.
UNIT_REGISTRY.define('K- = 1000')
