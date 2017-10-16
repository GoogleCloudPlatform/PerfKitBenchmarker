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

from perfkitbenchmarker import flags


# Many places in the PKB codebase directly reference the global FLAGS. Several
# tests were written using python-gflags==2.0, which allows accessing flag
# values before they are parsed. Abseil forbids this behavior, so mark flags
# as parsed before any tests are executed.
flags.FLAGS.mark_as_parsed()
