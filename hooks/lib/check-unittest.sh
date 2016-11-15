#!/bin/bash

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

set -euo pipefail

if [[ -z $(command -v tox) ]]; then
  >&2 echo "Missing tox >= 2.0.0. Install it via 'pip' to enable unit testing."
  exit 1
fi

# Unit test failures can't easily be mapped to specific input files.
# Always run all tests, and report a failure via nonzero exit code if
# any test fails.
if ! tox -e py27,scripttests >&2; then
  >&2 echo "*** tox failed. Note: If dependencies have changed, try "\
    "running 'tox -r' to update the tox environment"
  exit 1
fi
