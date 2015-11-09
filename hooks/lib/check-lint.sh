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

if [[ -z "$(command -v flake8)" ]]; then
  >&2 echo "Missing flake8. Install it via 'pip' to enable linting."
  exit 1
fi

# Treat errors from grep other than the "not found" code 1 as fatal problems.
modified_py=($(printf -- '%s\n' "$@" | grep '\.py$' || [[ $? -le 1 ]]))

# Don't run linter with no arguments, that would check all files.
if [[ "${#modified_py[@]}" -ne 0 ]]; then
  # First, run flake with normal output so that the user sees errors.
  # If there are problems, re-run flake8, printing filenames only.
  if ! flake8 "${modified_py[@]}" >&2; then
    flake8 -q "${modified_py[@]}" || true
  fi
fi

exit 0
