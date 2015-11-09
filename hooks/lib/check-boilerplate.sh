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

HOOKS_LIB="$(dirname "$(test -L "$0" && echo "$(dirname $0)/$(readlink "$0")" || echo "$0")")"

# Check for files without the required boilerplate.
for file in "$@"; do
  ext=${file##*.}
  ref_file="${HOOKS_LIB}/boilerplate.${ext}.txt"

  if [ ! -e $ref_file ]; then
    continue
  fi

  lines=$(cat "${ref_file}" | wc -l | tr -d ' ')
  if [[ "${ext}" == "py" && -x "${file}" ]]; then
    # remove shabang and blank line from top of executable python files.
    header=$(cat "${file}" | tail --lines=+3 | head "-${lines}")
  else
    header=$(head "-${lines}" "${file}")
  fi

  # Treat errors from diff other than the usual "files differ"
  # code 1 as fatal problems.
  differ=$(echo "${header}" | diff - "${ref_file}" || [[ $? -le 1 ]])

  if [[ -z "${differ}" ]]; then
    # Header does not differ.
    continue
  fi

  if [ "$(echo "${differ}" | wc -l)" -eq 4 ]; then
    # Header differs by one line. Check if only copyright date differs. If so,
    # the contents of "${differ}" should resemble:
    #     1c1
    #     < # Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
    #     ---
    #     > # Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
    header_line='# Copyright [0-9]+ PerfKitBenchmarker Authors[.]'
    header_line+=' All rights reserved[.]'
    diff_prefix='[[:alnum:]]+[[:space:]]*< '
    diff_middle='[[:space:]]*---[[:space:]]*> '
    diff_pattern="^${diff_prefix}${header_line}${diff_middle}${header_line}\$"
    if [[ "${differ}" =~ $diff_pattern ]]; then
      # Change matches acceptable variation.
      continue
    fi
  fi

  # Report file as invalid boilerplate.
  echo "$file"
done

exit 0
