#!/bin/bash

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

# Print 1 if the file in $1 has the correct boilerplate header, 0 otherwise.
#
# See hooks/boilerplate.py.txt or hooks/boilerplate.sh.txt for examples for
# python files and bash scripts, respectively.
FILE=$1
EXT=${FILE##*.}

REF_FILE="$(dirname $0)/boilerplate.${EXT}.txt"

if [ ! -e $REF_FILE ]; then
  echo "1"
  exit 0
fi

LINES=$(cat "${REF_FILE}" | wc -l | tr -d ' ')
if [[ "${EXT}" == "py" && -x "${FILE}" ]]; then
  # remove shabang and blank line from top of executable python files.
  HEADER=$(cat "${FILE}" | tail --lines=+3 | head "-${LINES}")
else
  HEADER=$(head "-${LINES}" "${FILE}")
fi
DIFFER=$(echo "${HEADER}" | diff - "${REF_FILE}")

if [[ -z "${DIFFER}" ]]; then
  # Header does not differ.
  echo "1"
  exit 0
fi

if [ "$(echo "${DIFFER}" | wc -l)" -eq 4 ]; then
  # Header differs by one line. Check if only copyright date differs. If so,
  # the contents of "${DIFFER}" should resemble:
  #     1c1
  #     < # Copyright 2015 Google Inc. All rights reserved.
  #     ---
  #     > # Copyright 2014 Google Inc. All rights reserved.
  LINE_PREFIX='# Copyright '
  LINE_SUFFIX=' Google Inc[.] All rights reserved[.]'
  HEADER_LINE=${LINE_PREFIX}'[0-9]+'${LINE_SUFFIX}
  DIFF_PREFIX='[[:alnum:]]+[[:space:]]*< '
  DIFF_MIDDLE='[[:space:]]*---[[:space:]]*> '
  DIFF_PATTERN='^'${DIFF_PREFIX}${HEADER_LINE}${DIFF_MIDDLE}${HEADER_LINE}'$'
  if [[ "${DIFFER}" =~ $DIFF_PATTERN ]]; then
    echo "1"
    exit 0
  fi
fi

echo "0"
