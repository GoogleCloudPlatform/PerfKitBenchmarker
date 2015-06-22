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

set -o errexit
set -o nounset
set -o pipefail

readonly GIT_DIR=$(git rev-parse --show-toplevel)

pushd $GIT_DIR > /dev/null

>&2 echo "Running unit tests."
>&2 echo "-------------------"

find tests -name '*.pyc' | xargs --no-run-if-empty rm -f
tox

>&2 echo
>&2 echo "Running linter."
>&2 echo "---------------"
>&2 echo

flake8 && >&2 echo "OK." || >&2 echo "Failed."

>&2 echo
>&2 echo "Checking boilerplate."
>&2 echo "-------------------"
>&2 echo

missing_boilerplate=()
for f in $(git ls-files); do
  result=$($GIT_DIR/hooks/boilerplate.sh $f)
  if [[ "$result" == "0" ]]; then
    missing_boilerplate+=("$f")
  fi
done

if [[ ! -z "${missing_boilerplate[@]}" ]]; then
  >&2 echo "Missing boilerplate:"
  for f in "${missing_boilerplate[@]}"; do
    >&2 echo "  $f"
  done
fi

popd > /dev/null
