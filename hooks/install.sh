#!/bin/bash

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

# Symlinks git hooks from hooks/ into .git/hooks.

set -o errexit
set -o nounset
set -o pipefail

readonly HOOKS_DIR="$(dirname "$(test -L "$0" && echo "$(dirname $0)/$(readlink "$0")" || echo "$0")")"
readonly HOOK_FILES=(prepare-commit-msg commit-msg pre-push)

pushd $HOOKS_DIR/.. > /dev/null

if [[ ! -d ".git" ]]; then
  >&2 echo "No git directory in $(pwd)".
fi

pushd .git/hooks > /dev/null

for file in ${HOOK_FILES[@]}; do
  src=../../$(basename $HOOKS_DIR)/$file
  if [[ ! -e $src ]]; then
    >&2 echo "Missing $src!"
    exit 1
  fi
  if [[ -L $file && "$(readlink -m $file)" == $(readlink -m $src) ]]; then
    >&2 echo "$file: symlink already configured."
  elif [[ -e $file ]]; then
    >&2 echo "$file: exists, but does not link to $src."
  else
    >&2 echo "Linking $file to $src"
    ln --symbolic $src $file
  fi
done

popd > /dev/null; popd > /dev/null;
