#!/bin/bash
# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

# Runs as the `fetch-weights` init container of kubernetes_glm_sglang. Copies
# the preprovisioned checkpoint from MODEL_URI into the node-local cache (a
# hostPath under /var/lib/kubelet, which sits on the node's local NVMe, mounted
# at /models) once per node, so pod restarts on the same node skip the copy.
#
# Environment:
#   MODEL_URI  bucket prefix with the checkpoint, e.g.
#              gs://<bucket>/kubernetes_glm_sglang/GLM-5.3
#   MODEL_DIR  destination directory, e.g. /models/GLM-5.3
set -euo pipefail

: "${MODEL_URI:?}" "${MODEL_DIR:?}"
marker="${MODEL_DIR}/.pkb-download-complete"

mkdir -p "${MODEL_DIR}"

# Serialize with any other pod on this node writing the same directory.
if command -v flock >/dev/null 2>&1; then
  exec 9>"${MODEL_DIR}.lock"
  flock 9
fi

if [[ -f "${marker}" ]]; then
  echo "Weights already cached in ${MODEL_DIR}; skipping the copy."
  exit 0
fi

echo "Copying ${MODEL_URI} to ${MODEL_DIR} ..."
start=$(date +%s)
case "${MODEL_URI}" in
  gs://*)
    gcloud storage rsync --recursive "${MODEL_URI%/}" "${MODEL_DIR}"
    ;;
  *)
    echo "Unsupported MODEL_URI=${MODEL_URI} (expected gs://...)" >&2
    exit 1
    ;;
esac

# Minimal completeness check before marking the cache usable.
[[ -f "${MODEL_DIR}/config.json" ]] || {
  echo "config.json missing in ${MODEL_DIR}" >&2
  exit 1
}
compgen -G "${MODEL_DIR}/*.safetensors" >/dev/null || {
  echo "no *.safetensors in ${MODEL_DIR}" >&2
  exit 1
}

touch "${marker}"
echo "Done in $(($(date +%s) - start))s: $(du -sh "${MODEL_DIR}" | cut -f1)"
