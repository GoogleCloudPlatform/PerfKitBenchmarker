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
set -euxo pipefail

: "${GVISOR_VERSION:?must be set}"
HOST=/host
ARCH=$(uname -m)
URL="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/${ARCH}"

if ! command -v curl >/dev/null 2>&1 || ! command -v nsenter >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq curl util-linux
fi

NEEDS_RESTART=0

# On COS nodes /usr/local/bin is read-only; binaries live on the writable
# stateful partition at /home/kubernetes/bin. On all other nodes (Ubuntu,
# Amazon Linux) /usr/local/bin is writable and already on PATH.
if [ -d "${HOST}/home/kubernetes" ]; then
  INSTALL_DIR="${HOST}/home/kubernetes/bin"
  NEEDS_PATH_DROPIN=1
else
  INSTALL_DIR="${HOST}/usr/local/bin"
  NEEDS_PATH_DROPIN=0
fi
mkdir -p "${INSTALL_DIR}"

VERSION_FILE="${INSTALL_DIR}/.gvisor_version"
INSTALLED_VERSION=""
if [ -f "${VERSION_FILE}" ]; then
  INSTALLED_VERSION=$(cat "${VERSION_FILE}")
fi

for bin in runsc containerd-shim-runsc-v1; do
  TARGET="${INSTALL_DIR}/${bin}"
  if [ ! -x "${TARGET}" ] || [ "${INSTALLED_VERSION}" != "${GVISOR_VERSION}" ]; then
    curl -fsSL "${URL}/${bin}" -o "${TARGET}.new"
    chmod +x "${TARGET}.new"
    mv "${TARGET}.new" "${TARGET}"
    NEEDS_RESTART=1
  fi
done
echo "${GVISOR_VERSION}" > "${VERSION_FILE}"

# On COS, /home/kubernetes/bin is not on systemd's default PATH; drop in a
# unit override for containerd so the shim is found. Not needed on non-COS
# nodes where /usr/local/bin is already on PATH.
if [ "${NEEDS_PATH_DROPIN}" -eq 1 ]; then
  DROPIN_DIR="${HOST}/etc/systemd/system/containerd.service.d"
  DROPIN="${DROPIN_DIR}/10-runsc-path.conf"
  mkdir -p "${DROPIN_DIR}"
  if [ ! -f "${DROPIN}" ]; then
    cat > "${DROPIN}" <<'EOF'
[Service]
Environment="PATH=/home/kubernetes/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
EOF
    NEEDS_RESTART=1
  fi
fi

# Register the runsc runtime with containerd.
CONFIG="${HOST}/etc/containerd/config.toml"
if [ ! -f "${CONFIG}" ]; then
  mkdir -p "$(dirname "${CONFIG}")"
  nsenter -t 1 -m -u -i -n -p -- containerd config default > "${CONFIG}"
fi
# Determine the active CRI plugin namespace.
# containerd v2+ uses config version 3 (or version 2 with io.containerd.cri.v1.runtime)
# where the CRI runtime plugin moved from io.containerd.grpc.v1.cri to io.containerd.cri.v1.runtime.
# Appending to the wrong section is silently ignored, leaving runsc unconfigured.
if grep -qE '^version = 3([[:space:]]|$)' "${CONFIG}" || grep -qE '^\[plugins\."io\.containerd\.cri\.v1\.runtime"(\.containerd)?\]' "${CONFIG}"; then
  CRI_PLUGIN='io.containerd.cri.v1.runtime'
  WRONG_PLUGIN='io.containerd.grpc.v1.cri'
else
  CRI_PLUGIN='io.containerd.grpc.v1.cri'
  WRONG_PLUGIN='io.containerd.cri.v1.runtime'
fi

# Clean up any stale or mismatched 2-line runsc configurations
if grep -q "^\[plugins\.\"${WRONG_PLUGIN}\"\.containerd\.runtimes\.runsc\]" "${CONFIG}"; then
  sed -i "/^\[plugins\.\"${WRONG_PLUGIN}\"\.containerd\.runtimes\.runsc\]/,+1d" "${CONFIG}"
  NEEDS_RESTART=1
fi

if ! grep -q "^\[plugins\.\"${CRI_PLUGIN}\"\.containerd\.runtimes\.runsc\]" "${CONFIG}"; then
  cat >>"${CONFIG}" <<TOML

[plugins."${CRI_PLUGIN}".containerd.runtimes.runsc]
runtime_type = "io.containerd.runsc.v1"
TOML
  NEEDS_RESTART=1
fi

if [ "${NEEDS_RESTART}" -eq 1 ]; then
  nsenter -t 1 -m -u -i -n -p -- systemctl daemon-reload
  nsenter -t 1 -m -u -i -n -p -- systemctl restart containerd
fi

echo "gVisor self-install complete on $(uname -n)."
