#!/bin/bash
set -euxo pipefail

: "${GVISOR_VERSION:?must be set}"
HOST=/host
ARCH=$(uname -m)
URL="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/${ARCH}"

apt-get update -qq
apt-get install -y -qq curl util-linux

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

for bin in runsc containerd-shim-runsc-v1; do
  TARGET="${INSTALL_DIR}/${bin}"
  if [ ! -x "${TARGET}" ]; then
    curl -fsSL "${URL}/${bin}" -o "${TARGET}.new"
    chmod +x "${TARGET}.new"
    mv "${TARGET}.new" "${TARGET}"
    NEEDS_RESTART=1
  fi
done

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
if ! grep -q 'io.containerd.runsc.v1' "${CONFIG}"; then
  # containerd v2+ uses config version 3 where the CRI runtime plugin moved
  # from io.containerd.grpc.v1.cri to io.containerd.cri.v1.runtime.
  # Appending to the wrong section is silently ignored, leaving runsc
  # unconfigured even though the binary is installed.
  if grep -q 'version = 3' "${CONFIG}"; then
    CRI_PLUGIN='io.containerd.cri.v1.runtime'
  else
    CRI_PLUGIN='io.containerd.grpc.v1.cri'
  fi
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
