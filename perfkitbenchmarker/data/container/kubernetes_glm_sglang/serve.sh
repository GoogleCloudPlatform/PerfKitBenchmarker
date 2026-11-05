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

# Runs as the `sglang` container of kubernetes_glm_sglang. Builds the SGLang
# launch command for GLM-5.3 (FP8) on one 8x NVIDIA B200 node from environment
# variables set by the Deployment.
#
# Profiles follow the SGLang cookbook recipes for GLM-5.3 on 8x B200:
#   throughput: TP8 + DP-attention + DeepEP + MTP (EAGLE 1/1/2)
#   latency:    TP8 + a longer MTP draft (EAGLE 5/1/6)
set -euo pipefail

: "${MODEL_DIR:?}" "${SERVED_MODEL_NAME:?}" "${SERVICE_PORT:?}"
gpus="${GPU_COUNT:-8}"

args=(
  --model-path "${MODEL_DIR}"
  --served-model-name "${SERVED_MODEL_NAME}"
  --tp-size "${gpus}"
  --trust-remote-code
  # GLM-5.3 chat template parsers: thinking -> reasoning_content, tool calls.
  --reasoning-parser glm45
  --tool-call-parser glm47
  --host 0.0.0.0
  --port "${SERVICE_PORT}"
  --enable-metrics
)

case "${SERVING_PROFILE:-throughput}" in
  throughput)
    # DP-attention shards the KV cache across the 8 ranks instead of
    # replicating it per TP rank; DeepEP runs the MoE all-to-all over NVLink.
    # Newer SGLang maps `--dp-size N --enable-dp-attention` to
    # `--attn-dp-size N`.
    args+=(
      --dp-size "${gpus}" --enable-dp-attention
      --moe-a2a-backend deepep
      --speculative-algorithm EAGLE
      --speculative-num-steps 1
      --speculative-eagle-topk 1
      --speculative-num-draft-tokens 2
      --mem-fraction-static 0.85
      --chunked-prefill-size 32768
      --max-running-requests 256
    )
    ;;
  latency)
    args+=(
      --speculative-algorithm EAGLE
      --speculative-num-steps 5
      --speculative-eagle-topk 1
      --speculative-num-draft-tokens 6
      --mem-fraction-static 0.8
    )
    ;;
  *)
    echo "Unknown SERVING_PROFILE=${SERVING_PROFILE}" >&2
    exit 1
    ;;
esac

if [[ -n "${CONTEXT_LENGTH:-}" ]]; then
  args+=(--context-length "${CONTEXT_LENGTH}")
fi

# Hierarchical KV cache: spill evicted KV pages to pinned host DRAM
# (HICACHE_SIZE_GB per rank) so returning multi-turn sessions skip re-prefill.
if ((${HICACHE_SIZE_GB:-0} > 0)); then
  args+=(
    --enable-hierarchical-cache
    --hicache-size "${HICACHE_SIZE_GB}"
    --hicache-write-policy write_back
    --hicache-mem-layout page_first
    --hicache-io-backend kernel
    --page-size 64
  )
fi

# shellcheck disable=SC2206  # intentional word splitting of user flags
extra=(${SGLANG_EXTRA_ARGS:-})

echo "+ python3 -m sglang.launch_server ${args[*]} ${extra[*]:-}"
exec python3 -m sglang.launch_server "${args[@]}" ${extra[@]+"${extra[@]}"}
