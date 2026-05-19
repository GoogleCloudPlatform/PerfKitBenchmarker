# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Azure Blob Storage (blobfuse2) PV/PVC support for the AI inference benchmark.

This module owns the Azure-specific glue between the cloud-agnostic
WG-Serving inference server and Azure Storage: flag definitions, storage-
account resolution (delegating to `AzureStorageAccount` and the helpers in
`azure_network.py` / `azure/util.py`), and rendering of the static PV/PVC
manifest backed by the `blob.csi.azure.com` CSI driver.

It is loaded automatically by `LoadProvider('Azure')` and is only referenced
from the inference server module via a lazy import on the blobfuse branch, so
it has no impact on non-Azure runs.
"""

from __future__ import annotations

import base64
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.azure import util as azure_util
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands

_MANIFEST_TEMPLATE = 'container/kubernetes_ai_inference/blobfuse_pv_pvc.yaml.j2'


def ApplyBlobFusePVC(
    cluster: kubernetes_cluster.KubernetesCluster,
) -> list[str]:
  """Renders and applies the BlobFuse Secret/PV/PVC for the inference server.

  Args:
    cluster: The Kubernetes cluster whose AKS network owns a fallback
      `AzureStorageAccount` when --k8s_inference_server_blobstorage_account is
      unset.

  Returns:
    The list of created Kubernetes resource references, for cleanup by the
    caller.

  Raises:
    errors.Resource.CreationError: When the bucket flag is unset, when no
      storage account is available, or when the account key cannot be
      retrieved.
  """
  if not azure_flags.K8S_INFERENCE_BLOBSTORAGE_CONTAINER.value:
    raise errors.Resource.CreationError(
        'Azure Blob Storage container is required to apply BlobFuse PVC. '
        'Set --k8s_inference_server_blobstorage_container.'
    )

  blob_container = azure_flags.K8S_INFERENCE_BLOBSTORAGE_CONTAINER.value
  storage_account, resource_group, account_key = _ResolveBlobStorageAccount(
      cluster
  )

  encoded_account_key = base64.b64encode(account_key.encode('utf-8')).decode(
      'utf-8'
  )
  encoded_account_name = base64.b64encode(
      storage_account.encode('utf-8')
  ).decode('utf-8')

  created_resources = list(
      kubernetes_commands.ApplyManifest(
          _MANIFEST_TEMPLATE,
          blob_container=blob_container,
          storage_account=storage_account,
          resource_group=resource_group,
          encoded_account_key=encoded_account_key,
          encoded_account_name=encoded_account_name,
          should_log_file=False,
      )
  )
  logging.info(
      'Successfully applied BlobFuse PVC for storage account %s, container %s.',
      storage_account,
      blob_container,
  )
  return created_resources


def _ResolveBlobStorageAccount(
    cluster: kubernetes_cluster.KubernetesCluster,
) -> tuple[str, str, str]:
  """Resolves the Azure Storage account, resource group, and key.

  When --k8s_inference_server_blobstorage_account is set, PKB resolves the
  resource group (via the flag or `az storage account show`) and then reuses
  `azure.util.GetAzureStorageAccountKey` -- the same helper invoked by
  `AzureStorageAccount._PostCreate` in `azure_network.py`.

  Otherwise, PKB falls back to the `AzureStorageAccount` instance owned by the
  AKS cluster's network (auto-created in the cluster's resource group),
  triggering its idempotent `Create()` so the cached `.name`,
  `.resource_group.name` and `.key` are populated.

  Args:
    cluster: The Kubernetes cluster used for the fallback storage account.

  Returns:
    A tuple of (storage_account_name, resource_group_name, account_key).
  """
  if azure_flags.K8S_INFERENCE_BLOBSTORAGE_ACCOUNT.value:
    storage_account = azure_flags.K8S_INFERENCE_BLOBSTORAGE_ACCOUNT.value
    resource_group = (
        azure_flags.K8S_INFERENCE_BLOBSTORAGE_RESOURCE_GROUP.value
        or _ResolveStorageAccountResourceGroup(storage_account)
    )
    account_key = azure_util.GetAzureStorageAccountKey(
        storage_account, ['--resource-group', resource_group]
    )
    if not account_key:
      raise errors.Resource.CreationError(
          'Failed to retrieve account key for Azure Storage account '
          f'{storage_account}.'
      )
    return storage_account, resource_group, account_key

  if azure_flags.K8S_INFERENCE_BLOBSTORAGE_RESOURCE_GROUP.value:
    raise errors.Resource.CreationError(
        '--k8s_inference_server_blobstorage_resource_group was set without '
        '--k8s_inference_server_blobstorage_account. Either also set the '
        'account flag (to mount a pre-existing container) or clear both '
        '(to fall back to the cluster-owned storage account).'
    )

  cluster_network = getattr(cluster, 'network', None)
  cluster_storage_account = getattr(cluster_network, 'storage_account', None)
  if cluster_storage_account is None:
    raise errors.Resource.CreationError(
        'Azure Storage account is required to apply BlobFuse PVC. '
        'Set --k8s_inference_server_blobstorage_account or run on an '
        'Azure cluster whose network owns a storage account.'
    )
  cluster_storage_account.Create()
  if not cluster_storage_account.key:
    raise errors.Resource.CreationError(
        f'Storage account {cluster_storage_account.name} did not expose an '
        'access key after creation.'
    )
  logging.warning(
      'No --k8s_inference_server_blobstorage_account set; falling back to '
      'cluster-owned storage account %s in resource group %s. Mount will '
      'fail with [ContainerNotFound] unless container %r already exists in '
      'this account.',
      cluster_storage_account.name,
      cluster_storage_account.resource_group.name,
      azure_flags.K8S_INFERENCE_BLOBSTORAGE_CONTAINER.value,
  )
  return (
      cluster_storage_account.name,
      cluster_storage_account.resource_group.name,
      cluster_storage_account.key,
  )


def _ResolveStorageAccountResourceGroup(storage_account: str) -> str:
  """Discovers the resource group hosting a user-supplied storage account."""
  stdout, _, _ = vm_util.IssueCommand([
      azure.AZURE_PATH,
      'storage',
      'account',
      'show',
      '--name',
      storage_account,
      '--query',
      'resourceGroup',
      '-o',
      'tsv',
  ])
  resource_group = stdout.strip()
  if not resource_group:
    raise errors.Resource.CreationError(
        'Could not resolve resource group for Azure Storage account '
        f'{storage_account}. Set '
        '--k8s_inference_server_blobstorage_resource_group.'
    )
  return resource_group
