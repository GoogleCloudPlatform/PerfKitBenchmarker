# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Contains classes related to managed container services.

For now this just consists of a base cluster class that other container
services will be derived from and a Kubernetes specific variant. This enables
users to run PKB VM based benchmarks on container providers (e.g. Kubernetes)
without pre-provisioning container clusters. In the future, this may be
expanded to support first-class container benchmarks.
"""

# Temporarily hoist base related classes into this namespace
# Temporarily hoist kubernetes related classes into this namespace
from perfkitbenchmarker.resources.container_service.container import BaseContainer
from perfkitbenchmarker.resources.container_service.container import BaseContainerService
from perfkitbenchmarker.resources.container_service.container import BaseNodePoolConfig
from perfkitbenchmarker.resources.container_service.container import ContainerImage
from perfkitbenchmarker.resources.container_service.container import FLAGS
from perfkitbenchmarker.resources.container_service.container import KUBERNETES
from perfkitbenchmarker.resources.container_service.container import NodePoolName
# Temporarily hoist container_class related classes into this namespace
from perfkitbenchmarker.resources.container_service.container_cluster import BaseContainerCluster
from perfkitbenchmarker.resources.container_service.container_cluster import DEFAULT_NODEPOOL
from perfkitbenchmarker.resources.container_service.container_cluster import GetContainerClusterClass
# Temporarily hoist container_registry related classes into this namespace
from perfkitbenchmarker.resources.container_service.container_registry import BaseContainerRegistry
from perfkitbenchmarker.resources.container_service.container_registry import GetContainerRegistryClass
# Temporarily hoist error related classes into this namespace
from perfkitbenchmarker.resources.container_service.errors import ContainerException
from perfkitbenchmarker.resources.container_service.errors import FatalContainerException
from perfkitbenchmarker.resources.container_service.errors import RetriableContainerException
from perfkitbenchmarker.resources.container_service.kubectl import RETRYABLE_KUBECTL_ERRORS
from perfkitbenchmarker.resources.container_service.kubectl import RunKubectlCommand
from perfkitbenchmarker.resources.container_service.kubectl import RunRetryableKubectlCommand
from perfkitbenchmarker.resources.container_service.kubernetes import KubernetesContainer
from perfkitbenchmarker.resources.container_service.kubernetes import KubernetesContainerService
from perfkitbenchmarker.resources.container_service.kubernetes import KubernetesPod
from perfkitbenchmarker.resources.container_service.kubernetes_cluster import INGRESS_JSONPATH
from perfkitbenchmarker.resources.container_service.kubernetes_cluster import KubernetesCluster
from perfkitbenchmarker.resources.container_service.kubernetes_events import KubernetesEvent
from perfkitbenchmarker.resources.container_service.kubernetes_events import KubernetesEventPoller
from perfkitbenchmarker.resources.container_service.kubernetes_events import KubernetesEventResource
