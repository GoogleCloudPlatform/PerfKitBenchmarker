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
from .base import BaseContainer
from .base import BaseContainerCluster
from .base import BaseContainerRegistry
from .base import BaseContainerService
from .base import BaseNodePoolConfig
from .base import ContainerException
from .base import ContainerImage
from .base import DEFAULT_NODEPOOL
from .base import FatalContainerException
from .base import FLAGS
from .base import GetContainerClusterClass
from .base import GetContainerRegistryClass
from .base import KUBERNETES
from .base import NodePoolName
from .base import RetriableContainerException
# Temporarily hoist kubernetes related classes into this namespace
from .kubernetes import INGRESS_JSONPATH
from .kubernetes import KubernetesCluster
from .kubernetes import KubernetesClusterCommands
from .kubernetes import KubernetesContainer
from .kubernetes import KubernetesContainerService
from .kubernetes import KubernetesEvent
from .kubernetes import KubernetesEventPoller
from .kubernetes import KubernetesEventResource
from .kubernetes import KubernetesPod
from .kubernetes import RETRYABLE_KUBECTL_ERRORS
from .kubernetes import RunKubectlCommand
from .kubernetes import RunRetryableKubectlCommand
