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
"""Spec for a Kubernetes Inference Server resource created by wg-serving cli."""

from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.resources import kubernetes_inference_server_spec

# Register wg-serving Inference Server as the default inference server type.
REGISTERED_INFERENCE_SERVER_TYPE = 'KubernetesWGServingInferenceServer'
_DEFAULT_INFERENCE_SERVER_DEPLOYMENT_TIMEOUT = 720
_DEFAULT_HPA_CUSTOM_METRIC_NAME = 'vllm:num_requests_running'


class WGServingStaticInferenceServerConfigSpec(
    kubernetes_inference_server_spec.BaseStaticInferenceServerConfigSpec
):
  """Spec for static wg-serving inference server configuration.

  wg-serving inference server lined to a k8s service. try to find the service
  and extract metadata from it.

  Attributes:
    endpoint: Endpoint of the inference server.
    port: Port of the inference server.
    model_id: Model ID of the model to use for the benchmark.
    tokenizer_id: Tokenizer ID of the model to use for the benchmark.
  """

  INFERENCE_SERVER_TYPE = REGISTERED_INFERENCE_SERVER_TYPE

  def __init__(self, *args, **kwargs):
    self.endpoint: str
    self.port: int
    self.model_id: str | None
    self.tokenizer_id: str | None
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'endpoint': (
            option_decoders.StringDecoder,
            {'none_ok': False},
        ),
        'port': (
            option_decoders.IntDecoder,
            {'default': 8000, 'min': 1, 'max': 65535},
        ),
        'model_id': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
        'tokenizer_id': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
    })
    return result


class WGServingInferenceServerConfigSpec(
    kubernetes_inference_server_spec.BaseInferenceServerConfigSpec
):
  """Spec for k8s wg-serving inference server configuration.

  Attributes:
    deployment_timeout: Timeout for waiting for the deployment to be available.
    hf_token: Access Token of HuggingFace Hub, it could be either token or uri
      to the secret in object storage (e.g. gs://bucket/path/to/token).
    model_name: Name of the model to use for the benchmark. options are
      available at
      https://github.com/kubernetes-sigs/wg-serving/blob/main/serving-catalog/README.md
    model_server: Name of the inference server to use for the benchmark. options
      are available at
      https://github.com/kubernetes-sigs/wg
    catalog_provider: Name of the catalog provider to use for the benchmark.
      options are available at
      https://github.com/kubernetes-sigs/wg
    catalog_components: Name of components of inference server to use for the
      benchmark. options are available at
      https://github.com/kubernetes-sigs/wg
    extra_deployment_args: Extra arguments to pass to the catalog CLI.
    hpa_min_replicas: Minimum replicas for HPA.
    hpa_max_replicas: Maximum replicas for HPA.
    hpa_target_value: Target value for the HPA custom metric.
    hpa_custom_metric_name: Name of the HPA custom metric.
  """

  INFERENCE_SERVER_TYPE = REGISTERED_INFERENCE_SERVER_TYPE

  def __init__(self, *args, **kwargs):
    self.hf_token: str
    self.model_name: str
    self.model_server: str
    self.catalog_provider: str
    self.catalog_components: str
    self.extra_deployment_args: dict[str, str] | None
    self.hpa_min_replicas: int
    self.hpa_max_replicas: int
    self.hpa_target_value: float
    self.hpa_custom_metric_name: str
    self.static_inference_server: (
        WGServingStaticInferenceServerConfigSpec | None
    )
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'deployment_timeout': (
            option_decoders.IntDecoder,
            {'default': _DEFAULT_INFERENCE_SERVER_DEPLOYMENT_TIMEOUT},
        ),
        'hf_token': (
            option_decoders.StringDecoder,
            {'none_ok': False},
        ),
        'model_name': (
            option_decoders.StringDecoder,
            {'default': 'llama3-8b'},
        ),
        'model_server': (
            option_decoders.StringDecoder,
            {'default': 'vllm'},
        ),
        'catalog_provider': (
            option_decoders.StringDecoder,
            {'default': 'gke'},
        ),
        'catalog_components': (
            option_decoders.StringDecoder,
            {'default': '1-L4'},
        ),
        'extra_deployment_args': (
            option_decoders.TypeVerifier,
            {'default': None, 'none_ok': True},
        ),
        'hpa_min_replicas': (
            option_decoders.IntDecoder,
            {'default': 1, 'min': 1},
        ),
        'hpa_max_replicas': (
            option_decoders.IntDecoder,
            {'default': 10, 'min': 1},
        ),
        'hpa_target_value': (
            option_decoders.FloatDecoder,
            {'default': 1},
        ),
        'hpa_custom_metric_name': (
            option_decoders.StringDecoder,
            {'default': _DEFAULT_HPA_CUSTOM_METRIC_NAME},
        ),
    })
    return result
