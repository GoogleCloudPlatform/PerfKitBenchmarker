"""Common utilities and base classes for all agent frameworks."""

import abc
import collections.abc
import json
from typing import Any, Self
import urllib.parse

from absl import logging
from google.cloud import storage
import pydantic


class ApiError(Exception):
  """Exception raised for API errors in agent frameworks."""


class AgentConfig(pydantic.BaseModel):
  """Base configuration for agents. Subclass this for custom fields."""

  model: str


class BasePromptConfig(pydantic.BaseModel):
  """Base Configuration for prompting an agent."""

  agent: str
  framework: str


class PromptConfig[AgentConfigT](BasePromptConfig):
  """Configuration for prompting an agent."""

  prompt: str
  output_dir: str
  session_id: str
  user_id: str
  run_uri: str
  agent_config: AgentConfigT

  @classmethod
  def create_for_initial_prompt(cls, deployment_config: Any) -> Self:
    return cls(
        agent=deployment_config.agent,
        framework=deployment_config.framework,
        prompt=deployment_config.initial_prompt,
        output_dir="",
        session_id="timetoreadysession",
        user_id="timetoreadyuser",
        run_uri=deployment_config.run_uri,
        agent_config=deployment_config.agent_config,
    )


class BaseEndpoint(abc.ABC):
  """An abstract interface for executing agents across different environments.

  This interface abstracts the underlying execution mechanism (e.g., Vertex AI,
  Docker streaming, or local evaluation).
  """

  @abc.abstractmethod
  def stream_execute(
      self, prompt_config: PromptConfig[Any]
  ) -> collections.abc.AsyncIterable[Any]:
    """Yields response chunks for a given prompt from the backend.

    Args:
      prompt_config: The configuration containing the prompt and session info.

    Yields:
      Data chunks streamed from the execution engine.
    """


class BaseAgentHandler(abc.ABC):
  """Generic glue interface for an agent to be deployed and run."""

  AGENT_CONFIG_CLS = AgentConfig

  @abc.abstractmethod
  def get_app_name_prefix(self) -> str:
    """Returns the prefix used for naming deployed apps and db paths."""

  @abc.abstractmethod
  def get_local_agent(self, config: Any, run_uri: str) -> Any:
    """Returns the agent instance wired for local Client VM execution."""

  @abc.abstractmethod
  def get_deployable_agent(self, config: Any, run_uri: str) -> Any:
    """Returns the agent instance wired for Cloud Deployment."""

  @abc.abstractmethod
  def create_endpoint(self, agent: Any) -> BaseEndpoint:
    """Wraps a generic proxy or local agent into a BaseEndpoint."""

  @abc.abstractmethod
  async def execute(
      self, endpoint: BaseEndpoint, prompt_config: PromptConfig[Any]
  ) -> str:
    """Invokes the agent pipeline and returns the response.

    Endpoint will always implement `stream_execute`.

    Args:
      endpoint: The endpoint object for the agent.
      prompt_config: The configuration containing the prompt and session info.
    """

  @abc.abstractmethod
  def export_results(
      self, output_dir: str, response_text: str, generic_metrics: dict[str, Any]
  ) -> None:
    """Exports results to destination."""


def upload_dict_to_gcs(payload: dict[str, Any], target_uri: str) -> None:
  """Writes dict to a local file. Uploads it to GCS if URI starts with gs://."""
  results_file = "results.json"
  with open(results_file, "w") as f:
    json.dump(payload, f, indent=2)

  logging.info("Wrote results to %s", results_file)
  if not target_uri.startswith("gs://"):
    return

  parsed_url = urllib.parse.urlparse(target_uri)
  bucket_name = parsed_url.netloc
  blob_path = parsed_url.path.strip("/")
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_path)
  blob.upload_from_filename(results_file)
  logging.info("Successfully uploaded to %s", target_uri)
