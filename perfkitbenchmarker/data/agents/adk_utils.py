"""Common utilities and base classes for ADK-based agents."""

import abc
import collections
from typing import Any, override

from absl import logging
import common_utils
from google.adk.events import event as adk_event
from google.adk.sessions import sqlite_session_service
from google.genai import errors
from google.genai import types
from vertexai import agent_engines


class AdkEndpoint(common_utils.BaseEndpoint):
  """An implementation of BaseEndpoint for ADK framework applications.

  This wraps an underlying ADK app (which can be a local object or a deployed
  Vertex proxy) to expose the standard execution interface required by the
  framework.
  """

  def __init__(self, adk_app: Any):
    self.adk_app = adk_app

  async def stream_execute(self, prompt_config: common_utils.PromptConfig):
    try:
      await self.adk_app.async_get_session(
          user_id=prompt_config.user_id, session_id=prompt_config.session_id
      )
    except (RuntimeError, errors.ClientError):
      await self.adk_app.async_create_session(
          user_id=prompt_config.user_id, session_id=prompt_config.session_id
      )

    async for chunk in self.adk_app.async_stream_query(
        message=prompt_config.prompt,
        session_id=prompt_config.session_id,
        user_id=prompt_config.user_id,
    ):
      yield chunk


class AdkAgentHandler(common_utils.BaseAgentHandler):
  """Base class for handling ADK agent execution and results."""

  @abc.abstractmethod
  def _create_agent(self, config: common_utils.AgentConfig) -> Any:
    """Returns the base ADK agent instance."""

  @override
  def get_local_agent(
      self, config: common_utils.AgentConfig, run_uri: str
  ) -> Any:
    """Returns the local agent instance (an AdkApp with SQLite session)."""
    agent = self._create_agent(config)
    db_path = "/tmp/pkb_agent_sessions.db"
    app_name = f"{self.get_app_name_prefix()}_{run_uri}"

    return agent_engines.AdkApp(
        agent=agent,
        app_name=app_name,
        session_service_builder=lambda: sqlite_session_service.SqliteSessionService(
            db_path=db_path
        ),
    )

  @override
  def get_deployable_agent(
      self, config: common_utils.AgentConfig, run_uri: str
  ) -> Any:
    """Returns the agent instance for deployment (an AdkApp without SQLite)."""
    agent = self._create_agent(config)
    app_name = f"{self.get_app_name_prefix()}_{run_uri}"

    return agent_engines.AdkApp(
        agent=agent,
        app_name=app_name,
    )

  def create_endpoint(self, agent: Any) -> common_utils.BaseEndpoint:
    """Wraps a generic proxy or local agent into an AdkEndpoint."""
    return AdkEndpoint(agent)

  @abc.abstractmethod
  def export_results(
      self, output_dir: str, response_text: str, generic_metrics: dict[str, Any]
  ) -> None:
    """Exports the results and metrics."""

  def _extract_usage_tokens(
      self, usage: types.GenerateContentResponseUsageMetadata
  ) -> dict[str, int]:
    """Extracts input and output token counts from usage metadata."""
    output_tokens = (usage.candidates_token_count or 0) + (
        usage.thoughts_token_count or 0
    )
    return {
        "input_tokens": usage.prompt_token_count or 0,
        "output_tokens": output_tokens,
    }

  async def execute(
      self,
      endpoint: common_utils.BaseEndpoint,
      prompt_config: common_utils.PromptConfig,
  ) -> common_utils.ExecutionResult:
    """Prompts the agent and streams the response.

    Args:
      endpoint: The BaseEndpoint instance.
      prompt_config: The prompt and session configuration.

    Returns:
      ExecutionResult containing the combined response text and token metrics.

    Raises:
      common_utils.ApiError: If the ADK returns an API error event.
    """
    logging.info("=== Starting Pipeline ===")

    raw_output = []
    token_totals: dict[str, int] = collections.defaultdict(int)

    async for raw_event in endpoint.stream_execute(prompt_config=prompt_config):
      if isinstance(raw_event, dict):
        # Generic check for API errors in events
        if "code" in raw_event and "message" in raw_event:
          raise common_utils.ApiError(
              f"API Error (code {raw_event['code']}): {raw_event['message']}"
          )
        event = adk_event.Event.model_validate(raw_event)
      else:
        event = raw_event
      usage_tokens = None
      if event.usage_metadata:
        usage_tokens = self._extract_usage_tokens(event.usage_metadata)
        for token_metric, count in usage_tokens.items():
          token_totals[token_metric] += count

      self._print_event(event, usage_tokens=usage_tokens)

      # Collect response text for results.json
      if not event.content or not event.content.parts:
        continue

      for part in event.content.parts:
        if part.text:
          raw_output.append(part.text)

    response_text = "".join(raw_output)
    return common_utils.ExecutionResult(
        response_text=response_text,
        token_metrics=dict(token_totals),
    )

  def _print_event(
      self,
      event: adk_event.Event,
      usage_tokens: dict[str, int] | None = None,
  ) -> None:
    """Logs an ADK event."""
    if usage_tokens:
      usage_str = ", ".join(f"{k}: {v}" for k, v in usage_tokens.items())
      logging.info("%s > [Usage: %s]", event.author or "model", usage_str)

    if not event.content or not event.content.parts:
      return

    text_buffer = []

    def flush_text() -> None:
      if not text_buffer:
        return
      combined_text = "".join(text_buffer)
      logging.info("%s > %s", event.author, combined_text)
      text_buffer.clear()

    for part in event.content.parts:
      if part.text:
        text_buffer.append(part.text)
      else:
        flush_text()

      if part.function_call:
        logging.info(
            "%s > [Calling tool: %s(%s)]",
            event.author,
            part.function_call.name,
            part.function_call.args,
        )
      elif part.function_response:
        logging.info(
            "%s > [Tool result: %s]",
            event.author,
            part.function_response.response,
        )
      elif part.executable_code:
        lang = part.executable_code.language or "code"
        logging.info("%s > [Executing %s code...]", event.author, lang)
      elif part.code_execution_result:
        output = part.code_execution_result.output or "result"
        logging.info("%s > [Code output: %s]", event.author, output)
      elif part.inline_data:
        mime_type = part.inline_data.mime_type or "data"
        logging.info("%s > [Inline data: %s]", event.author, mime_type)
      elif part.file_data:
        uri = part.file_data.file_uri or "file"
        logging.info("%s > [File: %s]", event.author, uri)

    flush_text()
