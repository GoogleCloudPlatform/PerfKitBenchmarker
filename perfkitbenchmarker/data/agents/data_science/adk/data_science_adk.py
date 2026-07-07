"""Data Science Multi-Agent System using ADK."""

import os
from typing import Any, override

import adk_utils
import common_utils
import data_science_adk_agents


class AgentHandler(adk_utils.AdkAgentHandler):
  """Standard interface for a data science agent written in ADK."""

  @override
  def get_app_name_prefix(self) -> str:
    return "data_science_adk"

  @override
  def _create_agent(self, config: common_utils.AgentConfig) -> Any:
    return data_science_adk_agents.create_supervisor_agent(config.model)

  @override
  def export_results(
      self, output_dir: str, response_text: str, generic_metrics: dict[str, Any]
  ) -> None:
    results = {
        "metrics": generic_metrics,
        "artifacts": {"answer": os.path.join(output_dir, "answer.txt")},
        "response": response_text,
    }
    target_path = os.path.join(output_dir, "results.json")
    common_utils.upload_dict_to_gcs(results, target_path)

  @override
  async def execute(
      self,
      endpoint: common_utils.BaseEndpoint,
      prompt_config: common_utils.PromptConfig,
  ) -> str:
    # Data science specific env vars
    os.environ["GOOGLE_API_USE_MTLS_ENDPOINT"] = "never"
    os.environ["GOOGLE_API_USE_CLIENT_CERTIFICATE"] = "false"
    return await adk_utils.AdkAgentHandler.execute(
        self, endpoint, prompt_config
    )
