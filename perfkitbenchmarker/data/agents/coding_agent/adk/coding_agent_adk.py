"""Coding Agent System using ADK."""

import os
from typing import Any, override

import adk_utils
import coding_agent_adk_agents
import common_utils


class AgentHandler(adk_utils.AdkAgentHandler):
  """Standard interface for a coding agent written in ADK."""

  @override
  def get_app_name_prefix(self) -> str:
    return "coding_agent_adk"

  @override
  def _create_agent(self, config: common_utils.AgentConfig) -> Any:
    return coding_agent_adk_agents.create_coding_agent(config.model)

  @override
  def export_results(
      self, output_dir: str, response_text: str, generic_metrics: dict[str, Any]
  ) -> None:
    results = {
        "metrics": generic_metrics,
        "artifacts": {"resulting_patch": os.path.join(output_dir, "fix.patch")},
        "response": response_text,
    }
    target_path = os.path.join(output_dir, "results.json")
    common_utils.upload_dict_to_gcs(results, target_path)
