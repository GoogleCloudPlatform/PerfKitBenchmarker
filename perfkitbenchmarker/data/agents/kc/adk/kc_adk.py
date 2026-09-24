"""Knowledge Catalog Agent using ADK."""

import os
from typing import Any, override

import adk_utils
import common_utils
from google.adk.agents import llm_agent


class AgentHandler(adk_utils.AdkAgentHandler):
  """Standard interface for a Knowledge Catalog agent written in ADK."""

  @override
  def get_app_name_prefix(self) -> str:
    return "kc_adk"

  @override
  def _create_agent(self, config: common_utils.AgentConfig) -> Any:
    return llm_agent.Agent(
        name="kc_supervisor",
        description="A KC agent.",
        instruction="You are a helpful assistant.",
        model=config.model,
    )

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
