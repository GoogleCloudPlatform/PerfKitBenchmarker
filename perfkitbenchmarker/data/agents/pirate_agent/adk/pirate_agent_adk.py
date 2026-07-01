"""Pirate Agent using ADK."""

import os
from typing import Any, override
import urllib.parse

from absl import logging
import adk_utils
import common_utils
from google.adk.agents import llm_agent
from google.cloud import storage


class AgentHandler(adk_utils.AdkAgentHandler):
  """Standard interface for a pirate agent written in ADK."""

  @override
  def get_app_name_prefix(self) -> str:
    return "pirate_agent_adk"

  @override
  def _create_agent(self, config: common_utils.AgentConfig) -> Any:
    return llm_agent.Agent(
        name="pirate_supervisor",
        description="A pirate agent.",
        instruction=(
            "You are a pirate. You must reply back to the user's prompt by"
            " translating it to pirate speak."
        ),
        model=config.model,
    )

  @override
  def export_results(
      self, output_dir: str, response_text: str, generic_metrics: dict[str, Any]
  ) -> None:
    answer_file = "answer.txt"
    with open(answer_file, "w") as f:
      f.write(response_text)

    target_answer_path = os.path.join(output_dir, answer_file)

    if target_answer_path.startswith("gs://"):
      parsed_url = urllib.parse.urlparse(target_answer_path)
      bucket_name = parsed_url.netloc
      blob_path = parsed_url.path.strip("/")
      storage_client = storage.Client()
      bucket = storage_client.bucket(bucket_name)
      blob = bucket.blob(blob_path)
      blob.upload_from_filename(answer_file)
      logging.info("Successfully uploaded answer to %s", target_answer_path)

    results = {
        "metrics": generic_metrics,
        "artifacts": {"answer": target_answer_path},
        "response": response_text,
    }
    target_path = os.path.join(output_dir, "results.json")
    common_utils.upload_dict_to_gcs(results, target_path)
