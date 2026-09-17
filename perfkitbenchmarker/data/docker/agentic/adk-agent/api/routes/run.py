"""General agent execution route."""

import logging
from typing import Any

from api import utils
import fastapi
import pydantic

logger = logging.getLogger(__name__)
router = fastapi.APIRouter()


class RunRequest(pydantic.BaseModel):
  """Request parameters for running the agent."""

  prompt: str


@router.post("/run")
async def run_agent_endpoint(req: RunRequest) -> dict[str, Any]:
  """Runs the agent with the provided prompt."""
  try:
    output = await utils.run_agent(req.prompt)
    return {"response": output}
  except Exception as e:  # pylint: disable=broad-exception-caught
    logger.exception("Agent run failed: %s", e)
    raise fastapi.HTTPException(status_code=500, detail=str(e)) from e
