import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from api.utils import run_agent

logger = logging.getLogger(__name__)
router = APIRouter()

class RunRequest(BaseModel):
    prompt: str

@router.post("/run")
async def run_agent_endpoint(req: RunRequest):
    try:
        output = await run_agent(req.prompt)
        return {"response": output}
    except Exception as e:
        logger.exception("Agent run failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
