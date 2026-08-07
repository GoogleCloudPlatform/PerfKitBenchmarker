import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from api.utils import run_agent

logger = logging.getLogger(__name__)
router = APIRouter()

class RunRequest(BaseModel):
    prompt: str = "start"

@router.post("/run")
async def run_agent_endpoint(req: RunRequest):
    try:
        output = await run_agent(req.prompt)
        return {"response": output}
    except Exception as e:
        logger.exception("Agent run failed")
        raise HTTPException(status_code=500, detail=str(e))
