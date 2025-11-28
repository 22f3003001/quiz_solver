import os
import asyncio
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ValidationError
import httpx
from datetime import datetime, timedelta

from core.agent_loop import run_quiz_agent
from utils.logger import setup_logger

app = FastAPI(title="LLM Quiz Solver")
logger = setup_logger(__name__)

# Load secret from environment
EXPECTED_SECRET = os.getenv("QUIZ_SECRET")
EXPECTED_EMAIL = os.getenv("QUIZ_EMAIL")


class QuizRequest(BaseModel):
    email: str
    secret: str
    url: str

class QuizResponse(BaseModel):
    status: str
    message: str

@app.post("/quiz", response_model=QuizResponse)
async def handle_quiz(request: QuizRequest):
    """
    Main endpoint to receive quiz tasks.
    """
    start_time = datetime.now()
    deadline = start_time + timedelta(minutes=3)
    
    # Verify secret
    print(EXPECTED_SECRET,EXPECTED_EMAIL)
    if request.secret != EXPECTED_SECRET:
        logger.warning(f"Invalid secret from {request.email}")
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    if request.email != EXPECTED_EMAIL:
        logger.warning(f"Email mismatch: {request.email}")
        raise HTTPException(status_code=403, detail="Email mismatch")
    
    logger.info(f"Received quiz task: {request.url}")
    
    try:
        # Run the agent loop
        await run_quiz_agent(
            quiz_url=request.url,
            email=request.email,
            secret=request.secret,
            deadline=deadline
        )
        
        return QuizResponse(
            status="success",
            message="Quiz processing initiated"
        )
    
    except Exception as e:
        logger.error(f"Quiz processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    logger.error(f"Validation error: {exc}")
    raise HTTPException(status_code=400, detail="Invalid JSON payload")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 7860))  # HuggingFace Space uses 7860

    uvicorn.run(app, host="0.0.0.0", port=port)
