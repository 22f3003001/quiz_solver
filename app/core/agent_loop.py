import asyncio
import json
from datetime import datetime
from typing import Dict, Any, Optional
import httpx

from core.extractor import extract_quiz_content
from llm.analyzer_llm import call_analyzer_llm
from llm.agent_llm import call_agent_llm
from core.code_executor import execute_code
from core.fetcher import download_resource
from core.preprocessor import preprocess_downloaded_files
from utils.logger import setup_logger

logger = setup_logger(__name__)

# Safety limits to prevent infinite loops
MAX_TOTAL_ATTEMPTS = 30  # Hard limit across all questions (allow for retries)
MAX_ATTEMPTS_PER_URL = 2  # Try each question max TWICE (original + 1 retry)
MAX_QUESTION_RETRIES = 1  # Allow 1 retry per wrong answer

async def run_quiz_agent(
    quiz_url: str,
    email: str,
    secret: str,
    deadline: datetime,
    max_iterations: int = 2  # Agent code generation iterations
):
    """
    Main agent loop that orchestrates the entire quiz-solving process.
    Protected against infinite loops with multiple safety mechanisms.
    """
    current_url = quiz_url
    total_attempts = 0
    url_attempt_counts = {}  # Track attempts per URL
    visited_urls = []  # Track URL sequence for debugging
    
    while current_url and datetime.now() < deadline:
        total_attempts += 1
        
        # Safety check 1: Global attempt limit
        if total_attempts > MAX_TOTAL_ATTEMPTS:
            logger.error(f"Exceeded maximum total attempts ({MAX_TOTAL_ATTEMPTS})")
            break
        
        # Safety check 2: Per-URL attempt limit
        url_attempt_counts[current_url] = url_attempt_counts.get(current_url, 0) + 1
        if url_attempt_counts[current_url] > MAX_ATTEMPTS_PER_URL:
            logger.warning(f"Attempted {current_url} {MAX_ATTEMPTS_PER_URL} times, skipping")
            break
        
        # Safety check 3: Deadline
        time_remaining = (deadline - datetime.now()).total_seconds()
        if time_remaining < 10:  # Less than 10 seconds left
            logger.warning("Less than 10 seconds remaining, stopping")
            break
        
        visited_urls.append(current_url)
        logger.info(f"Attempt {total_attempts}: Processing {current_url}")
        logger.info(f"Time remaining: {time_remaining:.1f}s")
        
        try:
            # Step 1: Extract content from quiz page
            logger.info("Step 1: Extracting quiz content...")
            extraction = await extract_quiz_content(current_url)
            
            if not extraction:
                logger.error("Failed to extract quiz content")
                break
            
            # Step 2: Analyze with Analyzer LLM
            logger.info("Step 2: Analyzing task...")
            analysis = await call_analyzer_llm(
                page_text=extraction.get("page_text", ""),
                links=extraction.get("links", []),
                tables_repr=extraction.get("tables", []),
                question_context=extraction.get("question", ""),
                constraints={
                    "time_remaining": time_remaining,
                    "max_iterations": max_iterations
                }
            )
            
            logger.info(f"Task type: {analysis.task_type}")
            logger.info(f"Question: {analysis.question}")
            logger.info(f"Steps: {len(analysis.steps)}")
            
            # Step 3: Download resources if needed
            downloaded_files = {}
            for resource in analysis.resources:
                if resource.download and resource.source_url:
                    logger.info(f"Downloading: {resource.source_url}")
                    file_path = await download_resource(str(resource.source_url))
                    if file_path:
                        downloaded_files[str(resource.source_url)] = file_path
                    else:
                        logger.warning(f"Failed to download: {resource.source_url}")
            
            # Step 4: Preprocess downloaded files
            file_metadata = {}
            if downloaded_files:
                logger.info("Step 4: Preprocessing downloaded files...")
                try:
                    file_metadata = await preprocess_downloaded_files(downloaded_files, analysis)
                    logger.info(f"Extracted metadata for {len(file_metadata)} files")
                except Exception as e:
                    logger.error(f"Preprocessing failed: {e}", exc_info=True)
                    file_metadata = {}
            
            # Step 5: Agent loop (limited iterations)
            answer = None
            execution_history = []
            
            for iteration in range(1, max_iterations + 1):
                logger.info(f"Agent iteration {iteration}/{max_iterations}")
                
                # Check time before each iteration
                if datetime.now() >= deadline:
                    logger.warning("Deadline reached during agent loop!")
                    break
                
                # Call agent LLM
                agent_response = await call_agent_llm(
                    analysis=analysis,
                    downloaded_files=downloaded_files,
                    file_metadata=file_metadata,
                    execution_history=execution_history,
                    iteration=iteration
                )
                
                # Check if agent returned final answer without code
                if agent_response.get("final_answer") is not None:
                    answer = agent_response["final_answer"]
                    logger.info(f"Agent provided final answer: {answer}")
                    break
                
                # Execute code
                if agent_response.get("code"):
                    logger.info("Executing generated code...")
                    
                    exec_result = await execute_code(
                        code=agent_response["code"],
                        context={
                            "downloaded_files": downloaded_files,
                            "file_metadata": file_metadata,
                            "analysis": analysis.dict()
                        }
                    )
                    
                    execution_history.append({
                        "iteration": iteration,
                        "code": agent_response["code"],
                        "result": exec_result
                    })
                    
                    # Check if execution gave us the answer
                    if exec_result.get("success") and exec_result.get("result") is not None:
                        answer = exec_result["result"]
                        logger.info(f"Got answer from execution: {answer}")
                        break
                    elif not exec_result.get("success"):
                        logger.warning(f"Execution failed: {exec_result.get('error')}")
                        # Continue to next iteration with error context
                else:
                    logger.warning("Agent didn't provide code")
                    break
            
            # Step 6: Submit answer
            if answer is not None and analysis.submission_link:
                logger.info(f"Submitting answer: {answer} to {analysis.submission_link}")
                submission_response = await submit_answer(
                    url=str(analysis.submission_link),
                    email=email,
                    secret=secret,
                    quiz_url=current_url,
                    answer=answer
                )
                
                if submission_response.get("correct"):
                    logger.info("✓ Answer correct!")
                    next_url = submission_response.get("url")
                    
                    if next_url:
                        if next_url == current_url:
                            logger.warning("Server returned same URL after correct answer, stopping")
                            break
                        
                        logger.info(f"Moving to next quiz: {next_url}")
                        current_url = next_url
                    else:
                        logger.info("Quiz completed - no more URLs!")
                        break
                else:
                    reason = submission_response.get("reason", "Unknown")
                    logger.warning(f"✗ Answer incorrect: {reason}")
                    
                    # Check if we got next URL
                    next_url = submission_response.get("url")
                    
                    # Check how many times we've tried this URL
                    current_attempt = url_attempt_counts.get(current_url, 0)
                    
                    if next_url and next_url != current_url:
                        # Got a NEW URL - move forward regardless of wrong answer
                        logger.info(f"Got new URL, moving forward: {next_url}")
                        current_url = next_url
                    elif current_attempt < MAX_ATTEMPTS_PER_URL:
                        # Same URL or no URL, but we can retry once
                        logger.info(f"Retrying same question (attempt {current_attempt + 1}/{MAX_ATTEMPTS_PER_URL})")
                        # current_url stays the same, will retry on next loop
                        continue
                    else:
                        # Already tried max times
                        logger.warning(f"Already attempted {current_attempt} times, moving on")
                        if next_url:
                            logger.info(f"Moving to: {next_url}")
                            current_url = next_url
                        else:
                            logger.warning("No next URL provided - stopping")
                            break
            else:
                if answer is None:
                    logger.error("No answer generated")
                if not analysis.submission_link:
                    logger.error("No submission link found")
                break
                
        except Exception as e:
            logger.error(f"Error in agent loop: {e}", exc_info=True)
            # Don't break immediately - might be temporary error
            # url_attempt_counts will prevent infinite retries
            continue
    
    # Final summary
    logger.info("=" * 60)
    logger.info("Quiz agent finished")
    logger.info(f"Total attempts: {total_attempts}")
    logger.info(f"Unique URLs visited: {len(set(visited_urls))}")
    logger.info(f"URL sequence: {' → '.join(visited_urls[-5:])}")  # Last 5 URLs
    logger.info("=" * 60)


async def submit_answer(
    url: str,
    email: str,
    secret: str,
    quiz_url: str,
    answer: Any
) -> Dict:
    """
    Submit answer to the quiz endpoint.
    """
    payload = {
        "email": email,
        "secret": secret,
        "url": quiz_url,
        "answer": answer
    }
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Submission failed: {e}")
            return {"correct": False, "reason": str(e)}