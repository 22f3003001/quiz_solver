import os
import json
import asyncio
from typing import Dict, Any, List
import httpx

from utils.logger import setup_logger

logger = setup_logger(__name__)

AIPIPE_URL = "https://aipipe.org/openrouter/v1/chat/completions"
AIPIPE_TOKEN = os.getenv("AIPIPE_TOKEN")
# Load agent prompt
AGENT_PROMPT_PATH = os.path.join(
    os.path.dirname(__file__), "prompts", "agent_prompt.txt"
)
with open(AGENT_PROMPT_PATH, "r", encoding="utf-8") as f:
    AGENT_PROMPT_TEMPLATE = f.read()


async def call_agent_llm(
    analysis: Any,  # AnalyzerResult
    downloaded_files: Dict[str, str],
    file_metadata: Dict[str, Any],
    execution_history: List[Dict],
    iteration: int,
    model: str = "openai/gpt-4.1-nano",
    timeout: int = 45,
) -> Dict:
    """
    Call the agent LLM to generate code or provide final answer.
    Now includes file metadata for better context.
    
    Returns:
        {
            "code": str | None,
            "final_answer": Any | None,
            "reasoning": str
        }
    """
    if not AIPIPE_TOKEN:
        raise RuntimeError("AIPIPE_TOKEN not set")
    
    # Simplify metadata for prompt - NO SUMMARIZATION, just limit depth
    simplified_metadata = {}
    for url, metadata in file_metadata.items():
        # Create a shallow copy with controlled data size
        meta_copy = {
            "filepath": metadata.get("filepath"),
            "type": metadata.get("type"),
            "extension": metadata.get("extension")
        }
        
        # Copy everything else but limit preview sizes
        for key, value in metadata.items():
            if key in ["filepath", "type", "extension"]:
                continue
            
            if key == "data_preview" and isinstance(value, list):
                # Limit to 3 rows
                meta_copy[key] = value[:3]
            elif key == "tables" and isinstance(value, list):
                # Limit table data to 3 rows each
                limited_tables = []
                for table in value:
                    table_copy = table.copy()
                    if "data" in table_copy and isinstance(table_copy["data"], list):
                        table_copy["data"] = table_copy["data"][:3]
                    limited_tables.append(table_copy)
                meta_copy[key] = limited_tables
            elif key == "full_data":
                # Skip full_data in prompt, but mention it exists
                meta_copy["has_full_data"] = value is not None
            elif key == "text_samples" and isinstance(value, list):
                meta_copy[key] = value[:2]  # Only 2 text samples
            else:
                meta_copy[key] = value
        
        simplified_metadata[url] = meta_copy
    
    # Build compact context
    user_message = f"""CONTEXT:
Question: {analysis.question}
Task Type: {analysis.task_type}
Answer Expected: {analysis.final_answer_expected}
Iteration: {iteration}/3

STEPS:
{json.dumps([{"step": s.step_number, "action": s.action, "details": s.details} for s in analysis.steps], indent=2)}

FILES AVAILABLE:
{json.dumps({"files": list(downloaded_files.keys())}, indent=2)}

FILE METADATA (complete structure, use as-is):
{json.dumps(simplified_metadata, indent=2, default=str)}

EXECUTION HISTORY (learn from errors):
{json.dumps(execution_history[-2:] if len(execution_history) > 2 else execution_history, indent=2, default=str)}

TASK:
Generate Python code to solve this task.

CRITICAL INSTRUCTIONS:
1. Access files via: context["downloaded_files"]["<url>"]
2. Access metadata via: context["file_metadata"]["<url>"]
3. The metadata structure is EXACTLY as shown above
4. For PDFs, tables are in: context["file_metadata"]["<url>"]["tables"]
5. Each table has: {{"page": int, "shape": tuple, "columns": list, "data": list}}
6. Store result in variable named 'answer' or 'result'
7. If previous iteration failed, FIX the error shown in execution_history
8. If you can answer directly from metadata, return final_answer instead of code

RESPOND WITH JSON ONLY (no markdown, no backticks):
{{"code": "<python code>" OR null, "final_answer": <value> OR null, "reasoning": "..."}}
"""

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": AGENT_PROMPT_TEMPLATE},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.1,
        "max_tokens": 2000,
    }

    headers = {
        "Authorization": f"Bearer {AIPIPE_TOKEN}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(AIPIPE_URL, headers=headers, json=body)
        r.raise_for_status()
        resp = r.json()

    # Extract content
    text = None
    try:
        text = resp["choices"][0]["message"]["content"]
    except Exception:
        for choice in resp.get("choices", []):
            msg = choice.get("message") or choice.get("delta") or {}
            content = msg.get("content")
            if content:
                text = content
                break

    if not text:
        raise RuntimeError(f"No content in agent response: {resp}")

    # Parse JSON
    print("agent_response",text)
    json_text = _extract_json(text)
    if not json_text:
        # Fallback: treat entire text as code
        logger.warning("Agent didn't return JSON, treating as code")
        return {
            "code": text,
            "final_answer": None,
            "reasoning": "Direct code output"
        }

    try:
        parsed = json.loads(json_text)
        return {
            "code": parsed.get("code"),
            "final_answer": parsed.get("final_answer"),
            "reasoning": parsed.get("reasoning", "")
        }
    except json.JSONDecodeError as e:
        logger.error(f"Agent JSON parse error: {e}\nText: {text}")
        # Return as code
        return {
            "code": text,
            "final_answer": None,
            "reasoning": "Parse error fallback"
        }


def _extract_json(text: str) -> str:
    """Extract first JSON object from text."""
    start = text.find("{")
    if start == -1:
        return None
    stack = []
    i = start
    while i < len(text):
        ch = text[i]
        if ch == "{":
            stack.append("{")
        elif ch == "}":
            if stack:
                stack.pop()
                if not stack:
                    return text[start : i + 1]
        i += 1

    return None
