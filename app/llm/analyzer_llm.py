# app/llm/analyzer_llm.py
import os
import json
import asyncio
from typing import Dict, Any
import httpx
from pydantic import ValidationError
from pydantic import AnyUrl

from .schema import AnalyzerResult

AIPIPE_URL = "https://aipipe.org/openrouter/v1/chat/completions"
# AIPIPE_TOKEN = os.getenv("AIPIPE_TOKEN")  # set this in env
AIPIPE_TOKEN =os.getenv("AIPIPE_TOKEN")

# load prompt template
PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "analyzer_prompt.txt")
with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    PROMPT_TEMPLATE = f.read()


async def call_analyzer_llm(
    page_text: str,
    links: list,
    tables_repr: list,
    question_context: str,
    constraints: dict,
    model: str = "openai/gpt-4.1-nano",
    timeout: int = 30,
) -> AnalyzerResult:
    """
    Sends the analysis prompt to AiPipe (OpenRouter) and returns a validated AnalyzerResult.
    """

    if not AIPIPE_TOKEN:
        
        raise RuntimeError("AIPIPE_TOKEN not set in environment")

    user_payload = {
        "page_text": page_text or "",
        "links": links or [],
        "tables": tables_repr or [],
        "question_context": question_context or "",
        "constraints": constraints or {},
    }

    # Compose a short user message that contains the inputs as JSON to keep token use predictable
    user_message = (
        "Here are the inputs (JSON):\n"
        + json.dumps(user_payload, ensure_ascii=False, indent=None)
        + "\n\nProduce the analyzer JSON per the schema in the system instructions."
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": PROMPT_TEMPLATE},
            {"role": "user", "content": user_message},
        ],
        # You can tune temperature/other params here as needed
        "temperature": 0.0,
        "max_tokens": 1200,
    }

    headers = {
        "Authorization": f"Bearer {AIPIPE_TOKEN}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(AIPIPE_URL, headers=headers, json=body)
        r.raise_for_status()
        resp = r.json()

    # The exact structure depends on OpenRouter response shape. Try to extract assistant message text:
    # Commonly: resp["choices"][0]["message"]["content"]
    text = None
    try:
        text = resp["choices"][0]["message"]["content"]
    except Exception:
        # fallback for other shapes
        # attempt to find any assistant text
        for choice in resp.get("choices", []):
            msg = choice.get("message") or choice.get("delta") or {}
            content = msg.get("content")
            if content:
                text = content
                break

    if not text:
        raise RuntimeError(f"No assistant content in response: {resp}")

    # The model is instructed to return JSON ONLY, but sometimes it may include surrounding whitespace/newlines.
    # Extract first JSON object from the response text.
    json_text = _extract_json_from_text(text)
    if json_text is None:
        raise RuntimeError("Failed to extract JSON from LLM response:\n" + text[:1000])

    # Parse JSON
    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError as e:
        # provide debug info
        raise RuntimeError(f"LLM returned invalid JSON: {e}\nRaw text:\n{text}")

    # Validate with Pydantic
    try:
        result = AnalyzerResult.parse_obj(parsed)
    except ValidationError as e:
        raise RuntimeError(f"Analyzer JSON failed validation: {e}\nParsed JSON: {parsed}")
    print("analyzer result",result)
    return result


def _extract_json_from_text(text: str) -> str:
    """
    Attempt to find the first JSON object in the given text.
    Returns JSON string or None.
    """
    # Find the first { and the matching closing } using simple stack
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
                    # return substring from start to i inclusive
                    return text[start : i + 1]
            else:
                # unmatched closing curly: ignore
                pass
        i += 1
    return None

