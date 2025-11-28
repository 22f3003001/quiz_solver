# app/llm/schema.py
from typing import List, Optional
from pydantic import BaseModel, HttpUrl

class Resource(BaseModel):
    type: str  # pdf | csv | html | image | table | json | url
    source_url: Optional[HttpUrl] = None
    description: Optional[str] = None
    required_pages: Optional[List[int]] = None
    required_selectors: Optional[List[str]] = None
    required_tables: Optional[List[int]] = None
    download: bool = False

class Step(BaseModel):
    step_number: int
    action: str  # e.g., "download", "extract_table", "clean", "analyze", "visualize", "submit"
    details: Optional[str] = None
    needs_code: bool = False
    code_language: Optional[str] = None
    code_description: Optional[str] = None

class AnalyzerResult(BaseModel):
    # top-level
    question: str
    submission_link: Optional[HttpUrl] = None

    resources: List[Resource] = []
    task_type: str  # data_sourcing | data_preparation | data_analysis | visualization | multi_step | unknown

    # ordered steps to perform (1..n)
    steps: List[Step] = []

    # expected final answer specification
    final_answer_expected: Optional[str] = None  # "number"|"string"|"boolean"|"json"|"base64_image" etc.

    # small metadata / hints (optional but present)
    priority: Optional[int] = 1
    notes: Optional[str] = None
