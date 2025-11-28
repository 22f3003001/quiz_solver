import asyncio
import sys
import io
import traceback
from typing import Dict, Any
import contextlib
import json

from utils.logger import setup_logger

logger = setup_logger(__name__)

async def execute_code(code: str, context: Dict[str, Any], timeout: int = 60) -> Dict:
    """
    Execute Python code in a controlled environment with timeout.
    
    Returns:
        {
            "success": bool,
            "result": Any,  # Return value or final variable
            "stdout": str,
            "stderr": str,
            "error": str | None
        }
    """
    try:
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _execute_sync, code, context),
            timeout=timeout
        )
        return result
    except asyncio.TimeoutError:
        return {
            "success": False,
            "result": None,
            "stdout": "",
            "stderr": "",
            "error": f"Code execution timeout ({timeout}s)"
        }
    except Exception as e:
        logger.error(f"Execution wrapper error: {e}")
        return {
            "success": False,
            "result": None,
            "stdout": "",
            "stderr": "",
            "error": str(e)
        }


def _execute_sync(code: str, context: Dict[str, Any]) -> Dict:
    """
    Synchronous code execution with stdout/stderr capture.
    """
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    
    # Build safe globals
    safe_globals = {
        "__builtins__": __builtins__,
        "context": context,
        "json": json,
        # Add commonly needed modules
        "pd": None,  # Will try to import
        "np": None,
        "plt": None,
        "requests": None,
        "re": None,
        "base64": None,
        "PIL": None,
    }
    
    # Try importing common libraries
    try:
        import pandas as pd
        safe_globals["pd"] = pd
    except ImportError:
        pass
    
    try:
        import numpy as np
        safe_globals["np"] = np
    except ImportError:
        pass
    
    try:
        import matplotlib.pyplot as plt
        safe_globals["plt"] = plt
    except ImportError:
        pass
    
    try:
        import requests
        safe_globals["requests"] = requests
    except ImportError:
        pass
    
    try:
        import re
        safe_globals["re"] = re
    except ImportError:
        pass
    
    try:
        import base64
        safe_globals["base64"] = base64
    except ImportError:
        pass
    
    try:
        from PIL import Image
        safe_globals["PIL"] = __import__("PIL")
        safe_globals["Image"] = Image
    except ImportError:
        pass
    
    local_vars = {}
    
    try:
        with contextlib.redirect_stdout(stdout_buffer), \
             contextlib.redirect_stderr(stderr_buffer):
            
            # Execute code
            exec(code, safe_globals, local_vars)
        
        # Get result - look for common result variables
        result = None
        for var_name in ["result", "answer", "output", "final_answer"]:
            if var_name in local_vars:
                result = local_vars[var_name]
                break
        
        # If no explicit result variable, try to get last assigned variable
        if result is None and local_vars:
            # Get last item that's not a module or function
            for key, value in reversed(list(local_vars.items())):
                if not callable(value) and not key.startswith('_'):
                    result = value
                    break
        print(result)
        return {
            "success": True,
            "result": result,
            "stdout": stdout_buffer.getvalue(),
            "stderr": stderr_buffer.getvalue(),
            "error": None
        }
        
    except Exception as e:
        error_msg = traceback.format_exc()
        logger.error(f"Code execution error:\n{error_msg}")
        
        return {
            "success": False,
            "result": None,
            "stdout": stdout_buffer.getvalue(),
            "stderr": stderr_buffer.getvalue(),
            "error": error_msg
        }