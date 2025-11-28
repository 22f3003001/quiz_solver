import os
import json
import pandas as pd
import pdfplumber
from typing import Dict, Any, Optional
from pathlib import Path

from utils.logger import setup_logger

logger = setup_logger(__name__)

async def preprocess_downloaded_files(
    downloaded_files: Dict[str, str],
    analysis: Any  # AnalyzerResult
) -> Dict[str, Any]:
    """
    Extract metadata and sample data from downloaded files.
    Returns dict with file metadata to pass to agent.
    """
    file_metadata = {}
    
    for url, filepath in downloaded_files.items():
        logger.info(f"Preprocessing: {filepath}")
        
        try:
            file_ext = Path(filepath).suffix.lower()
            
            if file_ext == '.csv':
                metadata = await _extract_csv_metadata(filepath)
            elif file_ext == '.pdf':
                metadata = await _extract_pdf_metadata(filepath, analysis)
            elif file_ext in ['.xlsx', '.xls']:
                metadata = await _extract_excel_metadata(filepath)
            elif file_ext == '.json':
                metadata = await _extract_json_metadata(filepath)
            elif file_ext in ['.txt', '.log']:
                metadata = await _extract_text_metadata(filepath)
            else:
                metadata = {"type": "unknown", "size": os.path.getsize(filepath)}
            print("metadata",metadata)    
            
            file_metadata[url] = {
                "filepath": filepath,
                "extension": file_ext,
                **metadata
            }
            
        except Exception as e:
            logger.error(f"Error preprocessing {filepath}: {e}")
            file_metadata[url] = {
                "filepath": filepath,
                "error": str(e)
            }
    
    return file_metadata


async def _extract_csv_metadata(filepath: str) -> Dict[str, Any]:
    """Extract metadata from CSV file."""
    try:
        # Read CSV
        df = pd.read_csv(filepath)
        
        rows, cols = df.shape
        
        # Determine what to return
        if rows <= 10 and cols <= 10:
            # Small dataset - return everything
            data_preview = df.to_dict(orient='records')
        else:
            # Large dataset - return sample
            data_preview = df.head(5).to_dict(orient='records')
        
        metadata = {
            "type": "csv",
            "shape": {"rows": rows, "columns": cols},
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "data_preview": data_preview,
            "full_data": df.to_dict(orient='records') if rows <= 10 and cols <= 10 else None,
            "summary_stats": df.describe().to_dict() if df.select_dtypes(include='number').shape[1] > 0 else None
        }
        print("csv_metadata",metadata)
        
        return metadata
        
    except Exception as e:
        logger.error(f"CSV extraction error: {e}")
        return {"type": "csv", "error": str(e)}


async def _extract_pdf_metadata(filepath: str, analysis: Any) -> Dict[str, Any]:
    """Extract metadata from PDF file."""
    try:
        with pdfplumber.open(filepath) as pdf:
            total_pages = len(pdf.pages)
            
            # Determine which pages to extract
            pages_to_extract = []
            if analysis and analysis.resources:
                for resource in analysis.resources:
                    if resource.required_pages:
                        pages_to_extract.extend(resource.required_pages)
            
            # Default to first 2 pages if not specified
            if not pages_to_extract:
                pages_to_extract = list(range(min(2, total_pages)))
            
            # Extract tables from specified pages
            tables_data = []
            text_data = []
            
            for page_idx in pages_to_extract[:3]:  # Max 3 pages
                if page_idx < total_pages:
                    page = pdf.pages[page_idx]
                    
                    # Extract tables
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            # Convert to DataFrame
                            if len(table) > 1:
                                try:
                                    df = pd.DataFrame(table[1:], columns=table[0])
                                    tables_data.append({
                                        "page": page_idx,
                                        "shape": list(df.shape),  # Convert tuple to list for JSON
                                        "columns": [str(col) for col in df.columns],  # Ensure strings
                                        "data": df.to_dict(orient='records')[:10]  # First 10 rows
                                    })
                                except Exception as e:
                                    logger.warning(f"Failed to parse table on page {page_idx}: {e}")
                                    continue
                    
                    # Extract text
                    text = page.extract_text()
                    if text:
                        text_data.append({
                            "page": page_idx,
                            "text": text[:500]  # First 500 chars
                        })
            
            metadata = {
                "type": "pdf",
                "total_pages": total_pages,
                "extracted_pages": pages_to_extract,
                "tables": tables_data,
                "text_samples": text_data
            }
            print("pdf_metadata",metadata)
            
            return metadata
            
    except Exception as e:
        logger.error(f"PDF extraction error: {e}")
        return {"type": "pdf", "error": str(e)}


async def _extract_excel_metadata(filepath: str) -> Dict[str, Any]:
    """Extract metadata from Excel file."""
    try:
        # Read all sheets
        excel_file = pd.ExcelFile(filepath)
        sheets_metadata = {}
        
        for sheet_name in excel_file.sheet_names[:3]:  # Max 3 sheets
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            rows, cols = df.shape
            
            if rows <= 10 and cols <= 10:
                data_preview = df.to_dict(orient='records')
            else:
                data_preview = df.head(5).to_dict(orient='records')
            
            sheets_metadata[sheet_name] = {
                "shape": {"rows": rows, "columns": cols},
                "columns": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "data_preview": data_preview,
                "full_data": df.to_dict(orient='records') if rows <= 10 and cols <= 10 else None
            }
        
        metadata = {
            "type": "excel",
            "sheets": list(excel_file.sheet_names),
            "sheets_data": sheets_metadata
        }
        print("excel_metadata",metadata)
        
        return metadata
        
    except Exception as e:
        logger.error(f"Excel extraction error: {e}")
        return {"type": "excel", "error": str(e)}


async def _extract_json_metadata(filepath: str) -> Dict[str, Any]:
    """Extract metadata from JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Analyze structure
        if isinstance(data, list):
            structure = "array"
            length = len(data)
            sample = data[:5] if length > 5 else data
        elif isinstance(data, dict):
            structure = "object"
            length = len(data)
            sample = {k: data[k] for k in list(data.keys())[:10]}
        else:
            structure = "primitive"
            length = 1
            sample = data
        
        metadata = {
            "type": "json",
            "structure": structure,
            "length": length,
            "sample": sample,
            "full_data": data if length <= 20 else None
        }
        print("json_metadata",metadata)
        
        return metadata
        
    except Exception as e:
        logger.error(f"JSON extraction error: {e}")
        return {"type": "json", "error": str(e)}


async def _extract_text_metadata(filepath: str) -> Dict[str, Any]:
    """Extract metadata from text file."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        lines = content.split('\n')
        
        metadata = {
            "type": "text",
            "size_bytes": len(content),
            "line_count": len(lines),
            "preview": '\n'.join(lines[:20]),  # First 20 lines
            "full_content": content if len(content) < 5000 else None
        }
        print("text_metadata",metadata)
        
        return metadata
        
    except Exception as e:
        logger.error(f"Text extraction error: {e}")
        return {"type": "text", "error": str(e)}