import os
import httpx
import asyncio
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from utils.logger import setup_logger

logger = setup_logger(__name__)

# Create downloads directory
DOWNLOADS_DIR = Path("downloads")
DOWNLOADS_DIR.mkdir(exist_ok=True)

async def download_resource(url: str, timeout: int = 60) -> Optional[str]:
    """
    Download a resource (PDF, CSV, image, etc.) from URL.
    
    Returns:
        Local file path if successful, None otherwise
    """
    try:
        # Parse URL to get filename
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        
        if not filename:
            # Generate filename from URL
            filename = f"resource_{abs(hash(url))}"
        
        # Add extension if missing
        if not os.path.splitext(filename)[1]:
            # Try to detect from URL or default to .dat
            if 'pdf' in url.lower():
                filename += '.pdf'
            elif 'csv' in url.lower():
                filename += '.csv'
            elif any(ext in url.lower() for ext in ['.png', '.jpg', '.jpeg', '.gif']):
                pass  # Already has extension
            else:
                filename += '.dat'
        
        filepath = DOWNLOADS_DIR / filename
        
        # Download
        logger.info(f"Downloading {url} to {filepath}")
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            
            # Write to file
            filepath.write_bytes(response.content)
            logger.info(f"Downloaded {len(response.content)} bytes to {filepath}")
            
            return str(filepath)
            
    except httpx.TimeoutException:
        logger.error(f"Timeout downloading {url}")
        return None
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return None


async def download_multiple(urls: list, max_concurrent: int = 3) -> dict:
    """
    Download multiple resources concurrently.
    
    Returns:
        Dict mapping URL to local filepath
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def download_with_limit(url):
        async with semaphore:
            return url, await download_resource(url)
    
    tasks = [download_with_limit(url) for url in urls]
    results = await asyncio.gather(*tasks)
    
    return {url: path for url, path in results if path is not None}