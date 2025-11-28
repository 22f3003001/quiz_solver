import asyncio
import base64
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

from utils.logger import setup_logger

logger = setup_logger(__name__)

async def extract_quiz_content(url: str, timeout: int = 30000) -> Optional[Dict]:
    """
    Extract quiz content from a JavaScript-rendered page using Playwright.
    Returns dict with: page_text, links, tables, question, raw_html
    """
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # Navigate to URL
            await page.goto(url, wait_until='networkidle', timeout=timeout)
            
            # Wait for any dynamic content to load
            await asyncio.sleep(2)
            
            # Get rendered HTML
            html_content = await page.content()
            
            # Extract text content
            text_content = await page.evaluate('() => document.body.innerText')
            
            # Extract all links
            links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(href => href && !href.startsWith('javascript:'));
            }''')
            
            await browser.close()
            
            # Parse with BeautifulSoup for additional extraction
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract tables (if small enough)
            tables = extract_tables(soup)
            
            # Decode any base64 content (common in quiz pages)
            decoded_content = decode_base64_content(soup)
            if decoded_content:
                text_content = decoded_content + "\n\n" + text_content
            
            # Extract question (try to find main question text)
            question = extract_question(text_content, soup)
            
            return {
                "page_text": text_content.strip(),
                "links": list(set(links)),  # Remove duplicates
                "tables": tables,
                "question": question,
                "raw_html": html_content
            }
            
    except PlaywrightTimeout:
        logger.error(f"Timeout loading {url}")
        return None
    except Exception as e:
        logger.error(f"Error extracting content from {url}: {e}", exc_info=True)
        return None


def decode_base64_content(soup: BeautifulSoup) -> Optional[str]:
    """
    Find and decode base64 content (like atob() calls in JavaScript).
    """
    try:
        # Look for script tags with atob
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'atob' in script.string:
                # Extract base64 strings
                matches = re.findall(r'atob\s*\(\s*["\']([A-Za-z0-9+/=]+)["\']\s*\)', script.string)
                if matches:
                    decoded_parts = []
                    for b64_str in matches:
                        try:
                            decoded = base64.b64decode(b64_str).decode('utf-8')
                            decoded_parts.append(decoded)
                        except:
                            continue
                    if decoded_parts:
                        return '\n'.join(decoded_parts)
    except Exception as e:
        logger.warning(f"Error decoding base64: {e}")
    return None


def extract_tables(soup: BeautifulSoup, max_size: int = 50) -> List[str]:
    """
    Extract small tables as text representations.
    """
    tables = []
    for table in soup.find_all('table')[:5]:  # Max 5 tables
        rows = table.find_all('tr')
        if len(rows) <= max_size:
            table_text = []
            for row in rows:
                cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
                table_text.append(' | '.join(cells))
            tables.append('\n'.join(table_text))
    return tables


def extract_question(text: str, soup: BeautifulSoup) -> str:
    """
    Try to identify the main question from the page.
    """
    # Look for common question patterns
    patterns = [
        r'Q\d+\.\s*(.+?)(?:\n|$)',
        r'Question:\s*(.+?)(?:\n|$)',
        r'Task:\s*(.+?)(?:\n|$)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            # Get next few lines as well
            question_start = match.start()
            question_text = text[question_start:question_start + 500]
            return question_text.strip()
    
    # Fallback: return first few lines
    lines = text.split('\n')
    return '\n'.join(lines[:10]).strip()

