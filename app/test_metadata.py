"""
Test script to verify metadata structure matches what agent expects
"""
import asyncio
import json
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.preprocessor import preprocess_downloaded_files

async def test_pdf_metadata():
    """Test PDF preprocessing"""
    print("=" * 60)
    print("Testing PDF Metadata Structure")
    print("=" * 60)
    
    # Mock downloaded files
    downloaded_files = {
        "http://127.0.0.1:5000/pdf": "downloads/pdf.pdf"
    }
    
    try:
        metadata = await preprocess_downloaded_files(downloaded_files, None)
        
        print("\n✅ Metadata extracted successfully!\n")
        print(json.dumps(metadata, indent=2, default=str))
        
        # Verify structure
        for url, meta in metadata.items():
            print(f"\n🔍 Checking structure for: {url}")
            
            assert "type" in meta, "Missing 'type' key"
            assert "filepath" in meta, "Missing 'filepath' key"
            
            if meta["type"] == "pdf":
                assert "tables" in meta, "Missing 'tables' key"
                assert isinstance(meta["tables"], list), "'tables' must be a list"
                
                if meta["tables"]:
                    table = meta["tables"][0]
                    assert "page" in table, "Table missing 'page' key"
                    assert "shape" in table, "Table missing 'shape' key"
                    assert "columns" in table, "Table missing 'columns' key"
                    assert "data" in table, "Table missing 'data' key"
                    
                    print(f"  ✓ Table structure valid")
                    print(f"  ✓ Shape: {table['shape']}")
                    print(f"  ✓ Columns: {table['columns']}")
                    print(f"  ✓ Data rows: {len(table['data'])}")
                
                print(f"\n✅ PDF metadata structure is VALID")
            
            elif meta["type"] == "csv":
                assert "columns" in meta, "CSV missing 'columns' key"
                assert "shape" in meta, "CSV missing 'shape' key"
                assert "data_preview" in meta, "CSV missing 'data_preview' key"
                print(f"\n✅ CSV metadata structure is VALID")
        
        return metadata
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

async def test_csv_metadata():
    """Test CSV preprocessing"""
    print("\n" + "=" * 60)
    print("Testing CSV Metadata Structure")
    print("=" * 60)
    
    # You would test with a real CSV file
    # For now, just show expected structure
    expected = {
        "http://example.com/data.csv": {
            "filepath": "downloads/data.csv",
            "type": "csv",
            "extension": ".csv",
            "shape": {"rows": 100, "columns": 3},
            "columns": ["id", "name", "value"],
            "dtypes": {"id": "int64", "name": "object", "value": "float64"},
            "data_preview": [
                {"id": 1, "name": "Alice", "value": 10.5},
                {"id": 2, "name": "Bob", "value": 20.3}
            ],
            "has_full_data": False
        }
    }
    
    print("\nExpected CSV structure:")
    print(json.dumps(expected, indent=2))

def test_agent_code():
    """Show how agent should access metadata"""
    print("\n" + "=" * 60)
    print("Example: How Agent Should Access Metadata")
    print("=" * 60)
    
    example_code = """
# Correct way to access PDF tables in agent code:

# Get metadata for the PDF
metadata = context["file_metadata"]["http://127.0.0.1:5000/pdf"]

# Access tables
tables = metadata["tables"]

if tables:
    # Get first table
    table_data = tables[0]["data"]
    columns = tables[0]["columns"]
    
    # Convert to DataFrame
    import pandas as pd
    df = pd.DataFrame(table_data)
    
    # Calculate average
    answer = df['Marks'].mean()
else:
    # No tables found, manual extraction needed
    import pdfplumber
    filepath = context["downloaded_files"]["http://127.0.0.1:5000/pdf"]
    with pdfplumber.open(filepath) as pdf:
        table = pdf.pages[0].extract_table()
        # ... process
"""
    
    print(example_code)

if __name__ == "__main__":
    print("\n🧪 METADATA STRUCTURE TEST\n")
    
    # Test PDF
    asyncio.run(test_pdf_metadata())
    
    # Show CSV expected structure
    asyncio.run(test_csv_metadata())
    
    # Show agent code examples
    test_agent_code()
    
    print("\n" + "=" * 60)
    print("✅ Test Complete")
    print("=" * 60)