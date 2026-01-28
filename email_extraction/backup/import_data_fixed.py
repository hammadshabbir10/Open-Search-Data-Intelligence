import requests
import json
import os
import time
from datetime import datetime

print("Importing data to OpenSearch...")

OPENSEARCH_HOST = "http://localhost:9200"
BULK_FILE = "bulk_import.json"
INDEX_NAME = "email-traffic"

# Check if file exists
if not os.path.exists(BULK_FILE):
    print(f"❌ File not found: {BULK_FILE}")
    print("Run clean_data_fixed.py first!")
    exit(1)

# Get file size
file_size = os.path.getsize(BULK_FILE) / (1024 * 1024)  # MB
print(f"File size: {file_size:.2f} MB")

# First, let's fix the bulk file by ensuring it ends with a newline
print("Fixing bulk file format...")
with open(BULK_FILE, 'rb') as f:
    content = f.read()
    
# Ensure the file ends with a newline
if not content.endswith(b'\n'):
    content += b'\n'
    
# Write back fixed content
with open('bulk_import_fixed.json', 'wb') as f:
    f.write(content)

print("✓ Fixed bulk file format")

# Read in chunks to avoid memory issues
def import_in_chunks(file_path, chunk_size=10000):
    """Import data in chunks to avoid memory issues"""
    print(f"Importing data in chunks of {chunk_size} documents...")
    
    total_imported = 0
    chunk_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        chunk = []
        
        for line_num, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
                
            chunk.append(line)
            
            if len(chunk) >= chunk_size * 2:  # *2 because each document has 2 lines
                chunk_count += 1
                imported = import_chunk(chunk, chunk_count)
                total_imported += imported
                chunk = []
        
        # Import remaining documents
        if chunk:
            chunk_count += 1
            imported = import_chunk(chunk, chunk_count)
            total_imported += imported
    
    return total_imported

def import_chunk(chunk_lines, chunk_number):
    """Import a chunk of data"""
    try:
        print(f"  Importing chunk {chunk_number} ({len(chunk_lines)//2} documents)...")
        
        # Create bulk data for this chunk
        bulk_data = '\n'.join(chunk_lines) + '\n'
        
        response = requests.post(
            f"{OPENSEARCH_HOST}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk_data,
            timeout=300
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errors'):
                errors = sum(1 for item in result.get('items', []) if 'error' in item.get('index', {}))
                if errors:
                    print(f"    ⚠️  Chunk {chunk_number} had {errors} errors")
            return len(result.get('items', []))
        else:
            print(f"    ❌ Error in chunk {chunk_number}: {response.status_code}")
            return 0
            
    except Exception as e:
        print(f"    ❌ Exception in chunk {chunk_number}: {e}")
        return 0

try:
    # Try importing in chunks
    total_imported = import_in_chunks('bulk_import_fixed.json', chunk_size=5000)
    
    if total_imported > 0:
        print(f"\n✅ Successfully imported {total_imported} documents!")
    else:
        print("\n⚠️  No documents were imported")
        
except Exception as e:
    print(f"\n❌ Error during import: {e}")
    
    # Try alternative method
    print("\nTrying alternative import method...")
    try:
        # Read the entire file and send
        with open('bulk_import_fixed.json', 'r', encoding='utf-8') as f:
            bulk_data = f.read()
        
        # Ensure it ends with newline
        if not bulk_data.endswith('\n'):
            bulk_data += '\n'
        
        response = requests.post(
            f"{OPENSEARCH_HOST}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk_data,
            timeout=600  # 10 minutes timeout
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Imported {len(result.get('items', []))} documents")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text[:200]}")
            
    except Exception as e2:
        print(f"❌ Alternative method also failed: {e2}")

# Verify import
print("\n" + "="*50)
print("🔍 Verifying import...")
try:
    # Wait a moment for indexing to complete
    time.sleep(2)
    
    # Get index count
    response = requests.get(f"{OPENSEARCH_HOST}/{INDEX_NAME}/_count", timeout=10)
    if response.status_code == 200:
        count = response.json().get('count', 0)
        print(f"✅ Documents in index: {count}")
        
        # Get SMTP command distribution
        print("\n📊 Sample statistics:")
        response = requests.post(
            f"{OPENSEARCH_HOST}/{INDEX_NAME}/_search",
            json={
                "size": 0,
                "aggs": {
                    "commands": {
                        "terms": {
                            "field": "smtp_command.keyword",
                            "size": 10
                        }
                    }
                }
            },
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            buckets = result.get('aggregations', {}).get('commands', {}).get('buckets', [])
            if buckets:
                print("SMTP Command Distribution:")
                for bucket in buckets:
                    print(f"  {bucket['key']}: {bucket['doc_count']}")
        
    else:
        print(f"❌ Could not get count: {response.status_code}")
        
except Exception as e:
    print(f"❌ Verification error: {e}")

print("\n" + "="*50)
print("🎉 IMPORT COMPLETE!")
print("="*50)
print("\nAccess OpenSearch Dashboards at: http://localhost:5601")
print("\nTo create visualizations:")
print("1. Go to http://localhost:5601")
print("2. Click 'Stack Management' -> 'Index Patterns'")
print("3. Create index pattern: email-traffic")
print("4. Time field: timestamp")
print("5. Click 'Discover' to view data")
print("6. Click 'Visualize' to create charts")