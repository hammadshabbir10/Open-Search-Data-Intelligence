import json
import requests
from datetime import datetime
import os
from email.utils import parsedate_to_datetime

print("📤 Indexing emails to OpenSearch...")

# Load parsed emails
email_file = "parsed_emails/emails.json"
if not os.path.exists(email_file):
    print(f"❌ File not found: {email_file}")
    print("Run parse_emails.py first!")
    exit(1)

with open(email_file, 'r', encoding='utf-8') as f:
    emails = json.load(f)

print(f"Loaded {len(emails)} emails for indexing")

# OpenSearch configuration
OPENSEARCH_HOST = "http://localhost:9200"
INDEX_NAME = "email-content"
BATCH_SIZE = 100  # Index in batches

# Prepare bulk data
bulk_data = []
indexed_count = 0

for i, email in enumerate(emails):
    # Add analysis fields
    email['has_attachments'] = len(email['attachments']) > 0
    email['attachment_count'] = len(email['attachments'])
    email['body_length'] = len(str(email['body']))
    email['parsed_at'] = datetime.now().isoformat()
    
    # Clean up body if it's empty dict
    if not email['body']:
        email['body'] = {'text': ''}
    
    # Create document ID
    doc_id = email.get('email_hash', f"email_{i}")

    # Normalize metadata.date to ISO-8601 or null so OpenSearch can parse it
    try:
        date_val = email.get('metadata', {}).get('date', '')
        if date_val:
            try:
                dt = parsedate_to_datetime(date_val)
                # convert to ISO format with offset if available
                email['metadata']['date'] = dt.isoformat()
            except Exception:
                email['metadata']['date'] = None
        else:
            email['metadata']['date'] = None
    except Exception:
        email.setdefault('metadata', {})['date'] = None
    
    # Add to bulk data
    bulk_data.append(json.dumps({"index": {"_index": INDEX_NAME, "_id": doc_id}}))
    bulk_data.append(json.dumps(email, ensure_ascii=False))
    
    # Send batch
    if len(bulk_data) >= BATCH_SIZE * 2 or i == len(emails) - 1:
        try:
            bulk_payload = '\n'.join(bulk_data) + '\n'
            
            response = requests.post(
                f"{OPENSEARCH_HOST}/_bulk",
                headers={"Content-Type": "application/x-ndjson"},
                data=bulk_payload.encode('utf-8'),
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errors'):
                    items = result.get('items', [])
                    # collect items that contain errors
                    error_items = [item for item in items if 'error' in item.get('index', {})]
                    error_count = len(error_items)
                    if error_count:
                        print(f"⚠️  Batch {i//BATCH_SIZE + 1} had {error_count} errors")
                        print("Sample errors (up to 5):")
                        for ei in error_items[:5]:
                            try:
                                print(json.dumps(ei, ensure_ascii=False))
                            except Exception:
                                print(str(ei))
                        # show a snippet of the raw response to aid debugging
                        print("Response snippet:", response.text[:1000])
                else:
                    indexed_count += len(bulk_data) // 2
                    print(f"✅ Batch {i//BATCH_SIZE + 1}: Indexed {len(bulk_data)//2} emails")
            else:
                print(f"❌ Batch {i//BATCH_SIZE + 1} failed: {response.status_code}")
                print(f"   Error: {response.text[:200]}")
        
        except Exception as e:
            print(f"❌ Error in batch {i//BATCH_SIZE + 1}: {e}")
        
        bulk_data = []

print(f"\n🎉 Indexing complete!")
print(f"Total emails indexed: {indexed_count}")

# Verify indexing
print(f"\n🔍 Verifying index...")
try:
    response = requests.get(f"{OPENSEARCH_HOST}/{INDEX_NAME}/_count", timeout=10)
    if response.status_code == 200:
        count = response.json().get('count', 0)
        print(f"✅ Documents in index: {count}")
        
        # Get sample data
        response = requests.get(
            f"{OPENSEARCH_HOST}/{INDEX_NAME}/_search?size=1",
            timeout=10
        )
        if response.status_code == 200:
            result = response.json()
            if result.get('hits', {}).get('hits'):
                sample = result['hits']['hits'][0]['_source']
                print(f"\n📧 Sample email indexed:")
                print(f"  From: {sample.get('metadata', {}).get('from', 'N/A')}")
                print(f"  To: {sample.get('metadata', {}).get('to', 'N/A')}")
                print(f"  Subject: {sample.get('metadata', {}).get('subject', 'N/A')}")
    else:
        print(f"❌ Could not get count: {response.status_code}")
except Exception as e:
    print(f"❌ Verification error: {e}")

print(f"\nAccess OpenSearch: http://localhost:5601")
print(f"Index: {INDEX_NAME}")