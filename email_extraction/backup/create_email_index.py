import requests
import json

print("📦 Creating email content index in OpenSearch...")

# OpenSearch connection
OPENSEARCH_HOST = "http://localhost:9200"
INDEX_NAME = "email-content"

# Index mapping for email content
index_mapping = {
    "settings": {
        "index": {
            "number_of_shards": 2,
            "number_of_replicas": 1,
            "refresh_interval": "5s"
        },
        "analysis": {
            "analyzer": {
                "email_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "stop", "snowball"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            # File info
            "file_id": {"type": "integer"},
            "filename": {"type": "keyword"},
            "file_size": {"type": "long"},
            "email_hash": {"type": "keyword"},
            
            # Metadata
            "metadata": {
                "type": "object",
                "properties": {
                    "message_id": {"type": "keyword"},
                    "date": {"type": "date"},
                    "from": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                        "analyzer": "email_analyzer"
                    },
                    "to": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                        "analyzer": "email_analyzer"
                    },
                    "cc": {"type": "text"},
                    "bcc": {"type": "text"},
                    "subject": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                        "analyzer": "email_analyzer"
                    },
                    "content_type": {"type": "keyword"},
                    "content_transfer_encoding": {"type": "keyword"},
                    "mime_version": {"type": "keyword"},
                    "received": {"type": "text"}
                }
            },
            
            # Email body
            "body": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "text",
                        "analyzer": "email_analyzer"
                    },
                    "html": {
                        "type": "text",
                        "analyzer": "email_analyzer"
                    }
                }
            },
            
            # Attachments
            "attachments": {
                "type": "nested",
                "properties": {
                    "filename": {"type": "keyword"},
                    "content_type": {"type": "keyword"},
                    "size": {"type": "long"},
                    "md5": {"type": "keyword"},
                    "sha256": {"type": "keyword"}
                }
            },
            
            # Analysis fields
            "has_attachments": {"type": "boolean"},
            "attachment_count": {"type": "integer"},
            "body_length": {"type": "integer"},
            "parsed_at": {"type": "date"}
        }
    }
}

try:
    # Delete index if exists
    response = requests.delete(f"{OPENSEARCH_HOST}/{INDEX_NAME}")
    if response.status_code in [200, 404]:
        print("✓ Cleaned existing index")
    
    # Create index
    response = requests.put(
        f"{OPENSEARCH_HOST}/{INDEX_NAME}",
        headers={"Content-Type": "application/json"},
        json=index_mapping,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        print(f"✅ Index '{INDEX_NAME}' created successfully!")
        print(f"   Response: {response.json()}")
    else:
        print(f"❌ Failed to create index: {response.status_code}")
        print(f"   Error: {response.text}")
        
except Exception as e:
    print(f"❌ Error: {e}")