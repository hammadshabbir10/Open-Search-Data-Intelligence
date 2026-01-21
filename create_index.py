import requests
import json

print("Creating OpenSearch index...")

# OpenSearch connection
OPENSEARCH_HOST = "http://localhost:9200"
INDEX_NAME = "email-traffic"

# Simple index mapping
index_mapping = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s"
        }
    },
    "mappings": {
        "properties": {
            # Network fields
            "frame_time_epoch": {"type": "float"},
            "source_ip": {"type": "ip"},
            "destination_ip": {"type": "ip"},
            "source_port": {"type": "integer"},
            "destination_port": {"type": "integer"},
            
            # SMTP fields
            "smtp_command": {"type": "keyword"},
            "smtp_parameter": {"type": "text"},
            "response_code": {"type": "integer"},
            "response_message": {"type": "text"},
            
            # Email fields
            "from_email": {"type": "keyword"},
            "to_email": {"type": "keyword"},
            "email_message": {"type": "text"},
            
            # Analysis fields
            "is_encrypted": {"type": "boolean"},
            "encryption_status": {"type": "keyword"},
            "protocol": {"type": "keyword"},
            "record_type": {"type": "keyword"},
            
            # Time field
            "timestamp": {"type": "date"}
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
    print("\nMake sure OpenSearch is running:")
    print("  - Check: http://localhost:9200")
    print("  - Start with: docker run -p 9200:9200 opensearchproject/opensearch:latest")