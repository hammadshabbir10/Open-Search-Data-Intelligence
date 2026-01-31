"""
OpenSearch Email Data Ingestion Script

This script ingests email data from final_emails.json into a local OpenSearch instance.
It creates an index with proper mappings and bulk-indexes the email documents.
"""

import json
from datetime import datetime
from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import RequestError
import sys


# OpenSearch connection configuration
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
INDEX_NAME = 'email-data'

# Initialize OpenSearch client (no authentication since security is disabled)
client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)


def create_index_with_mapping():
    """
    Create the OpenSearch index with proper field mappings for email data.
    """
    index_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "index": {
                "max_result_window": 50000
            }
        },
        "mappings": {
            "properties": {
                "timestamp": {
                    "type": "date",
                    "format": "EEE, dd MMM yyyy HH:mm:ss Z||epoch_millis||strict_date_optional_time"
                },
                "email": {
                    "properties": {
                        "from": {"type": "keyword"},
                        "to": {"type": "keyword"},
                        "cc": {"type": "keyword"},
                        "bcc": {"type": "keyword"}
                    }
                },
                "message": {
                    "properties": {
                        "message_id": {"type": "keyword"},
                        "subject": {"type": "text"},
                        "content_type": {"type": "keyword"},
                        "body_text": {"type": "text"},
                        "body_html": {"type": "text"}
                    }
                },
                "network": {
                    "properties": {
                        "protocol": {"type": "keyword"},
                        "source": {
                            "properties": {
                                "ip": {"type": "ip"},
                                "port": {"type": "keyword"},
                                "is_private": {"type": "boolean"}
                            }
                        },
                        "destination": {
                            "properties": {
                                "ip": {"type": "ip"},
                                "port": {"type": "keyword"},
                                "is_private": {"type": "boolean"}
                            }
                        }
                    }
                },
                "attachments": {"type": "object"},
                "correlation": {
                    "properties": {
                        "cgnat": {
                            "properties": {
                                "matched": {"type": "boolean"}
                            }
                        },
                        "radius": {
                            "properties": {
                                "session_found": {"type": "boolean"}
                            }
                        }
                    }
                }
            }
        }
    }
    
    try:
        # Delete index if it exists
        if client.indices.exists(index=INDEX_NAME):
            print(f"Index '{INDEX_NAME}' already exists. Deleting...")
            client.indices.delete(index=INDEX_NAME)
            print(f"Index '{INDEX_NAME}' deleted.")
        
        # Create new index
        print(f"Creating index '{INDEX_NAME}'...")
        client.indices.create(index=INDEX_NAME, body=index_mapping)
        print(f"Index '{INDEX_NAME}' created successfully with mappings.")
        return True
        
    except RequestError as e:
        print(f"Error creating index: {e}")
        return False


def load_email_data(json_file_path):
    """
    Load email data from JSON file.
    
    Args:
        json_file_path: Path to the JSON file containing email data
        
    Returns:
        List of email documents
    """
    print(f"Loading data from {json_file_path}...")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Loaded {len(data)} email records.")
        return data
    except FileNotFoundError:
        print(f"Error: File not found - {json_file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
        sys.exit(1)


def prepare_bulk_data(email_data):
    """
    Prepare email data for bulk indexing.
    
    Args:
        email_data: List of email documents
        
    Yields:
        Documents formatted for bulk indexing
    """
    for idx, email in enumerate(email_data):
        yield {
            "_index": INDEX_NAME,
            "_id": idx + 1,  # Use sequential IDs
            "_source": email
        }


def bulk_index_data(email_data):
    """
    Bulk index email data into OpenSearch.
    
    Args:
        email_data: List of email documents
        
    Returns:
        Tuple of (success_count, failed_count)
    """
    print(f"Starting bulk indexing of {len(email_data)} documents...")
    
    try:
        # Use the helpers.bulk function for efficient bulk indexing
        success, failed = helpers.bulk(
            client,
            prepare_bulk_data(email_data),
            chunk_size=500,
            request_timeout=60,
            raise_on_error=False,
            stats_only=False
        )
        
        print(f"Bulk indexing completed.")
        print(f"Successfully indexed: {success} documents")
        
        if failed:
            print(f"Failed to index: {len(failed)} documents")
            for item in failed[:5]:  # Show first 5 failures
                print(f"  - {item}")
        
        return success, len(failed) if failed else 0
        
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
        return 0, len(email_data)


def verify_indexing():
    """
    Verify that documents were indexed successfully.
    """
    print("\nVerifying indexed data...")
    
    # Refresh the index to make documents searchable
    client.indices.refresh(index=INDEX_NAME)
    
    # Get index stats
    stats = client.indices.stats(index=INDEX_NAME)
    doc_count = stats['_all']['primaries']['docs']['count']
    
    print(f"Total documents in index '{INDEX_NAME}': {doc_count}")
    
    # Sample query to show some data
    try:
        response = client.search(
            index=INDEX_NAME,
            body={
                "query": {"match_all": {}},
                "size": 3
            }
        )
        
        print(f"\nSample documents (showing {len(response['hits']['hits'])} of {doc_count}):")
        for hit in response['hits']['hits']:
            source = hit['_source']
            print(f"\n  Document ID: {hit['_id']}")
            print(f"  Timestamp: {source.get('timestamp', 'N/A')}")
            print(f"  From: {source.get('email', {}).get('from', [])}")
            print(f"  To: {source.get('email', {}).get('to', [])}")
            print(f"  Protocol: {source.get('network', {}).get('protocol', 'N/A')}")
            
    except Exception as e:
        print(f"Error querying index: {e}")


def main():
    """
    Main execution function.
    """
    print("=" * 60)
    print("OpenSearch Email Data Ingestion Script")
    print("=" * 60)
    
    # Check OpenSearch connection
    print("\nChecking OpenSearch connection...")
    try:
        info = client.info()
        print(f"Connected to OpenSearch cluster: {info['cluster_name']}")
        print(f"Version: {info['version']['number']}")
    except Exception as e:
        print(f"Error: Could not connect to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
        print(f"Details: {e}")
        print("\nMake sure your OpenSearch Docker container is running:")
        print("  docker-compose up -d")
        sys.exit(1)
    
    # Create index with mappings
    if not create_index_with_mapping():
        print("Failed to create index. Exiting.")
        sys.exit(1)
    
    # Load email data
    json_file_path = r"c:\Users\Bilal\Documents\Opensearch\Open-Search-Data-Intelligence\email_extraction\final_emails.json"
    email_data = load_email_data(json_file_path)
    
    # Bulk index the data
    success_count, failed_count = bulk_index_data(email_data)
    
    # Verify the indexing
    verify_indexing()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total records processed: {len(email_data)}")
    print(f"Successfully indexed: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Index name: {INDEX_NAME}")
    print(f"\nYou can now query your data at: http://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/{INDEX_NAME}/_search")
    print(f"OpenSearch Dashboards: http://{OPENSEARCH_HOST}:5601")
    print("=" * 60)


if __name__ == "__main__":
    main()
