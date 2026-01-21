import json
from pathlib import Path

print("Checking all file IDs in parsed_emails...")
print("-" * 80)

all_file_ids = []
file_ids_with_attachments = []

for file in sorted(Path('parsed_emails').glob('*.json')):
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        file_id = data.get('file_id')
        attachments = data.get('attachments', [])
        
        if file_id:
            all_file_ids.append(file_id)
            
            if len(attachments) > 0:
                file_ids_with_attachments.append(file_id)
                print(f"✓ File ID {file_id}: {len(attachments)} attachments")
    except:
        pass

print("\n" + "="*80)
print(f"Total unique file IDs: {len(set(all_file_ids))}")
print(f"File IDs with attachments: {len(file_ids_with_attachments)}")
print(f"File IDs with attachments: {sorted(file_ids_with_attachments)}")

# Check your mentioned IDs
mentioned_ids = [1036, 1037, 1038, 1049, 1062, 1070]
print("\n" + "="*80)
print("Your mentioned file IDs vs actual data:")
for fid in mentioned_ids:
    status = "✓ EXISTS" if fid in file_ids_with_attachments else "✗ NOT FOUND"
    print(f"  File ID {fid}: {status}")
