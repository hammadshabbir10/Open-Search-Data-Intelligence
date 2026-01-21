import json
from pathlib import Path

# File IDs with attachments
target_ids = [1036, 1037, 1038, 1049, 1062, 1070]

print("Filtering emails with specific file IDs:")
print("-" * 80)

results = []

for file in sorted(Path('parsed_emails').glob('*.json')):
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        file_id = data.get('file_id')
        
        if file_id in target_ids:
            from_addr = data.get('metadata', {}).get('from', 'N/A')
            subject = data.get('metadata', {}).get('subject', 'N/A')
            attachments = data.get('attachments', [])
            parsed_at = data.get('parsed_at', 'N/A')
            
            results.append({
                'file_id': file_id,
                'filename': file.name,
                'from': from_addr,
                'subject': subject,
                'attachments': len(attachments),
                'attachment_names': [a.get('filename', 'unknown') for a in attachments]
            })
            
            print(f"File ID: {file_id} | {file.name}")
            print(f"  From: {from_addr}")
            print(f"  Subject: {subject}")
            print(f"  Attachments: {len(attachments)}")
            if attachments:
                for att in attachments:
                    print(f"    - {att.get('filename', 'unknown')}")
            print()
    except Exception as e:
        pass

print(f"\nTotal emails found with target file IDs: {len(results)}")

# Export to CSV
if results:
    import csv
    with open('emails_with_attachments.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['File ID', 'Filename', 'From', 'Subject', 'Attachment Count', 'Attachment Names'])
        for r in results:
            writer.writerow([
                r['file_id'],
                r['filename'],
                r['from'],
                r['subject'],
                r['attachments'],
                ', '.join(r['attachment_names'])
            ])
    print(f"\nResults exported to: emails_with_attachments.csv")
