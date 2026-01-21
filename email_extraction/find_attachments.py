import json
import os
from pathlib import Path

print("Emails with attachments:")
print("-" * 80)

count = 0
for file in sorted(Path('parsed_emails').glob('*.json')):
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if data.get('has_attachments') == True:
            count += 1
            from_addr = data.get('metadata', {}).get('from', 'N/A')
            subject = data.get('metadata', {}).get('subject', 'N/A')
            attachments = len(data.get('attachments', []))
            print(f"{count}. {file.name}")
            print(f"   From: {from_addr}")
            print(f"   Subject: {subject}")
            print(f"   Attachments: {attachments}")
            print()
    except Exception as e:
        pass

print(f"Total emails with attachments: {count}")
