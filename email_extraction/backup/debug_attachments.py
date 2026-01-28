import json
from pathlib import Path

print("Debugging attachment fields...")
print("-" * 80)

count = 0
has_att_true = 0
has_att_false = 0

for file in sorted(Path('parsed_emails').glob('*.json'))[:10]:  # Check first 10 files
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        count += 1
        has_att = data.get('has_attachments')
        attachments = data.get('attachments', [])
        
        print(f"File: {file.name}")
        print(f"  has_attachments value: {has_att} (type: {type(has_att).__name__})")
        print(f"  attachments list: {len(attachments)} items")
        
        if has_att == True:
            has_att_true += 1
        elif has_att == False:
            has_att_false += 1
        
        print()
    except Exception as e:
        print(f"Error in {file.name}: {e}")
        print()

print(f"Summary of first 10 files:")
print(f"  has_attachments == True: {has_att_true}")
print(f"  has_attachments == False: {has_att_false}")
print(f"  Total checked: {count}")

# Now check total with attachments
print("\n" + "="*80)
print("Checking ALL files for attachments...")
total_with_attachments = 0

for file in Path('parsed_emails').glob('*.json'):
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        attachments = data.get('attachments', [])
        if len(attachments) > 0:
            total_with_attachments += 1
            print(f"{file.name}: {len(attachments)} attachments")
    except:
        pass

print(f"\nTotal emails with attachments (by checking attachments list): {total_with_attachments}")
