import os
import email
import json
import hashlib
import base64
from email import policy
from email.parser import BytesParser
from email.header import decode_header
from datetime import datetime

print("📊 Parsing extracted email files...")

# Create output directory for parsed data
os.makedirs("parsed_emails", exist_ok=True)

all_emails_data = []
stats = {
    'total_files': 0,
    'successfully_parsed': 0,
    'failed': 0,
    'with_attachments': 0,
    'total_attachments': 0,
    'unique_senders': set(),
    'unique_recipients': set()
}

def parse_email_file(eml_path, file_id):
    """Parse a single .eml file"""
    try:
        with open(eml_path, 'rb') as f:
            msg = BytesParser(policy=policy.default).parse(f)
        
        # Extract basic info
        email_data = {
            'file_id': file_id,
            'filename': os.path.basename(eml_path),
            'file_size': os.path.getsize(eml_path),
            'headers': {},
            'body': {},
            'attachments': [],
            'metadata': {}
        }
        
        # Extract all headers
        for header_name, header_value in msg.items():
            email_data['headers'][header_name] = header_value
        
        # Extract metadata from key headers
        subject = msg.get('Subject', '')
        # Try to decode subject if it's encoded-word format
        try:
            if subject and '=?' in subject:
                decoded = decode_header(subject)
                subject = ''.join([str(d[0], d[1] or 'utf-8') if isinstance(d[0], bytes) else d[0] for d in decoded])
        except:
            pass
        
        email_data['metadata'] = {
            'message_id': msg.get('Message-ID', ''),
            'date': msg.get('Date', ''),
            'from': msg.get('From', ''),
            'to': msg.get('To', ''),
            'cc': msg.get('CC', ''),
            'bcc': msg.get('BCC', ''),
            'subject': subject,
            'content_type': msg.get_content_type(),
            'content_transfer_encoding': msg.get('Content-Transfer-Encoding', ''),
            'mime_version': msg.get('MIME-Version', ''),
            'received': msg.get_all('Received', [])
        }
        
        # Extract body content
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get_content_disposition())
                
                # Text body
                if content_type == "text/plain" and "attachment" not in content_disposition:
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            text = payload.decode('utf-8', errors='ignore')
                            # Try to decode base64 if it looks encoded
                            try:
                                if text and len(text) % 4 == 0 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n' for c in text.strip()):
                                    decoded = base64.b64decode(text)
                                    text = decoded.decode('utf-8', errors='ignore')
                            except:
                                pass
                            email_data['body']['text'] = text
                    except:
                        try:
                            email_data['body']['text'] = str(part.get_payload())
                        except:
                            email_data['body']['text'] = ''
                
                # HTML body
                elif content_type == "text/html" and "attachment" not in content_disposition:
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            html = payload.decode('utf-8', errors='ignore')
                            # Try to decode base64 if it looks encoded
                            try:
                                if html and len(html) % 4 == 0 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n' for c in html.strip()):
                                    decoded = base64.b64decode(html)
                                    html = decoded.decode('utf-8', errors='ignore')
                            except:
                                pass
                            email_data['body']['html'] = html
                    except:
                        try:
                            email_data['body']['html'] = str(part.get_payload())
                        except:
                            email_data['body']['html'] = ''
                
                # Attachments
                elif part.get_filename():
                    attachment = {
                        'filename': part.get_filename(),
                        'content_type': content_type,
                        'size': len(part.get_payload(decode=True)) if part.get_payload() else 0
                    }
                    
                    # Calculate hash
                    try:
                        content = part.get_payload(decode=True)
                        if content:
                            attachment['md5'] = hashlib.md5(content).hexdigest()
                            attachment['sha256'] = hashlib.sha256(content).hexdigest()
                    except:
                        pass
                    
                    email_data['attachments'].append(attachment)
        else:
            # Single part message
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    text = payload.decode('utf-8', errors='ignore')
                    # Try to decode base64 if it looks encoded
                    try:
                        if text and len(text) % 4 == 0 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n' for c in text.strip()):
                            decoded = base64.b64decode(text)
                            text = decoded.decode('utf-8', errors='ignore')
                    except:
                        pass
                    email_data['body']['text'] = text
            except:
                try:
                    email_data['body']['text'] = str(msg.get_payload())
                except:
                    email_data['body']['text'] = ''
        
        # Calculate email hash
        email_str = json.dumps(email_data, sort_keys=True).encode('utf-8')
        email_data['email_hash'] = hashlib.md5(email_str).hexdigest()
        
        # Clean up empty fields
        email_data['body'] = {k: v for k, v in email_data['body'].items() if v}
        if not email_data['body']:
            email_data['body'] = {'text': ''}
        
        # Update stats
        stats['successfully_parsed'] += 1
        if email_data['metadata']['from']:
            stats['unique_senders'].add(email_data['metadata']['from'])
        if email_data['metadata']['to']:
            stats['unique_recipients'].add(email_data['metadata']['to'])
        if email_data['attachments']:
            stats['with_attachments'] += 1
            stats['total_attachments'] += len(email_data['attachments'])
        
        return email_data
    
    except Exception as e:
        print(f"❌ Error parsing {eml_path}: {e}")
        stats['failed'] += 1
        return None

# Parse all email files
eml_dir = "eml_files"
if not os.path.exists(eml_dir):
    print(f"❌ Directory not found: {eml_dir}")
    exit(1)

eml_files = [f for f in os.listdir(eml_dir) if f.endswith('.eml')]
stats['total_files'] = len(eml_files)

print(f"Found {len(eml_files)} .eml files to parse")

for i, eml_file in enumerate(eml_files):
    eml_path = os.path.join(eml_dir, eml_file)
    
    if (i + 1) % 100 == 0:
        print(f"Parsed {i + 1}/{len(eml_files)} files...")
    
    parsed = parse_email_file(eml_path, i)
    if parsed:
        all_emails_data.append(parsed)

# Save parsed data
output_file = "parsed_emails/emails.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(all_emails_data, f, indent=2, ensure_ascii=False)

# Save summary
summary_file = "parsed_emails/summary.json"
summary = {
    'total_emails_parsed': len(all_emails_data),
    # convert non-JSON-serializable types (sets) to lists
    'parsing_stats': {
        **{k: v for k, v in stats.items() if not isinstance(v, set)},
        'unique_senders': list(stats.get('unique_senders', [])),
        'unique_recipients': list(stats.get('unique_recipients', [])),
    },
    'parsing_time': datetime.now().isoformat()
}
with open(summary_file, 'w', encoding='utf-8') as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)

print(f"\n📊 PARSING SUMMARY:")
print("=" * 50)
print(f"Total files processed: {stats['total_files']}")
print(f"Successfully parsed: {stats['successfully_parsed']}")
print(f"Failed: {stats['failed']}")
print(f"Emails with attachments: {stats['with_attachments']}")
print(f"Total attachments: {stats['total_attachments']}")
print(f"Unique senders: {len(stats['unique_senders'])}")
print(f"Unique recipients: {len(stats['unique_recipients'])}")

if all_emails_data:
    print(f"\n📧 SAMPLE EMAILS:")
    print("=" * 50)
    
    for i, email in enumerate(all_emails_data[:3]):
        print(f"\nEmail {i+1}:")
        print(f"  From: {email['metadata'].get('from', 'N/A')[:50]}")
        print(f"  To: {email['metadata'].get('to', 'N/A')[:50]}")
        print(f"  Subject: {email['metadata'].get('subject', 'N/A')[:50]}")
        print(f"  Date: {email['metadata'].get('date', 'N/A')}")
        print(f"  Body length: {len(str(email['body'])):,} chars")
        print(f"  Attachments: {len(email['attachments'])}")

print(f"\n✅ Saved parsed data to: {output_file}")
print(f"✅ Saved summary to: {summary_file}")