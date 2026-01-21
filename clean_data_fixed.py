import pandas as pd
import numpy as np
from datetime import datetime
import json
import csv
import os

# Read CSV with error handling
print("Reading CSV file...")

try:
    # First, let's read the CSV line by line to understand the structure
    with open('smtp_metadata.csv', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"Total lines in file: {len(lines)}")
    
    # Parse CSV manually to handle inconsistent columns
    data = []
    header = None
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        if i == 0:  # Header line
            header = line.split(',')
            print(f"Header has {len(header)} columns: {header}")
            continue
            
        parts = line.split(',')
        
        # Handle lines with different number of columns
        if len(parts) > len(header):
            # Too many columns - merge the extra ones into the last column
            parts = parts[:len(header)-1] + [','.join(parts[len(header)-1:])]
        elif len(parts) < len(header):
            # Too few columns - pad with empty strings
            parts = parts + [''] * (len(header) - len(parts))
        
        data.append(parts)
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=header)
    
except Exception as e:
    print(f"Error reading CSV: {e}")
    print("\nTrying alternative method...")
    
    # Alternative method using csv module
    data = []
    with open('smtp_metadata.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            data.append(row)
    
    # Get header and data
    header = data[0]
    data = data[1:]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=header)

print(f"Successfully read {len(df)} records")
print(f"Columns: {list(df.columns)}")

# Clean column names (replace dots with underscores)
df.columns = [col.replace('.', '_') for col in df.columns]

# Print sample of data
print("\nFirst few rows:")
print(df.head())

# Function to clean individual values
def clean_value(val):
    if pd.isna(val) or val in ['', 'N/A', 'NA', 'NaN', 'nan', 'None', 'null']:
        return None
    try:
        # Remove extra quotes and whitespace
        val = str(val).strip()
        val = val.strip('"').strip("'").strip()
        return val if val else None
    except:
        return None

# Clean all columns
for col in df.columns:
    df[col] = df[col].apply(clean_value)

# Convert timestamp
def parse_timestamp(ts):
    try:
        if pd.isna(ts):
            return None
        ts = float(ts)
        return datetime.fromtimestamp(ts)
    except:
        return None

df['timestamp'] = df['frame_time_epoch'].apply(parse_timestamp)

# Extract SMTP commands
def extract_smtp_command(cmd):
    if pd.isna(cmd):
        return None
    cmd = str(cmd).strip().upper()
    
    # Common SMTP commands
    commands = {
        'EHLO': 'EHLO',
        'HELO': 'HELO',
        'MAIL': 'MAIL',
        'RCPT': 'RCPT',
        'DATA': 'DATA',
        'QUIT': 'QUIT',
        'RSET': 'RSET',
        'AUTH': 'AUTH',
        'STARTTLS': 'STARTTLS',
        'VRFY': 'VRFY',
        'EXPN': 'EXPN',
        'HELP': 'HELP',
        'NOOP': 'NOOP'
    }
    
    for key, value in commands.items():
        if key in cmd:
            return value
    
    return 'OTHER'

df['smtp_command'] = df['smtp_req_command'].apply(extract_smtp_command)

# Detect encryption
def detect_encryption(row):
    # Check for STARTTLS command
    if row.get('smtp_req_command') and 'STARTTLS' in str(row['smtp_req_command']).upper():
        return True
    
    # Check for TLS in response
    if row.get('smtp_response') and 'TLS' in str(row['smtp_response']).upper():
        return True
    
    # Check port (465 for SMTPS, 587 for STARTTLS)
    port = row.get('tcp_dstport')
    if port in ['465', '587', '993', '995']:
        return True
    
    return False

df['is_encrypted'] = df.apply(detect_encryption, axis=1)

# Extract email addresses if available
def extract_email(text):
    if pd.isna(text):
        return None
    
    import re
    text = str(text)
    
    # Look for email patterns
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    
    if emails:
        return emails[0]
    
    # Check for MAIL FROM: or RCPT TO: patterns
    if 'FROM:' in text.upper():
        parts = text.split('FROM:')
        if len(parts) > 1:
            email = parts[1].strip()
            email = email.replace('<', '').replace('>', '').strip()
            if '@' in email:
                return email
    
    if 'TO:' in text.upper():
        parts = text.split('TO:')
        if len(parts) > 1:
            email = parts[1].strip()
            email = email.replace('<', '').replace('>', '').strip()
            if '@' in email:
                return email
    
    return None

df['from_email'] = df['cflow_pie_ntop_smtp_mail_from'].apply(extract_email)
df['to_email'] = df['cflow_pie_ntop_smtp_rcpt_to'].apply(extract_email)

# Also check smtp_parameter for email addresses
def extract_from_parameter(param):
    if pd.isna(param):
        return None
    
    param = str(param)
    email = extract_email(param)
    if email:
        return email
    
    # Check for common patterns
    if param.startswith('<') and param.endswith('>'):
        email = param[1:-1].strip()
        if '@' in email:
            return email
    
    return None

df['param_email'] = df['smtp_req_parameter'].apply(extract_from_parameter)

# Create final email fields
def get_final_email(row, field_type='from'):
    # Try different fields in order
    if field_type == 'from':
        fields = ['from_email', 'param_email', 'cflow_pie_ntop_smtp_mail_from']
    else:  # 'to'
        fields = ['to_email', 'param_email', 'cflow_pie_ntop_smtp_rcpt_to']
    
    for field in fields:
        email = row.get(field)
        if email and pd.notna(email) and str(email).strip() and '@' in str(email):
            return str(email).strip()
    
    return None

df['final_from'] = df.apply(lambda x: get_final_email(x, 'from'), axis=1)
df['final_to'] = df.apply(lambda x: get_final_email(x, 'to'), axis=1)

# Prepare for OpenSearch
print("\nPreparing data for OpenSearch...")

# Create bulk import data
bulk_data = []
record_count = 0

for idx, row in df.iterrows():
    try:
        # Basic document structure
        doc = {
            # Network info
            "frame_time_epoch": float(row.get('frame_time_epoch')) if pd.notna(row.get('frame_time_epoch')) else None,
            "source_ip": row.get('ip_src'),
            "destination_ip": row.get('ip_dst'),
            "source_port": int(row['tcp_srcport']) if pd.notna(row.get('tcp_srcport')) and str(row['tcp_srcport']).isdigit() else None,
            "destination_port": int(row['tcp_dstport']) if pd.notna(row.get('tcp_dstport')) and str(row['tcp_dstport']).isdigit() else None,
            
            # SMTP info
            "smtp_command": row.get('smtp_command'),
            "smtp_parameter": row.get('smtp_req_parameter'),
            "response_code": int(row['smtp_response_code']) if pd.notna(row.get('smtp_response_code')) and str(row['smtp_response_code']).isdigit() else None,
            "response_message": row.get('smtp_response'),
            
            # Email info
            "from_email": row.get('final_from'),
            "to_email": row.get('final_to'),
            "email_message": row.get('smtp_message'),
            
            # Analysis fields
            "is_encrypted": bool(row.get('is_encrypted')),
            "encryption_status": "ENCRYPTED" if row.get('is_encrypted') else "UNENCRYPTED",
            "protocol": "SMTP",
            "record_type": "REQUEST" if pd.notna(row.get('smtp_req_command')) else "RESPONSE" if pd.notna(row.get('smtp_response_code')) else "UNKNOWN",
            
            # Timestamp
            "timestamp": row['timestamp'].isoformat() if pd.notna(row.get('timestamp')) else None,
        }
        
        # Remove None values
        doc = {k: v for k, v in doc.items() if v is not None}
        
        # Add action for bulk import
        action = {
            "index": {
                "_index": "email-traffic",
                "_id": f"record_{idx}"
            }
        }
        
        bulk_data.append(json.dumps(action))
        bulk_data.append(json.dumps(doc))
        record_count += 1
        
        if record_count % 1000 == 0:
            print(f"Processed {record_count} records...")
            
    except Exception as e:
        print(f"Error processing row {idx}: {e}")
        continue

# Save to file
output_file = "bulk_import.json"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('\n'.join(bulk_data))

print(f"\n✅ Successfully processed {record_count} records")
print(f"✅ Saved to: {output_file}")

# Create summary statistics
print("\n📊 Data Summary:")
print(f"Total records: {len(df)}")
print(f"Records with SMTP commands: {df['smtp_command'].notna().sum()}")
print(f"Records with response codes: {df['smtp_response_code'].notna().sum()}")
print(f"Encrypted connections: {df['is_encrypted'].sum()}")
print(f"Unique SMTP commands: {df['smtp_command'].nunique()}")
print(f"Unique source IPs: {df['ip_src'].nunique()}")
print(f"Unique destination IPs: {df['ip_dst'].nunique()}")

# Show SMTP command distribution
print("\n📈 SMTP Command Distribution:")
if 'smtp_command' in df.columns:
    cmd_dist = df['smtp_command'].value_counts()
    for cmd, count in cmd_dist.head(10).items():
        print(f"  {cmd}: {count}")

print("\n🚀 Next steps:")
print("1. Run: python create_index.py")
print("2. Run: python import_data.py")
print("3. Open http://localhost:5601")