import subprocess
import os
import time

print("📧 Extracting emails with your TShark location...")

# Your TShark location
TSHARK_PATH = r"D:\Wireshark\tshark.exe"
PCAP_PATH = r"..\smtp-July-28.pcap"
OUTPUT_DIR = "eml_files"

# Check if files exist
print(f"Checking TShark at: {TSHARK_PATH}")
print(f"Checking PCAP at: {PCAP_PATH}")

if not os.path.exists(TSHARK_PATH):
    print(f"❌ TShark not found at: {TSHARK_PATH}")
    exit(1)

if not os.path.exists(PCAP_PATH):
    print(f"❌ PCAP not found at: {PCAP_PATH}")
    exit(1)

print("✅ All files found!")

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# CORRECTED: Use "imf" instead of "smtp"
cmd = [
    TSHARK_PATH,
    "-r", PCAP_PATH,
    "--export-objects", f"imf,{OUTPUT_DIR}"  # ← CHANGED FROM "smtp" TO "imf"
]

print(f"\nRunning command:")
print(f"  {' '.join(cmd)}")
print("\nThis may take a few minutes...")

try:
    # Run with timeout
    result = subprocess.run(
        cmd, 
        capture_output=True, 
        text=True,
        timeout=300  # 5 minutes timeout
    )
    
    print(f"\nReturn code: {result.returncode}")
    
    if result.stdout:
        print(f"Output: {result.stdout[:500]}...")
    
    if result.stderr:
        print(f"Errors: {result.stderr[:500]}...")
    
    if result.returncode == 0:
        print("✅ TShark extraction completed!")
    else:
        print("⚠️  TShark had issues but may have extracted some data")
        
except subprocess.TimeoutExpired:
    print("❌ TShark took too long (5+ minutes)")
except Exception as e:
    print(f"❌ Error: {e}")

# Check results
print(f"\n🔍 Checking results in: {OUTPUT_DIR}")
if os.path.exists(OUTPUT_DIR):
    files = os.listdir(OUTPUT_DIR)
    
    # Look for various email file extensions
    email_files = [f for f in files if f.endswith(('.eml', '.txt', '.dat', ''))]
    
    if email_files:
        print(f"✅ SUCCESS! Found {len(email_files)} email files:")
        for i, file in enumerate(email_files[:10]):
            file_path = os.path.join(OUTPUT_DIR, file)
            size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            print(f"  {i+1}. {file} ({size:,} bytes)")
            
            # Show first few lines if it's a text file
            if file.endswith(('.txt', '.eml')) and size > 0 and size < 100000:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read(500)
                        print(f"     Preview: {content[:100]}...")
                except:
                    pass
        
        if len(email_files) > 10:
            print(f"  ... and {len(email_files) - 10} more")
    else:
        print("❌ No email files found in output directory")
        print(f"All files found: {files}")
else:
    print("❌ Output directory not created")

print("\n🎉 Done!")