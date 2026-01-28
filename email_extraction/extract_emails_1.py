import subprocess
import os
import sys
import json
from email import policy
from email.parser import BytesParser

TSHARK = r"C:\Program Files\Wireshark\tshark.exe"
PCAP = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\smtp-July-28.pcap"
OUT_DIR = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\emails"
OUT_JSON = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\emails.json"

os.makedirs(OUT_DIR, exist_ok=True)

# 1. Export IMF objects
cmd = [
    TSHARK,
    "-r", PCAP,
    "--export-objects", f"imf,{OUT_DIR}"
]

subprocess.run(cmd, check=True)

emails = []

for fname in os.listdir(OUT_DIR):
    if not fname.lower().endswith(".eml"):
        continue

    path = os.path.join(OUT_DIR, fname)
    try:
        msg = BytesParser(policy=policy.default).parse(open(path, "rb"))
    except Exception:
        continue

    body_text = ""
    body_html = ""

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue

            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"

            if ctype == "text/plain":
                body_text += payload.decode(charset, errors="replace")
            elif ctype == "text/html":
                body_html += payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True) or b""
        charset = msg.get_content_charset() or "utf-8"
        body_text = payload.decode(charset, errors="replace")

    emails.append({
        "message_id": msg.get("Message-ID"),
        "date": msg.get("Date"),
        "from": msg.get_all("From", []),
        "to": msg.get_all("To", []),
        "cc": msg.get_all("Cc", []),
        "bcc": msg.get_all("Bcc", []),
        "subject": msg.get("Subject"),
        "body_text": body_text.strip() or None,
        "body_html": body_html.strip() or None,
    })

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(emails, f, indent=2)

print(f"Extracted {len(emails)} emails → {OUT_JSON}")
