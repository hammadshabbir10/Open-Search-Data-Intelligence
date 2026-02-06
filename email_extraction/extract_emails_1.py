import subprocess
import os
import json
import hashlib
import mimetypes
from email import policy
from email.parser import BytesParser
from email.header import decode_header, make_header

TSHARK = r"C:\Program Files\Wireshark\tshark.exe"
PCAP = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\smtp-July-28.pcap"
OUT_DIR = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\emails"
OUT_JSON = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\emails.json"

os.makedirs(OUT_DIR, exist_ok=True)

def decode_mime(value: str) -> str:
    if not value:
        return value
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value

def guess_ext(content_type: str) -> str:
    # best-effort extension guess
    ext = mimetypes.guess_extension(content_type or "")
    return ext if ext else ".bin"

# 1) Export IMF objects
cmd = [TSHARK, "-r", PCAP, "--export-objects", f"imf,{OUT_DIR}"]
subprocess.run(cmd, check=True)

emails = []
files = os.listdir(OUT_DIR)

# IMPORTANT: some tshark exports do NOT end with .eml
# so we attempt to parse everything as an email message.
for fname in files:
    path = os.path.join(OUT_DIR, fname)
    if not os.path.isfile(path):
        continue

    # skip extremely tiny artifacts
    try:
        if os.path.getsize(path) < 50:
            continue
    except Exception:
        continue

    try:
        with open(path, "rb") as f:
            msg = BytesParser(policy=policy.default).parse(f)
    except Exception:
        continue

    body_text = ""
    body_html = ""
    attachments = []
    part_counter = 0

    if msg.is_multipart():
        for part in msg.walk():
            if part.is_multipart():
                continue

            part_counter += 1

            ctype = (part.get_content_type() or "").lower()
            disposition = part.get_content_disposition()  # 'attachment' | 'inline' | None
            filename = part.get_filename()

            if filename:
                filename = decode_mime(filename)

            # Sometimes filename exists only as Content-Type "name" param
            # policy.default supports get_param; check Content-Type header params
            name_param = part.get_param("name", header="Content-Type")
            if name_param and not filename:
                filename = decode_mime(name_param)

            payload_bytes = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"

            is_text_plain = (ctype == "text/plain")
            is_text_html = (ctype == "text/html")

            # Decide if this is body or attachment
            # Treat as body ONLY when it's text/plain or text/html AND no attachment indicators.
            has_attachment_indicator = (
                disposition in ("attachment", "inline") or
                bool(filename) or
                bool(name_param) or
                (ctype not in ("text/plain", "text/html") and ctype != "")
            )

            # --- BODY ---
            if not has_attachment_indicator and is_text_plain:
                try:
                    body_text += payload_bytes.decode(charset, errors="replace")
                except Exception:
                    body_text += payload_bytes.decode("utf-8", errors="replace")

            elif not has_attachment_indicator and is_text_html:
                try:
                    body_html += payload_bytes.decode(charset, errors="replace")
                except Exception:
                    body_html += payload_bytes.decode("utf-8", errors="replace")

            # --- ATTACHMENT ---
            else:
                # If it's text/plain/html but marked inline/attachment OR has filename, still treat as attachment.
                if not filename:
                    # fallback filename
                    filename = f"part-{part_counter}{guess_ext(ctype)}"

                att = {
                    "filename": filename,
                    "content_type": ctype or None,
                    "size": len(payload_bytes),
                    "content_disposition": disposition  # useful for debugging
                }

                if payload_bytes:
                    att["md5"] = hashlib.md5(payload_bytes).hexdigest()
                    att["sha256"] = hashlib.sha256(payload_bytes).hexdigest()

                attachments.append(att)

    else:
        payload_bytes = msg.get_payload(decode=True) or b""
        charset = msg.get_content_charset() or "utf-8"
        try:
            body_text = payload_bytes.decode(charset, errors="replace")
        except Exception:
            body_text = payload_bytes.decode("utf-8", errors="replace")

    emails.append({
        "message_id": msg.get("Message-ID"),
        "date": msg.get("Date"),
        "from": msg.get_all("From", []),
        "to": msg.get_all("To", []),
        "cc": msg.get_all("Cc", []),
        "bcc": msg.get_all("Bcc", []),
        "subject": decode_mime(msg.get("Subject") or ""),
        "body_text": body_text.strip() or None,
        "body_html": body_html.strip() or None,
        "attachments": attachments
    })

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(emails, f, indent=2, ensure_ascii=False)

# quick visibility / sanity stats
total_atts = sum(len(e.get("attachments", [])) for e in emails)
with_atts = sum(1 for e in emails if e.get("attachments"))
print(f"Extracted {len(emails)} emails → {OUT_JSON}")
print(f"Attachments: total={total_atts}, emails_with_attachments={with_atts}")
