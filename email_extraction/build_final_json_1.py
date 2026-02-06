import json
import ipaddress
from bs4 import BeautifulSoup
import re

BASE = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction"
EMAILS_JSON = f"{BASE}\\emails.json"
NETWORK_JSON = f"{BASE}\\network_flows.json"
OUT_JSON = f"{BASE}\\final_emails.json"

HTML_TAG_RE = re.compile(r"<[^>]+>")

def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

def looks_like_html(text):
    if not text:
        return False
    return bool(HTML_TAG_RE.search(text))

def is_private(ip):
    try:
        return ipaddress.ip_address(ip).is_private
    except Exception:
        return None

emails = json.load(open(EMAILS_JSON, encoding="utf-8"))
flows = json.load(open(NETWORK_JSON, encoding="utf-8"))

OUT = []

for i, email in enumerate(emails):
    net = flows[i] if i < len(flows) else {}

    body_text = email.get("body_text")
    body_html = email.get("body_html")

    # CASE 1: body_text exists but is actually HTML garbage
    if body_text and looks_like_html(body_text):
        body_text = html_to_text(body_text)

    # CASE 2: body_text missing but body_html exists
    elif not body_text and body_html:
        body_text = html_to_text(body_html)

    OUT.append({
        "timestamp": email.get("date"),

        "email": {
            "from": email.get("from", []),
            "to": email.get("to", []),
            "cc": email.get("cc", []),
            "bcc": email.get("bcc", [])
        },

        "message": {
            "message_id": email.get("message_id"),
            "subject": email.get("subject"),
            "content_type": "multipart",
            "body_text": body_text,
            "body_html": body_html
        },

        "network": {
            "protocol": net.get("protocol"),
            "source": {
                "ip": net.get("src_ip"),
                "port": net.get("src_port"),
                "is_private": is_private(net.get("src_ip"))
            },
            "destination": {
                "ip": net.get("dst_ip"),
                "port": net.get("dst_port"),
                "is_private": is_private(net.get("dst_ip"))
            }
        },

        # ✅ SMTP command lines / request/response fields
        # Keeping them separate from "network" to match your event-style schema.
        "smtp": {
            "command_line": net.get("smtp_command_line"),
            "req_command": net.get("smtp_req_command"),
            "req_parameter": net.get("smtp_req_parameter"),
            "response_code": net.get("smtp_response_code"),
            "response": net.get("smtp_response"),
            "is_starttls": net.get("is_starttls"),
            "tcp_len": net.get("tcp_len"),
            "frame_len": net.get("frame_len"),
            "tls_record_content_type": net.get("tls_record_content_type"),
        },

        # ✅ Attachments: carry forward if your email extraction already populated them
        "attachments": email.get("attachments", []),

        "correlation": {
            "cgnat": {"matched": False},
            "radius": {"session_found": False}
        }
    })

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(OUT, f, indent=2, ensure_ascii=False)

print(f"Final dataset ready → {OUT_JSON}")
