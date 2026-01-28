import subprocess
import json
import os
import sys

TSHARK = r"C:\Program Files\Wireshark\tshark.exe"
PCAP = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\smtp-July-28.pcap"
OUT_JSON = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\network_flows.json"

cmd = [
    TSHARK,
    "-r", PCAP,

    # 🔒 CRITICAL FIX
    "-Y", "smtp && ip",

    "-T", "fields",
    "-E", "separator=|",
    "-E", "occurrence=f",
    "-E", "quote=n",

    "-e", "frame.time_epoch",
    "-e", "ip.src",
    "-e", "tcp.srcport",
    "-e", "ip.dst",
    "-e", "tcp.dstport",
    "-e", "smtp.req.command",
]

result = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    encoding="utf-8",
    errors="replace"
)

flows = []

for line in result.stdout.splitlines():
    parts = line.strip().split("|")

    # Hard validation
    if len(parts) < 5:
        continue
    if not parts[0] or not parts[1] or not parts[3]:
        continue

    try:
        timestamp = float(parts[0])
    except ValueError:
        continue  # 🚫 discard corrupted frames

    flows.append({
        "timestamp": timestamp,
        "src_ip": parts[1],
        "src_port": parts[2] if parts[2] else None,
        "dst_ip": parts[3],
        "dst_port": parts[4] if parts[4] else None,
        "smtp_command": parts[5] if len(parts) > 5 else None,
        "protocol": "SMTP"
    })

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(flows, f, indent=2)

print(f"Extracted {len(flows)} clean SMTP network flows → {OUT_JSON}")
