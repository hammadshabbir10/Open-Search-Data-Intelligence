import subprocess
import json

TSHARK = r"C:\Program Files\Wireshark\tshark.exe"
PCAP = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\smtp-July-28.pcap"
OUT_JSON = r"D:\Opensearch\Open-Search-Data-Intelligence\email_extraction\network_flows.json"

# We extract BOTH SMTP-layer fields AND some size + TLS indicators.
# This helps you estimate plaintext vs encrypted (e.g., STARTTLS + subsequent TLS records).
cmd = [
    TSHARK,
    "-r", PCAP,

    # Only frames where tshark can decode smtp AND have IP.
    "-Y", "smtp && ip",

    "-T", "fields",
    "-E", "separator=|",
    "-E", "occurrence=f",
    "-E", "quote=n",

    # Core flow identity
    "-e", "frame.time_epoch",
    "-e", "ip.src",
    "-e", "tcp.srcport",
    "-e", "ip.dst",
    "-e", "tcp.dstport",

    # SMTP forensic fields
    "-e", "smtp.command_line",
    "-e", "smtp.req.command",
    "-e", "smtp.req.parameter",
    "-e", "smtp.response.code",
    "-e", "smtp.response",

    # Traffic sizing (helpful for plaintext/encrypted comparisons)
    "-e", "tcp.len",
    "-e", "frame.len",

    # TLS hint (will usually be empty in smtp-decoded frames, but safe to include)
    "-e", "tls.record.content_type",
]

result = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    encoding="utf-8",
    errors="replace"
)

flows = []

def to_int_or_none(x: str):
    try:
        x = (x or "").strip()
        return int(x) if x != "" else None
    except Exception:
        return None

for line in result.stdout.splitlines():
    parts = line.strip().split("|")

    # Expect at least the first 5 fields to be meaningful
    if len(parts) < 5:
        continue
    if not parts[0] or not parts[1] or not parts[3]:
        continue

    try:
        timestamp = float(parts[0])
    except ValueError:
        continue  # discard corrupted frames

    # Safe getters
    def get(idx):
        return parts[idx] if idx < len(parts) and parts[idx] != "" else None

    smtp_command_line = get(5)
    smtp_req_command = get(6)
    smtp_req_parameter = get(7)
    smtp_response_code = to_int_or_none(get(8) or "")
    smtp_response = get(9)

    tcp_len = to_int_or_none(get(10) or "")
    frame_len = to_int_or_none(get(11) or "")

    tls_content_type = get(12)

    # STARTTLS inference (plaintext command that switches to encryption)
    is_starttls = False
    if smtp_req_command and smtp_req_command.strip().upper() == "STARTTLS":
        is_starttls = True
    elif smtp_command_line and smtp_command_line.strip().upper().startswith("STARTTLS"):
        is_starttls = True

    flows.append({
        "timestamp": timestamp,
        "src_ip": parts[1],
        "src_port": to_int_or_none(parts[2]),
        "dst_ip": parts[3],
        "dst_port": to_int_or_none(parts[4]),
        "protocol": "SMTP",

        # SMTP forensic fields
        "smtp_command_line": smtp_command_line,
        "smtp_req_command": smtp_req_command,
        "smtp_req_parameter": smtp_req_parameter,
        "smtp_response_code": smtp_response_code,
        "smtp_response": smtp_response,

        # Useful sizing hints
        "tcp_len": tcp_len,
        "frame_len": frame_len,

        # TLS hint (rarely populated in smtp-decoded frames, but kept if present)
        "tls_record_content_type": tls_content_type,

        # Derived hint for STARTTLS
        "is_starttls": is_starttls,
    })

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(flows, f, indent=2, ensure_ascii=False)

print(f"Extracted {len(flows)} clean SMTP network flows → {OUT_JSON}")
