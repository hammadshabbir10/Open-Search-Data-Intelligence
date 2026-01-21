import pandas as pd

# Load CSV
df = pd.read_csv("smtp_output.csv")

# Normalize column names
df.columns = df.columns.str.strip()

# Total SMTP packets
total_packets = len(df)

# STARTTLS detection
tls_df = df[
    (df["smtp.req.command"].str.upper() == "STAR") &
    (df["smtp.req.parameter"].str.upper() == "TLS")
]

# Command counts
command_counts = df["smtp.req.command"].value_counts(dropna=True)

# Unique TLS sessions (5-tuple reduced to 4-tuple)
tls_sessions = tls_df[
    ["ip.src", "ip.dst", "tcp.srcport", "tcp.dstport"]
].drop_duplicates()

print("===== SMTP ANALYSIS =====")
print(f"Total SMTP packets      : {total_packets}")
print(f"STARTTLS packets        : {len(tls_df)}")
print(f"Unique TLS sessions     : {len(tls_sessions)}")

print("\n===== SMTP COMMAND COUNTS =====")
print(command_counts)

print("\n===== SAMPLE TLS SESSIONS =====")
print(tls_sessions.head())
