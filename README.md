# Open Search Data Intelligence

A comprehensive network forensics and email analysis platform powered by OpenSearch. Extract, parse, index, and visualize email metadata and attachments from PCAP files with advanced analytics and intelligence capabilities.

## Overview

Open Search Data Intelligence is an enterprise-grade platform designed to help security teams and forensic analysts extract, analyze, and visualize network traffic data, with a focus on email intelligence. Built for team collaboration and scalability.

## Key Features

- **PCAP Email Extraction**: Extract emails from network packet capture files (PCAP/PCAPNG)
- **Metadata Parsing**: Parse and structure email headers, recipients, and metadata
- **Attachment Analysis**: Identify, track, and analyze email attachments
- **OpenSearch Indexing**: Full-text search and advanced analytics on email data
- **Visualization Dashboard**: Interactive Discover and Dashboard views
- **Bulk Import**: Efficient bulk import of parsed email data
- **Forensic Analysis**: Timeline analysis and network intelligence
- **Team Collaboration**: Centralized platform for team members

## Technology Stack

- **OpenSearch** - Search and analytics engine
- **Python 3.x** - Email parsing and data processing
- **PCAP Libraries** - Network packet analysis
- **JSON** - Data serialization and indexing

## Project Structure

```
opensearch-analytics-suite/
├── README.md
├── .gitignore
├── create_index.py              # OpenSearch index creation
├── import_data_fixed.py         # Bulk import script
├── clean_data_fixed.py          # Data cleaning utilities
├── smtp_tls_analysis.py         # SMTP/TLS analysis
├── run_all.bat                  # Batch execution script
├── smtp_metadata.csv            # Sample metadata
├── smtp_output.csv              # Output results
├── *.pcap                       # Sample PCAP files
│
└── email_extraction/            # Email extraction module
    ├── extract_simple.py        # Simple extraction script
    ├── parse_emails.py          # Email parser
    ├── index_emails.py          # Email indexing
    ├── create_email_index.py    # Index configuration
    ├── extract_all.bat          # Batch extraction
    ├── find_attachments.py      # Attachment finder
    ├── check_file_ids.py        # File ID checker
    ├── eml_files/               # Extracted EML files
    ├── parsed_emails/           # Parsed JSON data
    │   ├── emails.json          # All parsed emails
    │   └── summary.json         # Parsing statistics
    └── email_extraction/        # Final output
```

## Quick Start

### Prerequisites

- Python 3.8+
- OpenSearch instance running
- Git

### Installation

1. Clone the repository:
```bash
git clone https://github.com/hammadshabbir10/Open-Search-Data-Intelligence.git
cd Open-Search-Data-Intelligence
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure OpenSearch connection:
   - Update `create_index.py` with your OpenSearch host/port
   - Default: `localhost:9200`

### Usage

#### 1. Extract Emails from PCAP
```bash
cd email_extraction
python extract_simple.py <path-to-pcap-file>
```

#### 2. Parse Email Metadata
```bash
python parse_emails.py
```

#### 3. Create OpenSearch Index
```bash
cd ..
python create_index.py
```

#### 4. Import Data to OpenSearch
```bash
python import_data_fixed.py
```

#### 5. Find Emails with Attachments
```bash
cd email_extraction
python find_attachments.py
```

#### 6. Filter Specific File IDs
```bash
python find_specific_attachments.py
```

### Batch Processing

Run all steps at once:
```bash
.\run_all.bat
```

Or in email_extraction:
```bash
.\extract_all.bat
```

## OpenSearch Queries

### Find Emails with Attachments
```
has_attachments:true
```

### Filter by Sender
```
metadata.from:"sender@example.com"
```

### Filter by File ID
```
file_id:(1036 OR 1062)
```

### Combined Query
```
has_attachments:true AND metadata.from:"sender@example.com"
```

## Visualizations

The platform supports various visualizations for email analysis:

- **Table** - Detailed email and attachment information
- **Bar Chart** - Attachment type distribution
- **Pie Chart** - Sender/recipient statistics
- **Timeline** - Email frequency over time
- **Metrics** - Total emails, attachments, unique senders/recipients

## Statistics

Sample from included PCAP files:
- **Total Emails Parsed**: 1,078
- **Unique Senders**: 56
- **Unique Recipients**: 64
- **Emails with Attachments**: 6
- **Total Attachments**: 20

## Troubleshooting

### JSON Parse Error (Duplicate Keys)
PowerShell's `ConvertFrom-Json` fails with duplicate keys. Use Python scripts instead:
```powershell
python check_file_ids.py
```

### OpenSearch Connection Failed
Verify OpenSearch is running:
```bash
curl http://localhost:9200/
```

### Large PCAP Import Timeout
Increase Git buffer for uploads:
```bash
git config http.postBuffer 524288000
```

## Contributing

1. Create a feature branch: `git checkout -b feature/analysis-tool`
2. Commit changes: `git commit -am 'Add new analysis feature'`
3. Push to branch: `git push origin feature/analysis-tool`
4. Submit a pull request

## Team Members

- Lead: [Your Name]
- Contributors: [Team Members]

## License

This project is licensed under the MIT License - see LICENSE file for details.

## Support

For issues, questions, or feature requests:
- Open an issue on GitHub
- Contact the development team
- Review documentation in `/docs`

## Roadmap

- [ ] Real-time email stream processing
- [ ] Machine learning anomaly detection
- [ ] Email threat intelligence integration
- [ ] Advanced visualization dashboards
- [ ] REST API for remote access
- [ ] Elasticsearch compatibility mode

---

**Last Updated**: January 21, 2026  
**Version**: 1.0.0  
**Status**: Production Ready
