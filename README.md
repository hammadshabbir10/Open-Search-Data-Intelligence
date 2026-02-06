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


## Quick Start

### Prerequisites

- Python 3.8+
- Git
- Opensearch-py package (pip install opensearch-py)

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

### Usage

#### 1. Extract Emails from PCAP
```bash
cd email_extraction
python extract_emails_1.py
```

#### 2. Extract Network related information from PCAP
```bash
python extract_network_1.py
```

#### 3. Build the complete json
```bash
cd ..
python build_final_json_1.py


```


