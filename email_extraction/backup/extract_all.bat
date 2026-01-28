@echo off
echo ========================================
echo COMPLETE EMAIL EXTRACTION
echo ========================================
echo.

echo Step 1: Creating directories...
mkdir email_extraction 2>nul
cd email_extraction
mkdir eml_files 2>nul
mkdir parsed_emails 2>nul

echo Step 2: Copying PCAP file...
copy "..\smtp-July-28.pcap" . 2>nul

echo Step 3: Extracting .eml files from PCAP...
python extract_simple.py
echo.

echo Step 4: Parsing email content...
python parse_emails.py
echo.

echo Step 5: Indexing to OpenSearch...
python index_emails.py
echo.

echo ========================================
echo ✅ EXTRACTION COMPLETE!
echo ========================================
echo.
echo Your data is now in 2 OpenSearch indexes:
echo 1. email-traffic    (SMTP metadata)
echo 2. email-content-full (Full emails)
echo.
echo Open: http://localhost:5601
echo.
pause