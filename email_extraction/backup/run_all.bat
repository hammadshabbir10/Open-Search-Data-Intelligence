@echo off
echo ==================================================
echo SMTP Data Pipeline for OpenSearch
echo ==================================================
echo.

echo Step 1: Cleaning CSV data...
python clean_data_fixed.py
if errorlevel 1 (
    echo ❌ Error in cleaning data
    pause
    exit /b 1
)
echo.

echo Step 2: Creating OpenSearch index...
python create_index.py
if errorlevel 1 (
    echo ❌ Error creating index
    pause
    exit /b 1
)
echo.

echo Step 3: Importing data to OpenSearch...
python import_data.py
if errorlevel 1 (
    echo ❌ Error importing data
    pause
    exit /b 1
)
echo.

echo ==================================================
echo ✅ SETUP COMPLETE!
echo ==================================================
echo.
echo Open your browser and go to:
echo   http://localhost:5601
echo.
echo Steps to create visualizations:
echo   1. Click "Analytics" -> "Visualize"
echo   2. Click "Create visualization"
echo   3. Select chart type (Pie, Line, etc.)
echo   4. Select index pattern: email-traffic*
echo   5. Configure as shown below
echo.
echo Press any key to continue...
pause > nul

echo.
echo 📊 RECOMMENDED VISUALIZATIONS:
echo ===============================
echo.
echo 1. PIE CHART - SMTP Commands:
echo    - Chart Type: Pie
echo    - Aggregation: Terms
echo    - Field: smtp_command.keyword
echo    - Size: 10
echo.
echo 2. LINE CHART - Traffic Over Time:
echo    - Chart Type: Line
echo    - X-axis: Date Histogram
echo    - Field: timestamp
echo    - Interval: Auto
echo    - Y-axis: Count
echo.
echo 3. PIE CHART - Encryption Status:
echo    - Chart Type: Pie
echo    - Aggregation: Terms
echo    - Field: encryption_status.keyword
echo.
echo 4. DATA TABLE - Top Source IPs:
echo    - Chart Type: Data Table
echo    - Aggregation: Terms
echo    - Field: source_ip
echo    - Size: 10
echo.
echo 5. DATA TABLE - Top Destination IPs:
echo    - Chart Type: Data Table
echo    - Aggregation: Terms
echo    - Field: destination_ip
echo    - Size: 10
echo.
pause