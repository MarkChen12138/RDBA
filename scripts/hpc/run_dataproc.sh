#!/bin/bash
# ============================================================
# GDELT GB-Scale Data Fetch & Analysis Script for NYU Dataproc
# ============================================================
# Usage:
#   chmod +x scripts/hpc/run_dataproc.sh
#   ./scripts/hpc/run_dataproc.sh
#
# Or run in background:
#   nohup ./scripts/hpc/run_dataproc.sh > logs/fetch.log 2>&1 &
# ============================================================

set -e  # Exit on error

# Project directory
PROJECT_DIR=~/RDBA
cd $PROJECT_DIR

# Create directories
mkdir -p market_data/gdelt/bulk logs data/silver/gdelt data/gold/gdelt_features analysis_output

echo "============================================================"
echo "GDELT GB-Scale Data Collection & Analysis - NYU Dataproc"
echo "Start Time: $(date)"
echo "Project Dir: $PROJECT_DIR"
echo "============================================================"

# ============================================================
# Step 1: Setup Python Environment
# ============================================================
echo ""
echo "[Step 1/4] Setting up Python environment..."
echo "------------------------------------------------------------"

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
echo "Python: $(which python)"
echo "Python version: $(python --version)"

echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "Dependencies installed."

# ============================================================
# Step 2: Fetch GB-Scale GDELT Data (6 months)
# ============================================================
echo ""
echo "[Step 2/4] Fetching GDELT Data (6 months ~ 1GB)..."
echo "------------------------------------------------------------"
echo "This will take several hours. Progress will be logged."
echo ""

python scripts/gdelt_bulk_fetch.py \
    --months 6 \
    --maxrecords 250 \
    --output market_data/gdelt/bulk

FETCH_STATUS=$?

if [ $FETCH_STATUS -ne 0 ]; then
    echo "ERROR: Data fetch failed with status $FETCH_STATUS"
    exit 1
fi

echo ""
echo "Data fetch completed successfully!"

# ============================================================
# Step 3: Process Data (Bronze -> Silver -> Gold)
# ============================================================
echo ""
echo "[Step 3/4] Processing data through Silver and Gold layers..."
echo "------------------------------------------------------------"

# Run the data processing pipeline
python fetch_data.py --gdelt-silver --gdelt-gold

PROCESS_STATUS=$?

if [ $PROCESS_STATUS -ne 0 ]; then
    echo "WARNING: Data processing completed with status $PROCESS_STATUS"
fi

# ============================================================
# Step 4: Run Analysis
# ============================================================
echo ""
echo "[Step 4/4] Running comprehensive analysis..."
echo "------------------------------------------------------------"

python scripts/analyze_gdelt.py \
    --input market_data/gdelt/bulk \
    --output analysis_output \
    --data-type bronze

ANALYSIS_STATUS=$?

# ============================================================
# Final Report
# ============================================================
echo ""
echo "============================================================"
echo "PIPELINE COMPLETE"
echo "============================================================"
echo "End Time: $(date)"
echo ""

echo "Data Sizes:"
echo "------------------------------------------------------------"
echo "Bronze (raw data):"
du -sh market_data/gdelt/bulk/ 2>/dev/null || echo "  Not found"
echo ""
echo "Silver (cleaned data):"
du -sh data/silver/gdelt/ 2>/dev/null || echo "  Not found"
echo ""
echo "Gold (features):"
du -sh data/gold/gdelt_features/ 2>/dev/null || echo "  Not found"
echo ""

echo "Article Counts:"
echo "------------------------------------------------------------"
if [ -f "market_data/gdelt/bulk/gdelt_bulk_combined.csv" ]; then
    ARTICLE_COUNT=$(wc -l < market_data/gdelt/bulk/gdelt_bulk_combined.csv)
    echo "Total articles in combined file: $ARTICLE_COUNT"
else
    echo "Combined file not found"
fi

echo ""
echo "Output Files:"
echo "------------------------------------------------------------"
echo "- market_data/gdelt/bulk/gdelt_bulk_combined.csv (raw data)"
echo "- market_data/gdelt/bulk/bulk_metadata.json (fetch metadata)"
echo "- data/silver/gdelt/*.parquet (cleaned data)"
echo "- data/gold/gdelt_features/*.parquet (features)"
echo "- analysis_output/analysis_report.txt (analysis report)"
echo "- analysis_output/analysis_results.json (detailed results)"

echo ""
echo "============================================================"
echo "To view the analysis report:"
echo "  cat analysis_output/analysis_report.txt"
echo ""
echo "To download data to local machine:"
echo "  Use the 'DOWNLOAD FILE' button in the top right corner"
echo "============================================================"
