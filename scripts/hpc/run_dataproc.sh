#!/bin/bash
# ============================================================
# GDELT GB-Scale Data Fetch Script for NYU Dataproc
# ============================================================
# Usage:
#   chmod +x scripts/hpc/run_dataproc.sh
#   ./scripts/hpc/run_dataproc.sh
# ============================================================

echo "============================================================"
echo "GDELT GB-Scale Data Collection - NYU Dataproc"
echo "Start Time: $(date)"
echo "============================================================"

# Navigate to project directory
cd ~/RDBA

# Create directories
mkdir -p market_data/gdelt/bulk logs

# Setup Python environment (Dataproc uses python3 directly)
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo ""
echo "============================================================"
echo "Starting GDELT Bulk Fetch (6 months ~ 1GB)"
echo "============================================================"
echo ""

# Run the bulk fetch (6 months of data)
python scripts/gdelt_bulk_fetch.py \
    --months 6 \
    --maxrecords 250 \
    --output market_data/gdelt/bulk

echo ""
echo "============================================================"
echo "Data Collection Complete: $(date)"
echo "============================================================"

# Show data size
echo ""
echo "Data Size:"
du -sh market_data/gdelt/bulk/
echo ""
echo "Total articles:"
wc -l market_data/gdelt/bulk/gdelt_bulk_combined.csv 2>/dev/null || echo "Combined file not found"
