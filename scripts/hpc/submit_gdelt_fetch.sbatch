#!/bin/bash
#SBATCH --job-name=gdelt_fetch
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32GB
#SBATCH --time=24:00:00
#SBATCH --output=logs/gdelt_fetch_%j.out
#SBATCH --error=logs/gdelt_fetch_%j.err
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=YOUR_EMAIL@nyu.edu

# ============================================================
# GDELT Bulk Data Fetch Job for NYU HPC
# ============================================================
# This script fetches 6 months of GDELT news data (~1GB)
# Estimated runtime: 6-12 hours
# ============================================================

echo "============================================================"
echo "GDELT Bulk Fetch Job Started"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start Time: $(date)"
echo "============================================================"

# Navigate to project directory
cd /scratch/$USER/RDBA

# Create logs directory if not exists
mkdir -p logs

# Load Python module
module purge
module load python/intel/3.8.6

# Activate virtual environment
source venv/bin/activate

# Verify Python environment
echo "Python: $(which python)"
echo "Python version: $(python --version)"

# Create output directory
mkdir -p market_data/gdelt/bulk_6months

# ============================================================
# Run GDELT Bulk Fetch - 6 Months for GB-scale data
# ============================================================
# Options:
#   --months 6    : Fetch 6 months of data (~500MB - 1GB)
#   --maxrecords 250 : Maximum articles per query per day
#   --output      : Output directory
#
# For different data sizes:
#   --months 3    : ~200-500 MB
#   --months 12   : ~1-2 GB
#   --start/--end : Custom date range
# ============================================================

echo ""
echo "Starting GDELT bulk fetch (6 months)..."
echo ""

python scripts/gdelt_bulk_fetch.py \
    --months 6 \
    --maxrecords 250 \
    --output market_data/gdelt/bulk_6months

FETCH_STATUS=$?

echo ""
echo "============================================================"
echo "Fetch completed with status: $FETCH_STATUS"
echo "============================================================"

# ============================================================
# Process through Silver and Gold layers
# ============================================================
if [ $FETCH_STATUS -eq 0 ]; then
    echo ""
    echo "Processing data through Silver and Gold layers..."
    echo ""

    python fetch_data.py \
        --gdelt-silver \
        --gdelt-gold

    PROCESS_STATUS=$?
    echo "Processing completed with status: $PROCESS_STATUS"
fi

# ============================================================
# Report data sizes
# ============================================================
echo ""
echo "============================================================"
echo "Data Size Report"
echo "============================================================"

echo "Bronze (raw data):"
du -sh market_data/gdelt/bulk_6months/ 2>/dev/null || echo "  Not found"

echo "Silver (cleaned data):"
du -sh data/silver/gdelt/ 2>/dev/null || echo "  Not found"

echo "Gold (features):"
du -sh data/gold/gdelt_features/ 2>/dev/null || echo "  Not found"

echo ""
echo "Total articles in combined file:"
wc -l market_data/gdelt/bulk_6months/gdelt_bulk_combined.csv 2>/dev/null || echo "  Not found"

echo ""
echo "============================================================"
echo "Job Completed: $(date)"
echo "============================================================"
