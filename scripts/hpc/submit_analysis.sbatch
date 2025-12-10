#!/bin/bash
#SBATCH --job-name=gdelt_analysis
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64GB
#SBATCH --time=04:00:00
#SBATCH --output=logs/gdelt_analysis_%j.out
#SBATCH --error=logs/gdelt_analysis_%j.err
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=YOUR_EMAIL@nyu.edu

# ============================================================
# GDELT Data Analysis Job for NYU HPC
# ============================================================
# This script analyzes the collected GDELT data
# Estimated runtime: 30 minutes - 2 hours (depending on data size)
# ============================================================

echo "============================================================"
echo "GDELT Analysis Job Started"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start Time: $(date)"
echo "============================================================"

# Navigate to project directory
cd /scratch/$USER/RDBA

# Create directories
mkdir -p logs
mkdir -p analysis_output

# Load Python module
module purge
module load python/intel/3.8.6

# Activate virtual environment
source venv/bin/activate

# Verify Python environment
echo "Python: $(which python)"
echo "Python version: $(python --version)"

# ============================================================
# Run Analysis on Bronze Data (raw articles)
# ============================================================
echo ""
echo "Analyzing Bronze layer data..."
echo ""

python scripts/analyze_gdelt.py \
    --input market_data/gdelt/bulk_6months \
    --output analysis_output/bronze_analysis \
    --data-type bronze

BRONZE_STATUS=$?
echo "Bronze analysis status: $BRONZE_STATUS"

# ============================================================
# Run Analysis on Gold Data (features)
# ============================================================
echo ""
echo "Analyzing Gold layer data..."
echo ""

python scripts/analyze_gdelt.py \
    --input data/gold/gdelt_features \
    --output analysis_output/gold_analysis \
    --data-type gold

GOLD_STATUS=$?
echo "Gold analysis status: $GOLD_STATUS"

# ============================================================
# Summary
# ============================================================
echo ""
echo "============================================================"
echo "Analysis Output Files"
echo "============================================================"

echo "Bronze Analysis:"
ls -la analysis_output/bronze_analysis/ 2>/dev/null || echo "  Not found"

echo ""
echo "Gold Analysis:"
ls -la analysis_output/gold_analysis/ 2>/dev/null || echo "  Not found"

echo ""
echo "============================================================"
echo "Job Completed: $(date)"
echo "============================================================"
