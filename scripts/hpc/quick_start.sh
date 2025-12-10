#!/bin/bash
# ============================================================
# Quick Start Script for NYU HPC GDELT Pipeline
# ============================================================
# Run this script after connecting to NYU HPC via SSH
#
# Usage:
#   chmod +x scripts/hpc/quick_start.sh
#   ./scripts/hpc/quick_start.sh
# ============================================================

echo "============================================================"
echo "NYU HPC GDELT Pipeline - Quick Start"
echo "============================================================"

# Check if we're on HPC
if [[ ! -d "/scratch" ]]; then
    echo "ERROR: This script should be run on NYU HPC!"
    echo "Please connect to greene.hpc.nyu.edu first."
    exit 1
fi

# Set up directories
PROJECT_DIR="/scratch/$USER/RDBA"

# Check if project exists
if [[ ! -d "$PROJECT_DIR" ]]; then
    echo "Project directory not found. Please clone the repository first:"
    echo "  cd /scratch/$USER"
    echo "  git clone <your-repo-url> RDBA"
    exit 1
fi

cd $PROJECT_DIR
echo "Working directory: $(pwd)"

# Create necessary directories
mkdir -p logs
mkdir -p market_data/gdelt/bulk_6months
mkdir -p data/silver/gdelt
mkdir -p data/gold/gdelt_features
mkdir -p analysis_output

# Load modules
echo ""
echo "Loading Python module..."
module purge
module load python/intel/3.8.6

# Check for virtual environment
if [[ ! -d "venv" ]]; then
    echo ""
    echo "Creating virtual environment..."
    python -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

echo "Python: $(which python)"
echo "Python version: $(python --version)"

# Menu
echo ""
echo "============================================================"
echo "Select an option:"
echo "============================================================"
echo "1) Test GDELT fetch (7 days, quick test)"
echo "2) Submit 6-month fetch job (GB-scale, background)"
echo "3) Run analysis on existing data"
echo "4) Check job status"
echo "5) View recent logs"
echo "6) Exit"
echo ""
read -p "Enter choice [1-6]: " choice

case $choice in
    1)
        echo ""
        echo "Running 7-day test fetch..."
        python scripts/gdelt_bulk_fetch.py --days 7 --output market_data/gdelt/bulk_test
        ;;
    2)
        echo ""
        echo "Submitting 6-month fetch job..."
        # Update email in sbatch file
        read -p "Enter your NYU email: " email
        sed -i "s/YOUR_EMAIL@nyu.edu/$email/g" scripts/hpc/submit_gdelt_fetch.sbatch
        sbatch scripts/hpc/submit_gdelt_fetch.sbatch
        echo "Job submitted! Check status with: squeue -u $USER"
        ;;
    3)
        echo ""
        echo "Running analysis..."
        if [[ -f "market_data/gdelt/bulk_6months/gdelt_bulk_combined.csv" ]]; then
            python scripts/analyze_gdelt.py \
                --input market_data/gdelt/bulk_6months \
                --output analysis_output
        else
            echo "No data found. Please run data fetch first."
        fi
        ;;
    4)
        echo ""
        echo "Current jobs:"
        squeue -u $USER
        ;;
    5)
        echo ""
        echo "Recent log files:"
        ls -lt logs/*.out 2>/dev/null | head -5
        echo ""
        read -p "View latest log? [y/n]: " view
        if [[ "$view" == "y" ]]; then
            latest_log=$(ls -t logs/*.out 2>/dev/null | head -1)
            if [[ -f "$latest_log" ]]; then
                tail -100 "$latest_log"
            fi
        fi
        ;;
    6)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "Done!"
