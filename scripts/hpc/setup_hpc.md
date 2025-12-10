# NYU HPC Setup Guide for GDELT Data Pipeline

## 1. VSCode Remote-SSH Connection to NYU HPC

### Step 1: Install VSCode Extension
1. Open VSCode
2. Go to Extensions (Ctrl+Shift+X)
3. Search for "Remote - SSH" and install it

### Step 2: Configure SSH Connection
1. Press `F1` or `Ctrl+Shift+P`
2. Type "Remote-SSH: Open SSH Configuration File"
3. Select the config file (usually `~/.ssh/config`)
4. Add the following configuration:

```ssh-config
# NYU HPC Greene Cluster
Host nyu-hpc
    HostName greene.hpc.nyu.edu
    User YOUR_NETID
    ForwardAgent yes

# NYU HPC Gateway (if needed)
Host nyu-gw
    HostName gw.hpc.nyu.edu
    User YOUR_NETID
    ForwardAgent yes

# Connect through gateway if off-campus
Host nyu-hpc-gateway
    HostName greene.hpc.nyu.edu
    User YOUR_NETID
    ProxyJump nyu-gw
    ForwardAgent yes
```

Replace `YOUR_NETID` with your NYU NetID.

### Step 3: Connect to HPC
1. Press `F1` → "Remote-SSH: Connect to Host"
2. Select `nyu-hpc` (on campus) or `nyu-hpc-gateway` (off campus)
3. Enter your NYU password when prompted
4. Complete Duo 2FA authentication

### Step 4: Open Project Folder
Once connected:
1. File → Open Folder
2. Navigate to your project directory (e.g., `/scratch/YOUR_NETID/RDBA`)

## 2. Initial HPC Setup

After connecting, run these commands in the terminal:

```bash
# Navigate to scratch directory (better for large data)
cd /scratch/$USER

# Clone the repository (if not already done)
git clone https://github.com/YOUR_REPO/RDBA.git
cd RDBA

# Load Python module
module load python/intel/3.8.6

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Directory Structure on HPC

```
/scratch/YOUR_NETID/RDBA/
├── data/
│   ├── silver/gdelt/          # Cleaned data (Parquet)
│   └── gold/gdelt_features/   # Feature data
├── market_data/
│   └── gdelt/
│       └── bulk/              # Raw GDELT data (Bronze)
├── scripts/
│   └── hpc/
│       ├── submit_gdelt_fetch.sbatch
│       └── submit_analysis.sbatch
└── logs/                      # Job logs
```

## 4. Running Jobs

### Interactive Session (for testing)
```bash
# Request an interactive session
srun --cpus-per-task=4 --mem=16GB --time=04:00:00 --pty /bin/bash

# Activate environment and run
source venv/bin/activate
python scripts/gdelt_bulk_fetch.py --days 7
```

### Batch Job (for large-scale fetching)
```bash
# Submit the GDELT fetch job
sbatch scripts/hpc/submit_gdelt_fetch.sbatch

# Check job status
squeue -u $USER

# View job output
tail -f logs/gdelt_fetch_*.out
```

## 5. Data Transfer

### Download data from HPC to local machine
```bash
# From your local machine
scp -r YOUR_NETID@greene.hpc.nyu.edu:/scratch/YOUR_NETID/RDBA/market_data/gdelt/bulk ./local_data/
```

### Using Globus (recommended for large files)
1. Go to https://www.globus.org
2. Login with NYU credentials
3. Search for "NYU Greene" endpoint
4. Transfer files to your local Globus endpoint

## 6. Troubleshooting

### Connection Issues
- **Off-campus**: Use VPN or gateway connection
- **Timeout**: Check Duo 2FA, may need to re-authenticate

### Job Issues
- **Out of memory**: Increase `--mem` in sbatch script
- **Timeout**: Increase `--time` limit
- **Check logs**: `cat logs/gdelt_fetch_*.err`

### Python Issues
```bash
# Reload modules if issues
module purge
module load python/intel/3.8.6
source venv/bin/activate
```
