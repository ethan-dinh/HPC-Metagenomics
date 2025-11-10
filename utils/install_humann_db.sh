#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -N install_humann_metaphlan_dbs
#$ -pe smp 1
#$ -l mem_free=1G
#$ -l h_rt=24:00:00
#$ -j y
#$ -o $HOME/logs/install_humann_metaphlan_dbs_$JOB_ID.log

# =====================================================
# Script: install_humann_metaphlan_dbs.sh
# Purpose: Install HUMAnN (ChocoPhlAn, UniRef90) and MetaPhlAn databases
# Author: Ethan Dinh
# =====================================================

# Expand home directory properly
HUMANN_DB_DIR="$HOME/metagenomics/databases/humann_dbs"
METAPHLAN_DB_DIR="$HOME/metagenomics/databases/metaphlan_db"
LOG_FILE="$HOME/logs/install_humann_metaphlan_dbs.log"

# Create database directories if they do not exist
mkdir -p "$HUMANN_DB_DIR" "$METAPHLAN_DB_DIR"

echo "====================================================="
echo "[$(date)] Starting HUMAnN and MetaPhlAn database installation"
echo "Target directories:"
echo "  HUMAnN:    $HUMANN_DB_DIR"
echo "  MetaPhlAn: $METAPHLAN_DB_DIR"
echo "====================================================="

# Function to run installation commands on the development node
run_remote_install() {
    local script_content="$1"
    echo "[$(date)] Executing installation on dev node..."
    ssh -i "$HOME/.ssh/wynton-dtn-key" \
        dev1.wynton.ucsf.edu \
        "bash -s" <<< "$script_content" >> "$LOG_FILE" 2>&1
    if [[ $? -ne 0 ]]; then
        echo "[$(date)] ERROR: Installation failed on dev node."
        exit 1
    fi
}

# -----------------------------
# Install HUMAnN Databases
# -----------------------------
if [[ -d "$HUMANN_DB_DIR/chocophlan" && -d "$HUMANN_DB_DIR/uniref" ]]; then
    echo "[$(date)] HUMAnN databases already exist in $HUMANN_DB_DIR"
else
    echo "[$(date)] Installing HUMAnN databases..."
    install_humann_script=$(cat <<EOF
#!/bin/bash
module load CBI miniforge3
conda activate metagenomics
set -e
echo "Downloading ChocoPhlAn..."
humann_databases --download chocophlan full "$HUMANN_DB_DIR"
echo "Downloading UniRef90 (DIAMOND)..."
humann_databases --download uniref uniref90_diamond "$HUMANN_DB_DIR"
EOF
)
    run_remote_install "$install_humann_script"
    echo "[$(date)] HUMAnN databases installation completed."
fi

# -----------------------------
# Install MetaPhlAn Database
# -----------------------------
if [[ -d "$METAPHLAN_DB_DIR" && -n "$(ls -A $METAPHLAN_DB_DIR 2>/dev/null)" ]]; then
    echo "[$(date)] MetaPhlAn database already exists in $METAPHLAN_DB_DIR"
else
    echo "[$(date)] Installing MetaPhlAn database..."
    install_metaphlan_script=$(cat <<EOF
#!/bin/bash
module load CBI miniforge3
conda activate metagenomics
set -e
echo "Installing MetaPhlAn Bowtie2 database..."
metaphlan --install --index mpa_vOct22_CHOCOPhlAnSGB_202403 \
  --bowtie2db "$METAPHLAN_DB_DIR"
EOF
)
    run_remote_install "$install_metaphlan_script"
    echo "[$(date)] MetaPhlAn database installation completed."
fi

echo "====================================================="
echo "[$(date)] All database installations completed successfully."
echo "====================================================="
