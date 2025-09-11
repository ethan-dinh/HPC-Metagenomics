#! /bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -N build_braken_db
#$ -pe smp 10
#$ -l mem_free=2G
#$ -l h_rt=6:00:00
#$ -j y
#$ -o /wynton/home/rotation/edinh/logs/build_braken_db_$JOB_ID.log

# -------------------------------------------------------------- #
# MODULES AND ENVIRONMENT
# -------------------------------------------------------------- #
module load CBI miniforge3
conda activate metagenomics

# -------------------------------------------------------------- #
# BUILD BRAKEN DATABASE
# -------------------------------------------------------------- #

# Build the Braken database
BRAKEN_DB_DIR="${HOME}/metagenomics/databases/bracken_db"
mkdir -p "${BRAKEN_DB_DIR}"

# Paths
SRC_DB="/wynton/group/databases/kraken2"
DST_DB="${HOME}/metagenomics/databases/kraken2_mirror"
mkdir -p "$DST_DB"

# Symlink every file from SRC into DST, preserving tree structure
if [[ ! -d "$DST_DB" ]]; then
    cp -as "$SRC_DB"/. "$DST_DB"/
fi

# Navigate to the Braken database directory
echo "=========================================="
echo "Buiding Braken database..."
echo "SRC_DB: ${SRC_DB}"
echo "DST_DB: ${DST_DB}"
echo "NUM THREADS: ${NSLOTS:-1}"
echo "=========================================="

cd "${BRAKEN_DB_DIR}"
bracken-build -d "${HOME}/metagenomics/databases/kraken2_mirror/" -t "${NSLOTS:-1}"
echo "Braken database built"

# Move the necessary files to the Braken database directory
echo "Moving necessary files to Braken database directory..."
mv "${DST_DB}/database100mers.kmer_distrib" "${BRAKEN_DB_DIR}/"
mv "${DST_DB}/database100mers.kraken" "${BRAKEN_DB_DIR}/"
mv "${DST_DB}/database.kraken" "${BRAKEN_DB_DIR}/"

# Delete the temporary directory
echo "Deleting temporary directory..."
rm -rf "${DST_DB}"

# Remove the kraken database
echo "Removing unnecessary kraken database..."
rm -f "${BRAKEN_DB_DIR}/database.kraken"

echo "Braken database built"