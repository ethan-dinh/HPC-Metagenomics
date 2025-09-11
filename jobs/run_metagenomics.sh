#!/bin/env bash
#$ -S /bin/bash                                 # Shell to use
#$ -cwd                                         # Run in the current working directory
#$ -N meta_pipe                                 # Job name
#$ -pe smp 10                                   # Number of cores
#$ -l mem_free=6G                               # Memory free per core
#$ -l scratch=100G                              # Scratch space
#$ -l h_rt=8:00:00                              # Max run time
#$ -j y                                         # Join the standard output and error streams
#$ -o ${HOME}/logs/metagenomics_$JOB_ID.log     # Output log file
#$ -tc 35                                       # Number of tasks to run concurrently (40 is the max number of concurrent Box transfers)
##$ -r y                                        # Re-run the job if it fails

# -------------------------------------------------------------- #
# LOGGING FUNCTIONS & COLORS
# -------------------------------------------------------------- #
log() {
    local level="$1"; shift
    local msg="$*"
    local ts=$(date '+%m-%d %I:%M:%S %p')
    local color reset="\033[0m"
    local level_width=7  # Longest level is "SUCCESS" (7 chars)
    local pad

    # Calculate padding after the brackets
    pad=$((level_width - ${#level}))
    [[ $pad -lt 0 ]] && pad=0

    case "$level" in
        INFO)    color="\033[34m" ;;   # blue
        WARN)    color="\033[33m" ;;   # yellow
        ERROR)   color="\033[31m" ;;   # red
        FATAL)   color="\033[35m" ;;   # magenta
        SUCCESS) color="\033[32m" ;;   # green
    esac

    # Errors go to stderr
    if [[ "$level" =~ (ERROR|FATAL) ]]; then
        printf "%b[%s]%*s%b [%s] - %s\n" "$color" "$level" "$pad" "" "$reset" "$ts" "$msg" >&2
    else
        printf "%b[%s]%*s%b [%s] - %s\n" "$color" "$level" "$pad" "" "$reset" "$ts" "$msg"
    fi

    # Exit on fatal errors
    if [[ "$level" == "FATAL" ]]; then
        exit 1
    fi
}

# -------------------------------------------------------------- #
# LOGGING HELPERS
# -------------------------------------------------------------- #
log_new_line() {
    echo ""
}

log_break() {
    local new_line="$1"
    local title="$2"
    if [[ "$new_line" == "1" ]]; then
        log_new_line
    fi
    if [[ -n "$title" ]]; then
        echo -e "\033[1m${title}\033[0m"
        printf '%*s\n' "${#title}" '' | tr ' ' '-'
    fi
}
log_info()  { log INFO  "$*"; }
log_warn()  { log WARN  "$*"; }
log_error() { log ERROR "$*"; }
log_fatal() { log FATAL "$*"; }
log_success() { log SUCCESS "$*"; }

# -------------------------------------------------------------- #
# PREPARE RUN ENVIRONMENT
# -------------------------------------------------------------- #
prepare_run_env() {
    # Set early exit flag to 0
    EARLY_EXIT=0

    log_break 1 "Module, Environment, and Thread Information"
    log_info "Loading modules..."
    module load CBI miniforge3

    log_info "Activating conda environment..."
    conda activate metagenomics

    log_info "Setting thread limits to ${NSLOTS:-1}..."
    THREADS="${NSLOTS:-1}"
    export OMP_NUM_THREADS="${THREADS}" 
    export MKL_NUM_THREADS="${THREADS}"
    export OPENBLAS_NUM_THREADS="${THREADS}"
    export BLAS_NUM_THREADS="${THREADS}"
    export NUMEXPR_NUM_THREADS="${THREADS}"

    # Point all temp files into your scratch
    SCRATCH_DIR="$TMPDIR/fastqc_tmp"
    mkdir -p "$SCRATCH_DIR"

    # Make Java honor it
    export TMPDIR="$SCRATCH_DIR"
    export _JAVA_OPTIONS="-Djava.io.tmpdir=$SCRATCH_DIR"
}

# -------------------------------------------------------------- #
# MANIFEST AND GLOBAL VARIABLES
# -------------------------------------------------------------- #
load_manifest() {
    log_break 1 "Manifest Information"
    log_info "Loading manifest..."

    # Check existence and readability
    if [[ ! -r "${MANIFEST}" ]]; then
        log_error "Manifest not found or not readable: ${MANIFEST}"
        EARLY_EXIT=1
        exit 1
    fi

    # Ensure there is at least one data line beyond the header
    total_lines=$(wc -l < "${MANIFEST}")
    data_lines=$(( total_lines - 1 ))
    if (( data_lines <= 0 )); then
        log_error "Manifest contains only a header and no samples: ${MANIFEST}"
        exit 1
    fi

    # Determine which line to process
    # Priority: explicit TASK_INDEX env > SGE_TASK_ID from array > default to 1
    if [[ -z "${TASK_INDEX}" || "${TASK_INDEX}" == "undefined"  ]]; then
        if [[ -z "${SGE_TASK_ID}" || "${SGE_TASK_ID}" == "undefined" ]]; then
            TASK_INDEX=1
        else
            TASK_INDEX="${SGE_TASK_ID}"
        fi
    else
        TASK_INDEX="${TASK_INDEX}"
    fi

    # Validate the task index
    if ! [[ "$TASK_INDEX" =~ ^[0-9]+$ ]]; then
        log_error "Task index is not numeric: '${TASK_INDEX}'"
        EARLY_EXIT=1
        exit 2
    fi
    if (( TASK_INDEX < 1 || TASK_INDEX > data_lines )); then
        log_error "Bad task index ${TASK_INDEX}. Valid range is 1..${data_lines}"
        EARLY_EXIT=1
        exit 2
    fi

    # Fetch the corresponding data line, skipping the header
    LINE=$(awk -v idx="$TASK_INDEX" 'NR==1{next} {if (++c==idx){print; exit}}' "$MANIFEST")
    if [[ -z "${LINE}" ]]; then
        log_error "Failed to read line ${TASK_INDEX} from manifest"
        EARLY_EXIT=1
        exit 2
    fi

    # Parse columns (whitespace-separated: sample_id R1 R2)
    read -r SAMPLE R1_SRC R2_SRC <<< "${LINE}"

    # Validate fields and inputs
    if [[ -z "${SAMPLE}" || -z "${R1_SRC}" || -z "${R2_SRC}" ]]; then
        log_error "Malformed manifest line ${TASK_INDEX}: '${LINE}'"
        EARLY_EXIT=1
        exit 2
    fi
    if [[ ! -r "${R1_SRC}" ]]; then log_error "R1 not readable: ${R1_SRC}"; exit 3; fi
    if [[ ! -r "${R2_SRC}" ]]; then log_error "R2 not readable: ${R2_SRC}"; exit 3; fi
    
    # Print the manifest information
    log_success "Manifest OK."
    log_info "This task index: ${TASK_INDEX}"
    log_info "SAMPLE: ${SAMPLE}"
    log_info "R1_SRC: ${R1_SRC}"
    log_info "R2_SRC: ${R2_SRC}"

    WORK="${TMPDIR}/${SAMPLE}" # This is the temporary working directory that is created on the node-local scratch space
    KD_OUT="${WORK}/kneaddata" # This is the output directory for kneaddata
    KR_OUT="${WORK}/kraken2"   # This is the output directory for kraken2
    BR_OUT="${WORK}/bracken" # This is the output directory for bracken

    # Defining output directories
    if [[ "$SAVE_TO_SCRATCH" -eq 1 ]]; then
        OUT_PERSIST="/wynton/scratch/$USER/${OUT_BASE_DIR}/${SAMPLE}"
    else
        OUT_PERSIST="${HOME}/${OUT_BASE_DIR}/${SAMPLE}"
    fi

    # Optional: stage Kraken2 DB if small enough. Otherwise read from shared.
    # KRAKEN_DB_DST="${WORK}/kraken_db"; 
    # rsync -a "/hpc/mydata/dbs/kraken2/standard_plus_mouse/" "${KRAKEN_DB_DST}/"
    KRAKEN_DB="/wynton/group/databases/kraken2"

    # Create the working directory and the output directories
    log_break 1 "Working Directory and Output Directories"
    log_info "Creating working directory and output directories..."
    log_info "WORK: ${WORK}"
    log_info "KD_OUT: ${KD_OUT}"
    log_info "KR_OUT: ${KR_OUT}"
    log_info "BR_OUT: ${BR_OUT}"
    mkdir -p "${WORK}" "${KD_OUT}" "${KR_OUT}" "${BR_OUT}" "${OUT_PERSIST}"
}

# -------------------------------------------------------------- #
# STAGE DATA AND DBs TO NODE-LOCAL SCRATCH ($TMPDIR)
# -------------------------------------------------------------- #
stage_kneaddata_data() {
    # Stage the fastqs
    log_info "Copying fastqs to local scratch space: ${WORK}"
    cp -f "${R1_SRC}" "${WORK}/" || {
        log_error "Failed to copy R1 fastq to local scratch space"
        EARLY_EXIT=1
        exit 3
    }
    cp -f "${R2_SRC}" "${WORK}/" || {
        log_error "Failed to copy R2 fastq to local scratch space"
        EARLY_EXIT=1
        exit 3
    }
    log_success "Fastqs copied to local scratch space"

    # Paths to compressed files in scratch
    R1_COMP="${WORK}/$(basename "${R1_SRC}")"
    R2_COMP="${WORK}/$(basename "${R2_SRC}")"

    # Decompress in parallel using pigz
    log_info "Decompressing fastqs with pigz using ${THREADS} threads"
    pigz -d -p "${THREADS}" "${R1_COMP}" || {
        log_error "Failed to decompress ${R1_COMP}"
        EARLY_EXIT=1
        exit 3
    }
    pigz -d -p "${THREADS}" "${R2_COMP}" || {
        log_error "Failed to decompress ${R2_COMP}"
        EARLY_EXIT=1
        exit 3
    }
    log_success "Fastqs decompressed"

    # After pigz -d, the .gz extension is stripped
    R1_LOCAL="${R1_COMP%.gz}"
    R2_LOCAL="${R2_COMP%.gz}"

    # Stage Bowtie2 mouse index (adjust path and size estimate)
    MOUSE_DB_SRC="${HOME}/metagenomics/databases/mouse_C57BL_6NJ"
    MOUSE_DB_DST="${WORK}/mouse_ref"

    mkdir -p "${MOUSE_DB_DST}"

    log_info "Copying Bowtie2 DB to scratch space: ${MOUSE_DB_DST}"
    cp -r "${MOUSE_DB_SRC}/." "${MOUSE_DB_DST}/" || {
        log_error "Failed to copy Bowtie2 mouse index to scratch space"
        EARLY_EXIT=1
        exit 3
    }
    log_success "Bowtie2 mouse index copied to scratch space"
}

stage_bracken_db() {
    # Defining Bracken database directory
    BR_DB_SRC="${HOME}/metagenomics/databases/bracken_db"
    BR_DB_DST="${WORK}/bracken_db"
    BR_KMER_DIST="${BR_DB_SRC}/database100mers.kmer_distrib"
    BR_KRAKEN="${BR_DB_SRC}/database100mers.kraken"

    # Create the destination directory if it doesn't exist
    mkdir -p "${BR_DB_DST}"

    # Check if the Bracken database files exist
    if [[ ! -s "${BR_KMER_DIST}" || ! -s "${BR_KRAKEN}" ]]; then
        log_error "Bracken database files not found"
        EARLY_EXIT=1
        exit 3
    fi

    # Stage the Bracken database
    log_info "Copying Bracken database to scratch space: ${BR_OUT}"    
    cp -f "${BR_KMER_DIST}" "${BR_DB_DST}/"
    cp -f "${BR_KRAKEN}" "${BR_DB_DST}/"
    log_success "Bracken database copied to scratch space"
}

# -------------------------------------------------------------- #
# CHECK TOOLS
# -------------------------------------------------------------- #
check_tools() {

    log_break 1 "Checking if tools are installed in conda environment"
    
    # Checking for KneadData and KneadData-Based Tools
    KNEAD_BIN="$(which kneaddata || true)"
    if [[ -z "${KNEAD_BIN}" ]]; then
        log_error "kneaddata not found"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "kneaddata found"
    
    BOWTIE2_BIN="$(which bowtie2 || true)"
    if [[ -z "${BOWTIE2_BIN}" ]]; then
        log_error "bowtie2 not found"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "bowtie2 found"

    # Test that bowtie2 is executable
    if ! "${BOWTIE2_BIN}" -h >/dev/null 2>&1; then
        log_error "bowtie2 is not executable"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "bowtie2 is executable"
    
    FASTQC_BIN="$(which fastqc || true)"
    if [[ -z "${FASTQC_BIN}" ]]; then
        log_error "fastqc not found in PATH"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "fastqc found"

    # Test that fastqc is executable
    if ! "${FASTQC_BIN}" -h >/dev/null 2>&1; then
        log_error "fastqc is not executable"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "fastqc is executable"

    # Validate that Trimmomatic is installed
    TRIMMOMATIC_DIR=$(ls -d "${CONDA_PREFIX}"/share/trimmomatic/ 2>/dev/null | head -n1)
    if [[ -z "${TRIMMOMATIC_DIR}" ]]; then
        log_error "Trimmomatic directory not found under ${TRIMMOMATIC_DIR}"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "Trimmomatic directory found"

    # Validate that "java -jar TRIMMOMATIC_DIR/trimmomatic" executes
    if ! java -jar "${TRIMMOMATIC_DIR}/trimmomatic.jar" -version >/dev/null 2>&1; then
        log_error "Trimmomatic is not executable"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "Trimmomatic is executable"

    # Validate that kneaddata config.py is pointing to trimmomatic.jar
    if ! grep -q "trimmomatic\.jar" "${CONDA_PREFIX}/lib/python3.9/site-packages/kneaddata/config.py"; then
        log_warn "Kneaddata config.py is not pointing to trimmomatic.jar"
        log_info "Patching Kneaddata config.py to use trimmomatic.jar directly"
        sed -i 's/trimmomatic_jar="trimmomatic\*"/trimmomatic_jar="trimmomatic.jar"/' "${CONDA_PREFIX}/lib/python3.9/site-packages/kneaddata/config.py"
        
        # Test the patch
        if ! grep -q "trimmomatic\.jar" "${CONDA_PREFIX}/lib/python3.9/site-packages/kneaddata/config.py"; then
            log_error "Failed to patch Kneaddata config.py"
            EARLY_EXIT=1
            exit 1
        fi
        log_success "Patched Kneaddata"
    fi
    log_success "Kneaddata config.py is pointing to trimmomatic.jar"


    # Checking for Downstream Tools
    KRAKEN2_BIN="$(which kraken2 || true)"
    if [[ -z "${KRAKEN2_BIN}" ]]; then
        log_error "kraken2 not found"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "kraken2 found"

    BRACKEN_BIN="$(which bracken || true)"
    if [[ -z "${BRACKEN_BIN}" ]]; then
        log_error "bracken not found"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "bracken found"

    METAPHLAN_BIN="$(which metaphlan || true)"
    if [[ -z "${METAPHLAN_BIN}" ]]; then
        log_error "metaphlan not found"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "metaphlan found"

    HUMANN_BIN="$(which humann || true)"
    if [[ -z "${HUMANN_BIN}" ]]; then
        log_error "humann not found"
        EARLY_EXIT=1
        exit 1
    fi
    log_success "humann found"

    log_success "All tools found"
}

# -------------------------------------------------------------- #
# KNEADDATA ON LOCAL DISK
# -------------------------------------------------------------- #
run_kneaddata() {
    log_info "Running kneaddata"

    P1=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_paired_1.fastq")
    P2=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_paired_2.fastq")

    # If paired outputs don't exist in persistent storage, run kneaddata
    log_info "Searching for kneaddata paired outputs in persistent storage..."
    if [[ ! -s "$P1" || ! -s "$P2" ]]; then
        log_info "Paired outputs not found in persistent storage"

        log_info "Staging data..."
        stage_kneaddata_data # Stage the mouse database and the raw files

        kneaddata \
            --input1 "${R1_LOCAL}" \
            --input2 "${R2_LOCAL}" \
            --output "${KD_OUT}" \
            --reference-db "${MOUSE_DB_DST}/" \
            --threads "${THREADS}" \
            --fastqc "${FASTQC_BIN}" \
            --trimmomatic "${TRIMMOMATIC_DIR}" \
            --trimmomatic-options "SLIDINGWINDOW:4:20 MINLEN:50" \
            --run-trim-repetitive \
            --bowtie2-options "--very-sensitive-local --dovetail -p ${THREADS}" \
            --log >(tee "${KD_OUT}/${SAMPLE}.kneaddata.log" >&2) \
            --verbose \
            --max-memory 4000m
        log_success "Kneaddata completed and outputs saved"

        log_info "Running kneaddata_read_count_table"
        kneaddata_read_count_table \
            --input "${KD_OUT}" \
            --output "${KD_OUT}/${SAMPLE}.kneaddata.read_count.tsv"
        log_success "kneaddata_read_count_table completed"

        log_info "Removing intermediate files"
        
        # Fuzzy match any file that has bowtie2 or trimmed in the filename
        rm -f \
            "${KD_OUT}/"*bowtie2*.fastq \
            "${KD_OUT}/"*trimmed*.fastq \
            "${KD_OUT}/"*repeats*.fastq \
            || true
        
        log_success "Intermediate files removed"
    else
        # Outputs already exist; copy them into scratch
        log_info "Kneaddata has already been run on ${SAMPLE}"
    fi
}

# -------------------------------------------------------------- #
# KRAKEN2 CLASSIFICATION
# -------------------------------------------------------------- #
run_taxonomic_classification() {
    # Persistent files
    local PKR="${OUT_PERSIST}/kraken2/${SAMPLE}.kraken2.report"
    local PLB="${OUT_PERSIST}/kraken2/${SAMPLE}.kraken2.labels.tsv"
    local PBR="${OUT_PERSIST}/bracken/${SAMPLE}_species.tsv"
    local PBG="${OUT_PERSIST}/bracken/${SAMPLE}_genus.tsv"

    # Scratch files inputs for Kraken2
    local K1=$(find "${KD_OUT}/" -name "${SAMPLE}_*_kneaddata_paired_1.fastq")
    local K2=$(find "${KD_OUT}/" -name "${SAMPLE}_*_kneaddata_paired_2.fastq")
    local KRAKEN_REPORT="${KR_OUT}/${SAMPLE}.kraken2.report"
    local KRAKEN_LABELS="${KR_OUT}/${SAMPLE}.kraken2.labels.tsv"

    # If the Kraken2 report file does not exist in persistent storage, run Kraken2
    if [[ ! -s "$PKR" && ! -s "$PLB" && "$RUN_KRAKEN2" -eq 1 ]]; then
        if [[ ! -s "$K1" || ! -s "$K2" ]]; then # Check if the kneaddata outputs exist in scratch space
            P1=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_paired_1.fastq")
            P2=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_paired_2.fastq")
            if [[ -s "$P1" && -s "$P2" ]]; then
                log_info "Paired KneadData outputs found in persistent storage"
                log_info "Copying kneaddata paired outputs from persistent storage to scratch space"
                cp -f "$P1" "$KD_OUT/"
                cp -f "$P2" "$KD_OUT/"
                log_success "Kneaddata paired outputs restored from persistent storage"
            else 
                log_error "Kneaddata paired outputs not found in persistent storage"
                log_error "Run this script with the --run-kneaddata | -k flag"
                exit 1
            fi
        fi

        log_info "Kraken2 report file not found in persistent storage"
        log_info "Running Kraken2"

        kraken2 \
            --db "${KRAKEN_DB}" \
            --threads "${THREADS}" \
            --confidence 0.5 \
            --report "${KRAKEN_REPORT}" \
            --output "${KRAKEN_LABELS}" \
            --use-names --paired "$K1" "$K2"
        log_success "Kraken2 classification completed"

        # Check if the output files exist
        if [[ ! -s "${KRAKEN_REPORT}" ]]; then
            log_error "Kraken2 report file does not exist"
            exit 1
        fi
    else 
        log_info "Skipping Kraken2 classification"
    fi

    # If the Bracken output file does not exist in persistent storage, run Bracken
    if [[ ! -s "$PBR" && ! -s "$PBG" && "$RUN_BRACKEN" -eq 1 ]]; then
        log_info "Running Bracken"

        # Check if the Kraken2 report and labels files exist locally. 
        # If not, copy them from persistent storage to scratch space
        if [[ ! -s "$KRAKEN_REPORT" && ! -s "$KRAKEN_LABELS" ]]; then
            if [[ -s "$PKR" && -s "$PLB" ]]; then
                log_info "Kraken2 report file and labels file found in persistent storage"
                log_info "Copying Kraken2 report file and labels file from persistent storage to scratch space"
                cp -f "$PKR" "$KRAKEN_REPORT"
                cp -f "$PLB" "$KRAKEN_LABELS"
                log_success "Kraken2 report file and labels file restored from persistent storage"
            else
                log_error "Kraken2 report file and labels file not found in persistent storage"
                log_error "Run this script with the --run-kraken2 | -r flag"
                exit 1
            fi
        fi

        stage_bracken_db # Stage the Bracken database
        log_info "Running Species-level Bracken"
        bracken \
            -d "${BR_DB_DST}" \
            -i "${KRAKEN_REPORT}" \
            -o "${BR_OUT}/${SAMPLE}_species.tsv" \
            -w "${BR_OUT}/${SAMPLE}_species.outreport" \
            -r 100 \
            -t 10 \
            -l S

        log_info "Running Genus-level Bracken"
        bracken \
            -d "${BR_DB_DST}" \
            -i "${KRAKEN_REPORT}" \
            -o "${BR_OUT}/${SAMPLE}_genus.tsv" \
            -w "${BR_OUT}/${SAMPLE}_genus.outreport" \
            -r 100 \
            -t 10 \
            -l G
        log_success "Bracken completed"
    else
        log_info "Bracken output file found in persistent storage"
    fi
}

# -------------------------------------------------------------- #
# CLEANUP TRAP, ALWAYS PERSIST RESULTS
# -------------------------------------------------------------- #
cleanup() {
    if [[ "$NO_CLEANUP" -eq 1 ]]; then
        return
    fi

    set +e
    log_break 1 "Cleaning up..."

    # Deactivate the conda environment
    conda deactivate
    log_info "Deactivated conda environment"

    # Copy the output files to the persistent and global directories
    if [[ "$EARLY_EXIT" -eq 1 ]]; then
        # Remove the working directory
        log_info "Removing working directory: ${WORK}"
        rm -rf "${WORK}"
        log_success "Removed scratch space"
    else
        # Need to compress the fastq files in the kneaddata output directory
        # Check if the fastq files exist in KD_OUT
        if [[ -s "${KD_OUT}/${SAMPLE}_1_kneaddata_paired_1.fastq" && -s "${KD_OUT}/${SAMPLE}_1_kneaddata_paired_2.fastq" ]]; then
            log_info "Fastq files exist in kneaddata output directory"
        else
            log_error "Fastq files do not exist in kneaddata output directory"
            exit 1
        fi
        
        # Check if Kneaddata outputs exist in persistent storage
        mkdir -p "${OUT_PERSIST}/kneaddata/"
        P1_compressed=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_paired_1.fastq.gz")
        P2_compressed=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_paired_2.fastq.gz")
        U1_compressed=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_unmatched_1.fastq.gz")
        U2_compressed=$(find "${OUT_PERSIST}/kneaddata/" -name "${SAMPLE}_*_kneaddata_unmatched_2.fastq.gz")
        if [[ ! -s "$P1_compressed" || ! -s "$P2_compressed" || ! -s "$U1_compressed" || ! -s "$U2_compressed" ]]; then
            log_info "Copying output files to persistent directory"

            # Compress the fastq files
            P1_uncompressed=$(find "${KD_OUT}/" -name "${SAMPLE}_*_kneaddata_paired_1.fastq")
            P2_uncompressed=$(find "${KD_OUT}/" -name "${SAMPLE}_*_kneaddata_paired_2.fastq")
            U1_uncompressed=$(find "${KD_OUT}/" -name "${SAMPLE}_*_kneaddata_unmatched_1.fastq")
            U2_uncompressed=$(find "${KD_OUT}/" -name "${SAMPLE}_*_kneaddata_unmatched_2.fastq")

            log_info "Compressing fastq files with pigz using ${THREADS} threads"
            pigz -p "${THREADS}" "${P1_uncompressed}"
            pigz -p "${THREADS}" "${P2_uncompressed}"
            pigz -p "${THREADS}" "${U1_uncompressed}"
            pigz -p "${THREADS}" "${U2_uncompressed}"
            log_success "Fastq files compressed"

            # Compress the fastqc directory
            log_info "Compressing fastqc directory"
            tar -czf "${KD_OUT}/${SAMPLE}_fastqc.tar.gz" -C "${KD_OUT}" fastqc
            log_success "Fastqc directory compressed"

            # Remove the fastqc directory
            log_info "Removing fastqc directory"
            rm -rf "${KD_OUT}/fastqc/"
            log_success "Fastqc directory removed"

            log_info "Beginning copy of kneaddata output to persistent directory: ${KD_OUT}/."
            cp -r --no-dereference "${KD_OUT}/." "${OUT_PERSIST}/kneaddata/" || {
                log_error "Failed to copy kneaddata output to persistent directory"
                exit 3
            }
            log_success "Kneaddata outputs copied to persistent directory"
        fi

        # Check if Kraken2 outputs exist in persistent storage
        KRAKEN_REPORT="${OUT_PERSIST}/kraken2/${SAMPLE}.kraken2.report"
        KRAKEN_LABELS="${OUT_PERSIST}/kraken2/${SAMPLE}.kraken2.labels.tsv"
        if [[ ! -s "$KRAKEN_REPORT" || ! -s "$KRAKEN_LABELS" ]]; then
            log_info "Copying output files to persistent directory"
            mkdir -p "${OUT_PERSIST}/kraken2/"
            log_info "Beginning copy of kraken2 output to persistent directory: ${KR_OUT}/."
            cp -r --no-dereference "${KR_OUT}/." "${OUT_PERSIST}/kraken2/" || {
                log_error "Failed to copy kraken2 output to persistent directory"
                exit 3
            }
            log_success "Kraken2 outputs copied to persistent directory"
        fi

        # Check if Bracken outputs exist in persistent storage
        BRACKEN_OUT="${OUT_PERSIST}/bracken/${SAMPLE}.bracken.tsv"
        if [[ ! -s "$BRACKEN_OUT" ]]; then
            log_info "Copying output files to persistent directory"
            mkdir -p "${OUT_PERSIST}/bracken/"
            log_info "Beginning copy of bracken output to persistent directory: ${BR_OUT}/."
            cp -r --no-dereference "${BR_OUT}/." "${OUT_PERSIST}/bracken/" || {
                log_error "Failed to copy bracken output to persistent directory"
                exit 3
            }
            log_success "Bracken outputs copied to persistent directory"
        fi

        qstat -j "$JOB_ID" &>"${OUT_PERSIST}/qstat_${JOB_ID}_${TASK_INDEX}.txt" || {
            log_error "Failed to copy qstat output to persistent directory"
            exit 3
        }
        log_success "Copied output files to persistent directory"

        # Remove the working directory
        log_info "Removing working directory: ${WORK}"
        rm -rf "${WORK}"
        log_success "Removed scratch space"

        if [[ "$TRANSFER_TO_BOX" -eq 1 ]]; then
            transfer_to_box
        fi
    fi

    # TOTAL TIME IN hr:min:sec
    END_TIME=$(date +%s)
    TOTAL_TIME=$(date -u -d "@$(( $END_TIME - $START_TIME ))" +%H:%M:%S)
    log_break 1 "TIME COMPLETED"
    log_success "Finished processing ${SAMPLE} in ${TOTAL_TIME}"

    # Move the log file to the correct subdirectory
    mkdir -p "${HOME}/logs/${STUDY_NAME}/"
    mv "${HOME}/logs/${STUDY_NAME}/metagenomics_${JOB_ID}_${TASK_INDEX}.log" "${HOME}/logs/${STUDY_NAME}/${SAMPLE}_meta.log" > /dev/null 2>&1
}

transfer_to_box() {
    log_break 1 "Transferring output files to Box"
    
    # Check if the transfer_to_box.sh script exists
    log_info "Checking if wyntonBoxTransfer.sh script exists"
    if [[ ! -f "${HOME}/utils/wyntonBoxTransfer.sh" ]]; then
        log_error "transfer_to_box.sh script not found"
        exit 1
    fi
    log_success "wyntonBoxTransfer.sh script found"

    # Check if the netrc file exists
    if [[ ! -f "${HOME}/.netrc" ]]; then
        log_error "netrc file not found"
        exit 1
    fi
    log_success "netrc file found"

    # Check if the output directory in persistent storage exists
    if [[ ! -d "${OUT_PERSIST}" ]]; then
        log_error "output directory not found in persistent storage"
        exit 1
    fi
    log_success "output directory found in persistent storage"

    # Check if the ssh key exists
    if [[ ! -f "${HOME}/.ssh/wynton-dtn-key" ]]; then
        log_error "ssh key not found"
        exit 1
    fi
    log_success "ssh key found"

    log_info "Invoking ssh to the data transfer node"

    # Invoke ssh to the data transfer node and run the transfer_to_box.sh script
    # Suppress the output of the ssh command
    LOG_DIR="${HOME}/logs/${STUDY_NAME}"
    OUTPUT_LOG="${LOG_DIR}/${SAMPLE}_transfer.log"
    mkdir -p "${LOG_DIR}"
    ssh -i ~/.ssh/wynton-dtn-key \
        dt1.wynton.ucsf.edu \
        "${HOME}/utils/wyntonBoxTransfer.sh ${OUT_PERSIST} ${BOX_TRANSFER_DIR}/${SAMPLE}/" > "${OUTPUT_LOG}" 2>&1

    log_success "Transferred output files to Box"
}

on_interrupt() {
    log_warn "Interrupt signal received"
    EARLY_EXIT=1
    exit 1
}

trap on_interrupt INT
trap cleanup EXIT

# -------------------------------------------------------------- #
# ARGUMENT HANDLING
# -------------------------------------------------------------- #
usage() {
    # SET NO_CLEANUP TO 1
    NO_CLEANUP=1

    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h | --help             Display this help message"
    echo "  -k | --run-kneaddata    Run kneaddata"
    echo "  -r | --run-kraken2      Run kraken2"
    echo "  -b | --run-bracken      Run bracken"
    echo "  -a | --all              Run all steps"
    echo "  -m | --manifest         Path to the manifest file"
    echo "  -t | --transfer-to-box  Transfer output files to Box"
    echo "  -d | --box-dir          Transfer output files to a specific directory in Box"
    echo "  -s | --save-to-scratch  Save output files to scratch. By default, output files" 
    echo "                          are saved to persistent storage."
    echo "  -o | --output-base-dir  Path to the output base directory"
    echo "  -n | --study-name       Name of the study. This will be used to create the" 
    echo "                          output directory"
    exit 0
}

handle_args() {
    # Set run flags to 0
    RUN_KNEADDATA=0
    RUN_KRAKEN2=0
    RUN_BRACKEN=0
    TRANSFER_TO_BOX=0
    SAVE_TO_SCRATCH=0
    NO_CLEANUP=0
    BOX_TRANSFER_DIR="" # By default, transfer to the root of the Box account
    MANIFEST="${HOME}/metagenomics/manifest.tsv"
    OUT_BASE_DIR="metagenomics/out"
    STUDY_NAME="metagenomics"
    
    # Pre-parse simple long flags
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --run-kneaddata)    RUN_KNEADDATA=1; shift ;;
        --run-kraken2)      RUN_KRAKEN2=1;   shift ;;
        --run-bracken)      RUN_BRACKEN=1;   shift ;;
        --transfer-to-box)  TRANSFER_TO_BOX=1; shift ;;
        --output-base-dir)  OUT_PERSIST="$2"; shift; shift ;;
        --box-dir)          BOX_TRANSFER_DIR="$2"; shift; shift ;;
        --save-to-scratch)  SAVE_TO_SCRATCH=1; shift ;;
        --all)              RUN_KNEADDATA=1; RUN_KRAKEN2=1; RUN_BRACKEN=1; shift ;;
        --manifest)         MANIFEST="$2"; shift; shift ;;
        --study-name)       STUDY_NAME="$2"; shift; shift ;;
        --help)             usage; exit 0 ;;
        --)                 shift; break ;;
        -*)                 break ;;
        *)                  break ;;
        esac
    done

    OPTIND=1
    while getopts "krbatshd:o:m:n:" opt; do
        case "$opt" in
            k)  RUN_KNEADDATA=1 ;;
            r)  RUN_KRAKEN2=1 ;;
            b)  RUN_BRACKEN=1 ;;
            a)  RUN_KNEADDATA=1; RUN_KRAKEN2=1; RUN_BRACKEN=1 ;;
            t)  TRANSFER_TO_BOX=1 ;;
            d)  BOX_TRANSFER_DIR="$OPTARG" ;;
            s)  SAVE_TO_SCRATCH=1 ;;
            m)  MANIFEST="$OPTARG" ;;
            o)  OUT_BASE_DIR="$OPTARG" ;;
            n)  STUDY_NAME="$OPTARG" ;;
            h)  usage; exit 0 ;;
            \?) log_error "Unknown option: -$OPTARG"; usage; exit 2 ;;
            :)  log_error "Option -$OPTARG requires an argument"; usage; exit 2 ;;
        esac
    done
    shift $((OPTIND - 1))

    # Strip the beginning and ending slashes from the output base directory
    OUT_BASE_DIR=$(echo "${OUT_BASE_DIR}" | sed 's:^/*::; s:/*$::')
}


# -------------------------------------------------------------- #
# MAIN
# -------------------------------------------------------------- #
main() {
    # SAVE START TIME
    START_TIME=$(date +%s)

    # Handle arguments
    handle_args "$@"

    # Prepare the run environment
    prepare_run_env

    # Load the manifest
    load_manifest

    # Check that the tools are installed
    check_tools
    
    log_break 1 "Processing ${SAMPLE}"
    if [[ "$RUN_KNEADDATA" -eq 1 ]]; then
        run_kneaddata
    else
        log_info "Skipping kneaddata"
    fi

    if [[ "$RUN_KRAKEN2" -eq 1 || "$RUN_BRACKEN" -eq 1 ]]; then
        log_info "Running taxonomic classification"
        run_taxonomic_classification
    fi
}

main "$@"
