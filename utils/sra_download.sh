#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -N sra_download
#$ -pe smp 8
#$ -l mem_free=4G
#$ -l scratch=200G
#$ -l h_rt=24:00:00
#$ -j y
#$ -o $HOME/logs/sra_download_$JOB_ID.log

# Downloads paired-end FASTQ files from NCBI SRA given a list of accessions,
# then writes a manifest TSV ready for run_metagenomics.sh.
#
# Usage (interactive):
#   bash sra_download.sh --accessions accessions.txt [OPTIONS]
#
# Usage (array job):
#   qsub -t 1-N sra_download.sh --accessions accessions.txt [OPTIONS]
#   Each task downloads one accession (SGE_TASK_ID selects the line).
#
# Accession file format: one SRA accession per line, no header.
#   SRR12345678
#   SRR12345679

# -------------------------------------------------------------- #
# LOGGING
# -------------------------------------------------------------- #
log() {
    local level="$1"; shift
    local msg="$*"
    local ts
    ts=$(date '+%m-%d %I:%M:%S %p')
    local color reset="\033[0m"
    local level_width=7
    local pad
    pad=$((level_width - ${#level}))
    [[ $pad -lt 0 ]] && pad=0
    case "$level" in
        INFO)    color="\033[34m" ;;
        WARN)    color="\033[33m" ;;
        ERROR)   color="\033[31m" ;;
        FATAL)   color="\033[35m" ;;
        SUCCESS) color="\033[32m" ;;
    esac
    if [[ "$level" =~ (ERROR|FATAL) ]]; then
        printf "%b[%s]%*s%b [%s] - %s\n" "$color" "$level" "$pad" "" "$reset" "$ts" "$msg" >&2
    else
        printf "%b[%s]%*s%b [%s] - %s\n" "$color" "$level" "$pad" "" "$reset" "$ts" "$msg"
    fi
    [[ "$level" == "FATAL" ]] && exit 1
}
log_info()    { log INFO    "$*"; }
log_warn()    { log WARN    "$*"; }
log_error()   { log ERROR   "$*"; }
log_fatal()   { log FATAL   "$*"; }
log_success() { log SUCCESS "$*"; }

# -------------------------------------------------------------- #
# DEFAULTS
# -------------------------------------------------------------- #
ACCESSIONS_FILE=""
OUT_DIR="/wynton/scratch/$USER/metag/raw"
MANIFEST_OUT="$HOME/metagenomics/manifest.tsv"
THREADS="${NSLOTS:-8}"
MAX_RETRIES=3

# -------------------------------------------------------------- #
# USAGE
# -------------------------------------------------------------- #
usage() {
    echo ""
    echo "Usage: $0 --accessions FILE [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  -a | --accessions FILE   Path to file with one SRA accession per line"
    echo ""
    echo "Options:"
    echo "  -o | --outdir DIR        Download directory (default: /wynton/scratch/\$USER/metag/raw)"
    echo "  -m | --manifest FILE     Manifest output path (default: ~/metagenomics/manifest.tsv)"
    echo "  -t | --threads N         Threads for fasterq-dump (default: \$NSLOTS or 8)"
    echo "  -h | --help              Show this message"
    echo ""
    echo "Examples:"
    echo "  # Download all accessions sequentially"
    echo "  bash sra_download.sh -a accessions.txt"
    echo ""
    echo "  # Submit as an SGE array job (one task per accession)"
    echo "  N=\$(wc -l < accessions.txt)"
    echo "  qsub -t 1-\${N} utils/sra_download.sh -a accessions.txt"
    exit 0
}

# -------------------------------------------------------------- #
# ARGUMENT PARSING
# -------------------------------------------------------------- #
handle_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -a|--accessions) ACCESSIONS_FILE="$2"; shift 2 ;;
            -o|--outdir)     OUT_DIR="$2";          shift 2 ;;
            -m|--manifest)   MANIFEST_OUT="$2";     shift 2 ;;
            -t|--threads)    THREADS="$2";           shift 2 ;;
            -h|--help)       usage ;;
            *) log_fatal "Unknown option: $1" ;;
        esac
    done

    if [[ -z "$ACCESSIONS_FILE" ]]; then
        log_error "Missing required --accessions argument"
        usage
    fi

    if [[ ! -r "$ACCESSIONS_FILE" ]]; then
        log_fatal "Accessions file not found or not readable: $ACCESSIONS_FILE"
    fi

    local total
    total=$(grep -c . "$ACCESSIONS_FILE" || true)
    if (( total == 0 )); then
        log_fatal "Accessions file is empty: $ACCESSIONS_FILE"
    fi
}

# -------------------------------------------------------------- #
# SELECT ACCESSION
# Returns the accession for this task in ACC and total count in TOTAL_ACCS
# -------------------------------------------------------------- #
select_accession() {
    TOTAL_ACCS=$(grep -c . "$ACCESSIONS_FILE")

    # Array job: SGE_TASK_ID selects the line; otherwise download all
    if [[ -n "${SGE_TASK_ID:-}" && "${SGE_TASK_ID}" != "undefined" ]]; then
        TASK_INDEX="${SGE_TASK_ID}"
        if (( TASK_INDEX < 1 || TASK_INDEX > TOTAL_ACCS )); then
            log_fatal "Task index ${TASK_INDEX} out of range (1–${TOTAL_ACCS})"
        fi
        ACC=$(sed -n "${TASK_INDEX}p" "$ACCESSIONS_FILE" | tr -d '[:space:]')
        ACCESSIONS=("$ACC")
    else
        mapfile -t ACCESSIONS < <(grep -v '^[[:space:]]*$' "$ACCESSIONS_FILE" | tr -d '[:space:]')
    fi
}

# -------------------------------------------------------------- #
# DOWNLOAD ONE ACCESSION
# -------------------------------------------------------------- #
download_accession() {
    local acc="$1"

    local r1="${OUT_DIR}/${acc}_1.fastq.gz"
    local r2="${OUT_DIR}/${acc}_2.fastq.gz"

    if [[ -s "$r1" && -s "$r2" ]]; then
        log_info "[$acc] Already downloaded — skipping"
        return 0
    fi

    log_info "[$acc] Starting download (attempt 1 of ${MAX_RETRIES})"

    local attempt=1
    while (( attempt <= MAX_RETRIES )); do
        if fasterq-dump \
                --split-files \
                --threads "${THREADS}" \
                --outdir "${OUT_DIR}" \
                --temp "${OUT_DIR}/.tmp_${acc}" \
                --progress \
                "$acc"; then
            break
        fi
        log_warn "[$acc] fasterq-dump failed (attempt ${attempt}/${MAX_RETRIES})"
        (( attempt++ ))
        if (( attempt <= MAX_RETRIES )); then
            sleep 30
        else
            log_error "[$acc] Download failed after ${MAX_RETRIES} attempts — skipping"
            return 1
        fi
    done

    # Validate that paired files were produced
    local raw1="${OUT_DIR}/${acc}_1.fastq"
    local raw2="${OUT_DIR}/${acc}_2.fastq"
    if [[ ! -s "$raw1" || ! -s "$raw2" ]]; then
        log_error "[$acc] Expected paired files not found after download:"
        log_error "  ${raw1}"
        log_error "  ${raw2}"
        log_error "  Verify the accession is paired-end."
        return 1
    fi

    log_info "[$acc] Compressing with pigz (${THREADS} threads)..."
    pigz -p "${THREADS}" "$raw1" "$raw2" || {
        log_error "[$acc] Compression failed"
        return 1
    }

    log_success "[$acc] Done → ${r1}"
    log_success "[$acc] Done → ${r2}"
}

# -------------------------------------------------------------- #
# BUILD MANIFEST
# Appends any newly downloaded pairs to the manifest.
# Safe to call multiple times; will not duplicate existing entries.
# -------------------------------------------------------------- #
build_manifest() {
    mkdir -p "$(dirname "$MANIFEST_OUT")"

    # Write header if the file doesn't exist yet
    if [[ ! -f "$MANIFEST_OUT" ]]; then
        echo -e "sample_id\tR1\tR2" > "$MANIFEST_OUT"
        log_info "Created manifest: $MANIFEST_OUT"
    fi

    local added=0
    for acc in "${ACCESSIONS[@]}"; do
        local r1="${OUT_DIR}/${acc}_1.fastq.gz"
        local r2="${OUT_DIR}/${acc}_2.fastq.gz"

        if [[ ! -s "$r1" || ! -s "$r2" ]]; then
            log_warn "[$acc] Skipping manifest entry — files not found"
            continue
        fi

        # Skip if already in the manifest
        if grep -qF "$acc" "$MANIFEST_OUT" 2>/dev/null; then
            log_info "[$acc] Already in manifest — skipping"
            continue
        fi

        echo -e "${acc}\t${r1}\t${r2}" >> "$MANIFEST_OUT"
        (( added++ ))
    done

    log_success "Manifest updated: ${added} sample(s) added → $MANIFEST_OUT"
}

# -------------------------------------------------------------- #
# MAIN
# -------------------------------------------------------------- #
main() {
    handle_args "$@"

    log_info "Loading modules and activating conda environment..."
    module load CBI miniforge3
    conda activate metagenomics

    mkdir -p "$OUT_DIR"
    log_info "Output directory: $OUT_DIR"
    log_info "Threads: $THREADS"

    select_accession

    log_info "Accessions to download: ${#ACCESSIONS[@]}"
    for acc in "${ACCESSIONS[@]}"; do
        download_accession "$acc"
    done

    build_manifest

    log_success "All downloads complete. Manifest: $MANIFEST_OUT"
}

main "$@"
