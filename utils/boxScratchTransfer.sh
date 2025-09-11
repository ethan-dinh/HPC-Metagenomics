#!/bin/bash

# -------------------------------------------------- #
# COLOR DEFINITIONS
# -------------------------------------------------- #
INFO_COLOR="\033[1;34m"
ERROR_COLOR="\033[1;31m"
SUCCESS_COLOR="\033[1;32m"
WARNING_COLOR="\033[1;33m"
USAGE_COLOR="\033[1;35m"
NC="\033[0m"

# -------------------------------------------------- #
# GLOBAL VARIABLES
# -------------------------------------------------- #
INPUT_DIR=""

# -------------------------------------------------- #
# HELPER FUNCTIONS
# -------------------------------------------------- #
log_info() {
    if [ "$1" == "ERROR" ]; then
        echo -e "${ERROR_COLOR}[ERROR]${NC} $2"
    elif [ "$1" == "INFO" ]; then
        echo -e "${INFO_COLOR}[INFO]${NC} $2"
    elif [ "$1" == "SUCCESS" ]; then
        echo -e "${SUCCESS_COLOR}[SUCCESS]${NC} $2"
    elif [ "$1" == "WARNING" ]; then
        echo -e "${WARNING_COLOR}[WARNING]${NC} $2"
    elif [ "$1" == "USAGE" ]; then
        echo -e "${USAGE_COLOR}[USAGE]${NC} $2"
    fi
}

show_usage() {
    echo -e "${USAGE_COLOR}[Usage]${NC}: $(basename "$0") [OPTIONS] <BOX_INPUT_DIR>"
    echo
    echo "Mirror a folder from UCSF Box (via FTPS) to global scratch on Wynton, to:"
    echo "  /wynton/scratch/\$USER/metag/raw"
    echo "and generate a manifest.tsv in the current working directory."
    echo
    echo "Positional arguments:"
    echo "  BOX_INPUT_DIR      Remote Box path to mirror. Example: /metagenomics_inbox"
    echo
    echo "Options:"
    echo "  -h, --help         Show this help message and exit"
}

process_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                show_usage
                exit 0
                ;;
            -*)
                log_info "USAGE" "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                if [[ -z "$INPUT_DIR" ]]; then
                    INPUT_DIR="$1"
                else
                    log_info "USAGE" "Multiple input directories specified"
                    show_usage
                    exit 1
                fi
                ;;
        esac
        shift
    done

    if [[ -z "$INPUT_DIR" ]]; then
        log_info "ERROR" "Missing required BOX_INPUT_DIR"
        show_usage
        exit 1
    fi
}

# -------------------------------------------------- #
# CREATE MANIFEST
# -------------------------------------------------- #
create_manifest() {
    local dest="$1"
    local manifest="$PWD/manifest.tsv"

    log_info "INFO" "Creating manifest: $manifest"

    echo -e "sample_id\tR1\tR2" > "$manifest"

    # Find R1 files, look for matching R2
    find "$(printf '%q' "$dest")" -type f -name "*R1*.fastq.gz" | sort | while read -r r1; do
        sample=$(basename "$r1" | sed -E 's/_R1.*//')
        r2="${r1/R1/R2}"
        if [[ -f "$r2" ]]; then
            echo -e "${sample}\t${r1}\t${r2}" >> "$manifest"
        else
            log_info "WARNING" "No R2 pair found for $r1"
        fi
    done

    log_info "SUCCESS" "Manifest created at $manifest"
}

# -------------------------------------------------- #
# MAIN FUNCTIONS
# -------------------------------------------------- #
main() {
    process_arguments "$@"

    if [[ "$(hostname -s)" != dt1* ]]; then
        log_info "ERROR" "This script must be run on dt1"
        exit 1
    fi

    DEST="/wynton/scratch/$USER/metag/raw"

    if [ ! -d "$DEST" ]; then
        log_info "INFO" "Creating destination directory: $DEST"
        mkdir -p "$DEST" || {
            log_info "ERROR" "Failed to create $DEST"
            exit 1
        }
    fi

    if [ ! -f "$HOME/.netrc" ] || [ ! -r "$HOME/.netrc" ]; then
        log_info "ERROR" "~/.netrc file not found or not readable"
        exit 1
    fi

    log_info "INFO" "Starting lftp transfer from BOX: $INPUT_DIR to Wynton: $DEST"
    if lftp -e "
        open ftp.box.com;
        cd $(printf '%q' "$INPUT_DIR");
        mirror --verbose --continue --parallel=4 \
            --only-newer \
            --use-pget-n=0 \
            --no-perms \
            --no-umask \
            --dereference \
            --no-empty-dirs \
            . $(printf '%q' "$DEST");
        bye;"; then
        log_info "SUCCESS" "lftp transfer completed"
        create_manifest "$(printf '%q' "$DEST")"
        exit 0
    else
        log_info "ERROR" "lftp transfer failed"
        exit 1
    fi
}

main "$@"
