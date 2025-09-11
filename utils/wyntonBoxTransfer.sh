#!/bin/bash

# INFO
# This script is used to transfer files from Wynton to UCSF Box.
# It will use lftp to transfer the files. It requires a ~/.netrc file 
# with the following content:
#   machine ftp.box.com
#       login $BOX_USERNAME
#       password $BOX_PASSWORD
#
# It will use the following positional arguments:
# INPUT_DIR - The directory on Wynton to transfer the files from
# DEST_DIR - The directory on UCSF Box to transfer the files to
#
# It will use the following options:
# -h, --help - Show this help message and exit
# -p, --parallel - The number of parallel transfers to use

# -------------------------------------------------- #
# GLOBAL VARIABLES
# -------------------------------------------------- #
INPUT_DIR=""
DEST_DIR=""
PARALLEL=4

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

log_info()  { log INFO  "$*"; }
log_warn()  { log WARN  "$*"; }
log_error() { log ERROR "$*"; }
log_fatal() { log FATAL "$*"; }
log_success() { log SUCCESS "$*"; }

# -------------------------------------------------- #
# SHOW USAGE
# -------------------------------------------------- #
show_usage() {
    echo -e "${USAGE_COLOR}[Usage]${NC}: $(basename "$0") [OPTIONS] <INPUT_DIR> <DEST_DIR>"
    echo
    echo "Transfer files from Wynton to UCSF Box"
    echo
    echo "Positional arguments:"
    echo "  INPUT_DIR - The directory on Wynton to transfer the files from"
    echo "  DEST_DIR - The directory on UCSF Box to transfer the files to"
    echo
    echo "Options:"
    echo "  -h, --help - Show this help message and exit"
    echo "  -p, --parallel - The number of parallel transfers to use"
}

# -------------------------------------------------- #
# PROCESS ARGUMENTS
# -------------------------------------------------- #
process_arguments() {
    local positional=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                show_usage
                exit 0
                ;;
            -p|--parallel)
                if [[ -n "$2" && "$2" != -* ]]; then
                    PARALLEL="$2"
                    shift 2
                else
                    log_error "Missing value for --parallel"
                    show_usage
                    exit 1
                fi
                ;;
            -*)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                positional+=("$1")
                shift
                ;;
        esac
    done

    if [[ ${#positional[@]} -lt 2 ]]; then
        log_error "Missing required positional arguments: INPUT_DIR and DEST_DIR"
        show_usage
        exit 1
    elif [[ ${#positional[@]} -gt 2 ]]; then
        log_error "Too many positional arguments"
        show_usage
        exit 1
    fi

    INPUT_DIR="${positional[0]}"
    DEST_DIR="${positional[1]}"
}


# -------------------------------------------------- #
# CHECK REQUIRED FILES
# -------------------------------------------------- #
check_required_files() {
    if [ ! -f "$HOME/.netrc" ] || [ ! -r "$HOME/.netrc" ]; then
        log_error "~/.netrc file not found or not readable"
        exit 1
    fi

    if [ ! -d "$INPUT_DIR" ] || [ ! -r "$INPUT_DIR" ]; then
        log_error "INPUT_DIR does not exist or is not readable"
        exit 1
    fi
}

# -------------------------------------------------- #
# TRANSFER FILES
# -------------------------------------------------- #
transfer_files() {
    log_info "Starting lftp transfer from Wynton to UCSF Box"
    log_info "INPUT_DIR: $INPUT_DIR"
    log_info "DEST_DIR: $DEST_DIR"

    MAX_RETRIES=5
    COUNT=0
    while [ $COUNT -lt $MAX_RETRIES ]; do
        if lftp -e "
            open ftp.box.com;
            mkdir -p $(printf '%q' "$DEST_DIR");
            mkdir -p $(printf '%q' "$DEST_DIR/kneaddata");
            mkdir -p $(printf '%q' "$DEST_DIR/kraken2");
            mkdir -p $(printf '%q' "$DEST_DIR/bracken");

            echo "Waiting for 5 seconds..."
            sleep 5; # Wait for the directory to be created

            mirror -R --verbose --continue \
                --only-newer \
                --use-pget-n=4 \
                --no-perms \
                --no-umask \
                --dereference \
                --parallel=$PARALLEL \
                $(printf '%q' "$INPUT_DIR/") $(printf '%q' "$DEST_DIR");
            bye;
        "; then
            log_success "Transfer completed"
            break
        else
            log_warn "Transfer failed, retrying in 10s..."
            sleep 10
            COUNT=$((COUNT+1))
        fi
    done

    if [ $COUNT -eq $MAX_RETRIES ]; then
        log_error "Transfer failed after $MAX_RETRIES retries"
        exit 1
    fi
    log_success "Transfer completed after $COUNT retries"
}

# -------------------------------------------------- #
# MAIN FUNCTION
# -------------------------------------------------- #
main() {
    process_arguments "$@"
    check_required_files
    transfer_files
}

main "$@"