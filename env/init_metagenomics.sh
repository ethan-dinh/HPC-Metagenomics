#!/usr/bin/env bash
# init_metagenomics.sh
# Create or update a Conda env for metagenomics tools, then enable conda-stage.
# Works on Wynton with CBI modules. Safe to re-run.

set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPTS_DIR="$SCRIPT_DIR"

# ----------------------------- Colors and helpers -----------------------------
safe_conda_activate() {
    set +u
    conda activate "$1"
    local rc=$?
    set -u
    return $rc
}
safe_conda_deactivate() {
    set +u
    conda deactivate
    local rc=$?
    set -u
    return $rc
}

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
info() { echo -e "${BLUE}[INFO]${NC} $*"; }
ok() { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err() { echo -e "${RED}[ERR ]${NC} $*" >&2; }

have() { command -v "$1" >/dev/null 2>&1; }

usage() {
    cat <<'USAGE'
Usage: ./init_metagenomics.sh [options]

Options:
  -e NAME         Conda env name. Default: metagenomics
  -f              Force recreate env if it exists
  -u              Update env from YAML if it exists
  -r              Restage env
  -y              Non-interactive yes to all
  -yml PATH       Environment YAML path. Default: ./metagenomics.yml
  -q              Quiet conda solver output
  -h              Show help

Behavior:
- Loads Wynton modules: CBI miniforge3 and CBI conda-stage
- Ensures conda-forge, bioconda, defaults channels
- Creates or updates env from YAML. If YAML is missing, installs a pinned tool set
- Enables automatic staging: `conda stage --auto-stage=enable ENV`
- Verifies activation and basic tool availability

USAGE
}

# ----------------------------- Defaults -----------------------------
ENV_NAME="metagenomics"
YAML_PATH="./metagenomics.yml"
YES_FLAG=""
QUIET_FLAG=""
DO_FORCE=0
DO_UPDATE=0
RESTAGE=0

# ----------------------------- Parse args -----------------------------
while (("$#")); do
    case "$1" in
    -e)
        ENV_NAME="$2"
        shift 2
        ;;
    -yml)
        YAML_PATH="$2"
        shift 2
        ;;
    -f)
        DO_FORCE=1
        shift
        ;;
    -u)
        DO_UPDATE=1
        shift
        ;;
    -y)
        YES_FLAG="-y"
        shift
        ;;
    -q)
        QUIET_FLAG="-q"
        shift
        ;;
    -r)
        RESTAGE=1
        shift
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        err "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
done

# ----------------------------- Wynton modules -----------------------------
info "Loading Wynton modules"
module load CBI miniforge3 conda-stage

# Ensure conda shell hooks are available
# shellcheck disable=SC1091
source "$(conda info --base)/etc/profile.d/conda.sh"

# ----------------------------- Channels -----------------------------
info "Ensuring conda channels"
conda config --add channels conda-forge >/dev/null 2>&1 || true
conda config --add channels bioconda >/dev/null 2>&1 || true
conda config --add channels defaults >/dev/null 2>&1 || true

# ----------------------------- Env create/update -----------------------------
env_exists() { conda env list | awk '{print $1}' | grep -Fxq "$ENV_NAME"; }

create_from_yaml() {
    info "Creating env '$ENV_NAME' from $YAML_PATH"
    conda env create $YES_FLAG $QUIET_FLAG -f "$YAML_PATH"
}

create_fallback() {
    info "YAML not found. Creating '$ENV_NAME' with a pinned tool set"
    conda create $YES_FLAG $QUIET_FLAG -n "$ENV_NAME" \
        python=3.9 \
        kneaddata=0.12.3 \
        bowtie2=2.5.1 \
        trimmomatic=0.39 \
        fastqc=0.12.1 \
        kraken2=2.1.3 \
        bracken=2.9 \
        metaphlan=4.0.6 \
        humann=3.8 \
        diamond=2.1.10 \
        pigz=2.8 \
        samtools=1.18 \
        parallel=20240722 \
        seqkit=2.6.1
}

update_from_yaml() {
    info "Updating env '$ENV_NAME' from $YAML_PATH"
    conda env update $QUIET_FLAG -n "$ENV_NAME" -f "$YAML_PATH"
}

if ((DO_FORCE)) && env_exists; then
    warn "Force flag set. Removing existing env '$ENV_NAME'"
    conda env remove -n "$ENV_NAME"
fi

if env_exists; then
    ok "Env '$ENV_NAME' already exists"
    if ((DO_UPDATE)); then
        [[ -f "$YAML_PATH" ]] && update_from_yaml || warn "YAML missing. Skipping update."
    else
        info "Skipping creation. Use -u to update or -f to recreate."
    fi
else
    if [[ -f "$YAML_PATH" ]]; then
        create_from_yaml
    else
        create_fallback
    fi
fi

# ----------------------------- Verify activation and tools -----------------------------
info "Disable staging temporarily"
conda stage --auto-stage=disable "$ENV_NAME" || {
    err "conda-stage disable failed. Verify module load CBI conda-stage"
    exit 1
}
ok "conda-stage disabled"

info "Activating env to verify staging and tools"
safe_conda_activate "$ENV_NAME"
trap 'safe_conda_deactivate || true' EXIT

# Show where binaries come from, expect /scratch/... after staging
KNEAD_PATH="$(command -v kneaddata || true)"
info "kneaddata resolved to: ${KNEAD_PATH:-not found}"

# Minimal tool sanity
need_tools=(kneaddata bowtie2 trimmomatic fastqc kraken2 bracken metaphlan humann)
for t in "${need_tools[@]}"; do
    if ! have "$t"; then
        err "Missing tool: $t"
        exit 1
    fi
done

ok "All requested tools are on PATH"
ok "Environment '$ENV_NAME' is ready and configured for conda-stage."


# ----------------------------- Patch for Trimmomatic -----------------------------
info "Patching KneadData config to use trimmomatic.jar directly"

CONFIG_FILE="$CONDA_PREFIX/lib/python3.9/site-packages/kneaddata/config.py"
if [[ -f "$CONFIG_FILE" ]]; then
    # Replace the default globbing with an explicit .jar filename
    sed -i 's/trimmomatic_jar="trimmomatic\*"/trimmomatic_jar="trimmomatic.jar"/' "$CONFIG_FILE"
    ok "Patched $CONFIG_FILE to force use of trimmomatic.jar"
else
    warn "Could not locate kneaddata/config.py under $ENV_PREFIX"
fi

# ----------------------------- Enable conda-stage -----------------------------
if ((RESTAGE)); then
    conda stage --repack --writable "$ENV_NAME" || {
        err "conda-stage repack failed. Verify module load CBI conda-stage"
        exit 1
    }
    ok "conda-stage repacked"
fi

info "Enabling automatic staging for '$ENV_NAME'"
conda stage --writable --auto-stage=enable "$ENV_NAME" || {
    err "conda-stage enable failed. Verify module load CBI conda-stage"
    exit 1
}
ok "conda-stage enabled and env is packed for job submission"