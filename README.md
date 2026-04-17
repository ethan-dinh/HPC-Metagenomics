# HPC Metagenomics Pipeline

> Reproducible shotgun metagenomics on SGE clusters — host depletion, taxonomic classification, and functional profiling from raw paired-end reads.

Tested on **UCSF Wynton HPC**. Data published to NCBI SRA.

---

## Pipeline

```
Raw FASTQ / SRA accession
         │
         ▼
   KneadData 0.12.3    ──  Quality trimming (Trimmomatic) + host depletion (Bowtie2/GRCm39)
         │
         ├──────────►  Kraken2 2.1.3     ──  k-mer taxonomic classification
         │                   │
         │                   ▼
         │             Bracken 2.9        ──  Species & genus abundance re-estimation
         │
         └──────────►  HUMAnN 3.8         ──  Functional profiling via MetaPhlAn 4 + UniRef90
```

| Tool | Version | Role |
|------|---------|------|
| KneadData | 0.12.3 | QC trimming + mouse host depletion |
| Bowtie2 | 2.5.1 | Host alignment |
| Trimmomatic | 0.39 | Adapter/quality trimming |
| FastQC | 0.12.1 | Pre/post QC reports |
| Kraken2 | 2.1.3 | Taxonomic classification |
| Bracken | 2.9 | Abundance re-estimation |
| MetaPhlAn | 4.0.6 | Marker-based taxonomy (HUMAnN input) |
| HUMAnN | 3.8 | Functional profiling |
| DIAMOND | 2.1.10 | Protein alignment (HUMAnN) |
| pigz | 2.8 | Parallel gzip |
| SRA Tools | 3.1.1 | SRA download (`fasterq-dump`) |
| Entrez Direct | 22.4 | NCBI metadata queries (`efetch`, `esearch`) |

---

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Database Setup](#database-setup)
- [Preparing Input](#preparing-input)
  - [From raw FASTQ files](#from-raw-fastq-files)
  - [From an SRA accession](#from-an-sra-accession)
- [Running the Pipeline](#running-the-pipeline)
  - [Single sample (dev node)](#single-sample-dev-node)
  - [Array job (compute nodes)](#array-job-compute-nodes)
- [Flags Reference](#flags-reference)
- [Output Structure](#output-structure)
- [Box Transfer Setup](#box-transfer-setup)
- [Wynton Notes](#wynton-notes)
- [Troubleshooting](#troubleshooting)

---

## Requirements

- UCSF Wynton (or any SGE cluster with `module load CBI miniforge3`)
- ~250 GB node-local scratch per sample
- ~100 GB persistent storage for reference databases
- UCSF Box account with an app password _(optional — for automatic output transfer)_

---

## Installation

Clone the repo into your home directory on the cluster:

```bash
git clone https://github.com/ethan-dinh/HPC-Metagenomics.git ~/HPC-Metagenomics
```

Create the conda environment from the pinned spec. **Run from a dev node — not the login node.**

```bash
bash ~/HPC-Metagenomics/env/init_metagenomics.sh
```

| Flag | Description |
|------|-------------|
| `-e NAME` | Env name (default: `metagenomics`) |
| `-f` | Force-recreate existing env |
| `-u` | Update existing env from YAML |
| `-r` | Repack for `conda-stage` |
| `-y` | Non-interactive |
| `-q` | Quiet solver output |

Verify:

```bash
module load CBI miniforge3
conda activate metagenomics
which kneaddata kraken2 bracken metaphlan humann fasterq-dump
```

---

## Database Setup

All databases must be in place before running the pipeline.

### Mouse host depletion (KneadData / Bowtie2)

Download GRCm39 and build a Bowtie2 index. Run on a dev node (~1–2 hours).

```bash
mkdir -p ~/metagenomics/databases/mouse_C57BL_6NJ
cd ~/metagenomics/databases/mouse_C57BL_6NJ

wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/635/GCF_000001635.27_GRCm39/GCF_000001635.27_GRCm39_genomic.fna.gz
pigz -d GCF_000001635.27_GRCm39_genomic.fna.gz

bowtie2-build --threads 8 GCF_000001635.27_GRCm39_genomic.fna mouse_C57BL_6NJ
```

> Adjust the NCBI accession if you are using a different mouse strain.

### Kraken2

On Wynton, a standard database is already available at `/wynton/group/databases/kraken2` — no download needed. On other clusters, set `KRAKEN_DB` in [`jobs/run_metagenomics.sh`](jobs/run_metagenomics.sh#L206).

### Bracken

Build k-mer distributions from the Kraken2 database:

```bash
qsub ~/HPC-Metagenomics/utils/build_braken_db.sh
```

Output: `~/metagenomics/databases/bracken_db/`

### HUMAnN + MetaPhlAn

Download ChocoPhlAn, UniRef90 (DIAMOND), and the MetaPhlAn marker index. Expect 4–12 hours.

```bash
qsub ~/HPC-Metagenomics/utils/install_humann_db.sh
```

Databases are installed to:

```
~/metagenomics/databases/
├── humann_dbs/
│   ├── chocophlan/
│   └── uniref/
└── metaphlan_db/
```

---

## Preparing Input

The pipeline reads a **manifest TSV** — one row per sample, pointing to paired gzipped FASTQs.

```tsv
sample_id	R1	R2
SampleA	/wynton/scratch/user/raw/SampleA_R1.fastq.gz	/wynton/scratch/user/raw/SampleA_R2.fastq.gz
SampleB	/wynton/scratch/user/raw/SampleB_R1.fastq.gz	/wynton/scratch/user/raw/SampleB_R2.fastq.gz
```

- Tab-separated, with a header row
- Paths must be absolute
- Files must be gzip-compressed (`.fastq.gz`)
- `sample_id` must not contain spaces or special characters

The default manifest location is `~/metagenomics/manifest.tsv`.

### From raw FASTQ files

If your FASTQs are in UCSF Box, mirror them to Wynton scratch and auto-generate a manifest using `boxScratchTransfer.sh`. This script must run from a `dt1` data transfer node.

```bash
ssh dt1.wynton.ucsf.edu
bash ~/HPC-Metagenomics/utils/boxScratchTransfer.sh /metagenomics_inbox
```

This mirrors all FASTQs to `/wynton/scratch/$USER/metag/raw/` and writes `manifest.tsv` to the current directory. Requires a `~/.netrc` file — see [Box Transfer Setup](#box-transfer-setup).

### From an SRA accession

Use [`utils/sra_download.sh`](utils/sra_download.sh) to download paired-end FASTQs from NCBI SRA and build the manifest in one step. SRA Tools and Entrez Direct are included in the conda environment — no separate install needed.

**Create an accessions file** — one SRA run accession per line, no header:

```
SRR12345678
SRR12345679
SRR12345680
```

**Download all accessions sequentially** (dev node, small cohorts):

```bash
ssh dev1.wynton.ucsf.edu
bash ~/HPC-Metagenomics/utils/sra_download.sh \
    --accessions accessions.txt
```

**Download as an SGE array job** (one task per accession, recommended for large cohorts):

```bash
N=$(grep -c . accessions.txt)
qsub -t 1-${N} \
    ~/HPC-Metagenomics/utils/sra_download.sh \
        --accessions accessions.txt
```

Each task downloads one accession, compresses the output with pigz, and appends its entry to the manifest. The manifest is safe to build incrementally — existing entries are never duplicated.

**`sra_download.sh` flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--accessions FILE` | `-a` | Accessions file (required) |
| `--outdir DIR` | `-o` | Download directory (default: `/wynton/scratch/$USER/metag/raw`) |
| `--manifest FILE` | `-m` | Manifest output path (default: `~/metagenomics/manifest.tsv`) |
| `--threads N` | `-t` | Threads for `fasterq-dump` (default: `$NSLOTS` or 8) |

Once the downloads complete, the manifest at `~/metagenomics/manifest.tsv` is ready to pass directly to the pipeline.

---

## Running the Pipeline

### Single sample (dev node)

Test on one sample interactively before submitting a full array job:

```bash
ssh dev1.wynton.ucsf.edu

TASK_INDEX=1 bash ~/HPC-Metagenomics/jobs/run_metagenomics.sh \
    --all --run-humann \
    --manifest ~/metagenomics/manifest.tsv \
    --study-name my_study \
    --output-base-dir metagenomics/out
```

`TASK_INDEX` selects the row in the manifest (1-based, excluding header).

### Array job (compute nodes)

**Count samples:**

```bash
N=$(( $(wc -l < ~/metagenomics/manifest.tsv) - 1 ))
echo "$N samples"
```

**Submit:**

```bash
qsub -t 1-${N} -tc 35 \
    ~/HPC-Metagenomics/jobs/run_metagenomics.sh \
        --all --run-humann \
        --manifest ~/metagenomics/manifest.tsv \
        --study-name my_study \
        --output-base-dir metagenomics/out
```

> `-tc 35` caps concurrent tasks at 35. If using `--transfer-to-box`, keep this value — Box throttles FTP above ~35 connections. Without Box transfer you can raise it to the cluster limit (~40–50).

**Monitor:**

```bash
qstat -u $USER
```

**Re-run a failed task:**

The pipeline is idempotent — completed steps are skipped automatically. Re-submit a specific task index to resume:

```bash
TASK_INDEX=7 bash ~/HPC-Metagenomics/jobs/run_metagenomics.sh \
    --all --run-humann \
    --manifest ~/metagenomics/manifest.tsv \
    --study-name my_study
```

**Run steps independently:**

If KneadData already completed, run only downstream steps:

```bash
qsub -t 1-${N} -tc 35 \
    ~/HPC-Metagenomics/jobs/run_metagenomics.sh \
        --run-kraken2 --run-bracken --run-humann \
        --manifest ~/metagenomics/manifest.tsv \
        --study-name my_study
```

---

## Flags Reference

| Flag | Short | Description |
|------|-------|-------------|
| `--run-kneaddata` | `-k` | Quality trim + host depletion |
| `--run-kraken2` | `-r` | Taxonomic classification |
| `--run-bracken` | `-b` | Abundance re-estimation |
| `--run-humann` | `-f` | Functional profiling |
| `--all` | `-a` | Run `-k -r -b` together |
| `--manifest PATH` | `-m` | Manifest TSV (default: `~/metagenomics/manifest.tsv`) |
| `--output-base-dir PATH` | `-o` | Output dir relative to `$HOME` (default: `metagenomics/out`) |
| `--study-name NAME` | `-n` | Study name for log/output organization (default: `metagenomics`) |
| `--save-to-scratch` | `-s` | Write outputs to `/wynton/scratch/$USER` instead of `$HOME` |
| `--transfer-to-box` | `-t` | Transfer outputs to UCSF Box on completion |
| `--box-dir PATH` | `-d` | Destination path in Box |
| `--help` | `-h` | Show usage |

---

## Output Structure

Default root: `$HOME/metagenomics/out/<sample_id>/`
With `--save-to-scratch`: `/wynton/scratch/$USER/metagenomics/out/<sample_id>/`

```
<sample_id>/
├── kneaddata/
│   ├── <sample_id>_*_kneaddata_paired_1.fastq.gz      # Clean R1
│   ├── <sample_id>_*_kneaddata_paired_2.fastq.gz      # Clean R2
│   ├── <sample_id>_*_kneaddata_unmatched_1.fastq.gz
│   ├── <sample_id>_*_kneaddata_unmatched_2.fastq.gz
│   ├── <sample_id>.kneaddata.read_count.tsv            # Read count summary
│   ├── <sample_id>.kneaddata.log
│   └── <sample_id>_fastqc.tar.gz                       # FastQC reports
├── kraken2/
│   ├── <sample_id>.kraken2.report                      # Kraken2 report
│   └── <sample_id>.kraken2.labels.tsv                  # Per-read taxonomy labels
├── bracken/
│   ├── <sample_id>_species.tsv                         # Species-level abundances
│   ├── <sample_id>_species.outreport
│   ├── <sample_id>_genus.tsv                           # Genus-level abundances
│   └── <sample_id>_genus.outreport
├── humann/
│   ├── <sample_id>_genefamilies.tsv                    # Gene family RPK
│   ├── <sample_id>_pathabundance.tsv                   # Pathway RPK
│   └── <sample_id>_pathcoverage.tsv                    # Pathway coverage
└── qstat_<JOB_ID>_<TASK_INDEX>.txt                     # SGE job metadata
```

Logs: `~/logs/<study_name>/<sample_id>_meta.log`

---

## Box Transfer Setup

Outputs can be automatically pushed to UCSF Box after each sample completes. This requires three things: a Box app password, a `~/.netrc` file, and an SSH key for the `dt1` transfer node.

### Create a Box app password

UCSF Box uses SSO, so FTP access requires a separate app password:

1. Log in at [ucsf.box.com](https://ucsf.box.com)
2. **Account Settings → Authentication → App Passwords → Create Password**
3. Name it (e.g., `wynton-ftp`) and save the generated password

### Configure `~/.netrc`

```bash
cat > ~/.netrc << 'EOF'
machine ftp.box.com
    login your_email@ucsf.edu
    password YOUR_APP_PASSWORD
EOF
chmod 600 ~/.netrc
```

### Create an SSH key for the data transfer node

The pipeline SSHes from compute nodes to `dt1` to run the Box upload. Create a dedicated key:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/wynton-dtn-key -N "" -C "wynton-dtn"
ssh-copy-id -i ~/.ssh/wynton-dtn-key.pub dt1.wynton.ucsf.edu

# Verify
ssh -i ~/.ssh/wynton-dtn-key dt1.wynton.ucsf.edu hostname
```

### Submit with transfer enabled

```bash
qsub -t 1-${N} -tc 35 \
    ~/HPC-Metagenomics/jobs/run_metagenomics.sh \
        --all --run-humann \
        --transfer-to-box \
        --box-dir /metagenomics_results/my_study \
        --manifest ~/metagenomics/manifest.tsv \
        --study-name my_study
```

---

## Wynton Notes

### Node types

| Node | Purpose |
|------|---------|
| Login (`log1`, `log2`) | Job submission and file editing only |
| Dev (`dev1`, `dev2`) | Interactive work, database downloads, env setup |
| Compute nodes | All pipeline jobs via `qsub` |

### Storage

| Location | Quota | Purge policy |
|----------|-------|--------------|
| `$HOME` | ~1 TB | Never |
| `/wynton/scratch/$USER` | None | Files older than 14 days |
| `$TMPDIR` (node-local) | ~300 GB/node | Cleared after job ends |

Keep raw FASTQs and databases on scratch or group storage. Use `--save-to-scratch` to write outputs there too.

### NIS/LDAP authentication error

If jobs fail with:

```
can't get password entry for user "USER". Either user does not exist or error with NIS/LDAP etc.
```

Find the failing node:

```bash
tail -100000 /opt/sge/wynton/common/accounting \
    | awk -F: '$12 == 1 {print $2}' \
    | sort | uniq -c | sort -rh
```

Exclude it at submission time:

```bash
qsub -l hostname="\!qb3-id138" ~/HPC-Metagenomics/jobs/run_metagenomics.sh ...
```

---

## Troubleshooting

**KneadData / Trimmomatic error**

The pipeline auto-patches `kneaddata/config.py` to reference `trimmomatic.jar` directly. If it still fails, verify the patch manually:

```bash
grep trimmomatic_jar $CONDA_PREFIX/lib/python3.9/site-packages/kneaddata/config.py
# Expected: trimmomatic_jar="trimmomatic.jar"
```

Or re-run `init_metagenomics.sh` to apply it fresh.

---

**Bracken database not found**

```bash
qsub ~/HPC-Metagenomics/utils/build_braken_db.sh

# Verify
ls ~/metagenomics/databases/bracken_db/
# Expected: database100mers.kmer_distrib  database100mers.kraken
```

---

**HUMAnN databases missing**

```bash
qsub ~/HPC-Metagenomics/utils/install_humann_db.sh

# Verify
humann_databases --available
```

---

**SRA download fails or produces single-end files**

Verify the accession is a paired-end run using Entrez Direct:

```bash
efetch -db sra -id SRR12345678 -format runinfo | cut -d',' -f16
# Expected: PAIRED
```

If the download stalls mid-way, re-run `sra_download.sh` — it skips accessions whose `.fastq.gz` files already exist. Partial downloads in `.tmp_<acc>/` are cleaned up automatically by `fasterq-dump` on retry.

---

**Box transfer fails after retries**

The transfer script retries 5 times with 10-second backoff. If all retries fail, test the connection manually:

```bash
ssh -i ~/.ssh/wynton-dtn-key dt1.wynton.ucsf.edu \
    "lftp -e 'open ftp.box.com; ls; bye'"
```

Check that `~/.netrc` has correct credentials and `chmod 600` is set.

---

**Job runs out of scratch space**

Each job requests 100 GB (`#$ -l scratch=100G` in [`jobs/run_metagenomics.sh`](jobs/run_metagenomics.sh)). Increase that value for very large samples, or verify available space mid-job:

```bash
df -h $TMPDIR
```
