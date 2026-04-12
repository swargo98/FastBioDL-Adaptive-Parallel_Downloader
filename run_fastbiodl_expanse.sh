#!/bin/bash
#SBATCH --job-name=fastbiodl_medium
#SBATCH --account=umr115
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=08:00:00
#SBATCH --output=slurm_%j.out       # no logs/ prefix — dir may not exist yet
#SBATCH --error=slurm_%j.err

set -uo pipefail                     # u and o but NOT e — don't abort on first error

export PS1="${PS1:-}"

# ── Conda ──────────────────────────────────────────────────────────────────
PYTHON_BIN="/home/rswargo/.conda/envs/fastbiodl/bin/python"
export PATH="/home/rswargo/.conda/envs/fastbiodl/bin:$PATH"

"$PYTHON_BIN" -c "import aiohttp" || { echo "[ERROR] aiohttp missing"; exit 1; }

REPO_DIR="${SLURM_SUBMIT_DIR:-$PWD}"

# ── Storage ────────────────────────────────────────────────────────────────
NVME_DIR="/scratch/$USER/job_$SLURM_JOB_ID"
mkdir -p "$NVME_DIR" || NVME_DIR="/tmp/$USER/job_$SLURM_JOB_ID" && mkdir -p "$NVME_DIR"

LUSTRE_OUT="/expanse/lustre/scratch/$USER/temp_project/results/$SLURM_JOB_ID"
mkdir -p "$LUSTRE_OUT"
mkdir -p logs/fastbiodl logs/kingfisher

export LOCAL_SCRATCH="$NVME_DIR"

echo "Repo:    $REPO_DIR"
echo "Python:  $PYTHON_BIN"
echo "NVMe:    $NVME_DIR"
echo "Results: $LUSTRE_OUT"
which aria2c fasterq-dump pigz

# ── FastBioDL ──────────────────────────────────────────────────────────────
echo "=== FastBioDL medium ==="
"$PYTHON_BIN" fastbiodl_upgrade.py \
    -i accessions_medium_PRJNA353374.txt \
    -o "$LUSTRE_OUT/fastbiodl_medium" \
    --segment-size 512 --max-segments 8 --max-retries 3
echo "FastBioDL exit code: $?"

# ── aria2c baseline ────────────────────────────────────────────────────────
echo "=== aria2c iterative medium ==="
mkdir -p "$NVME_DIR/aria2c/sra" "$NVME_DIR/aria2c/fastq"
"$PYTHON_BIN" benchmark_aria2c_kingfisher.py \
    -i accessions_medium_PRJNA353374.txt \
    --sra-dir  "$NVME_DIR/aria2c/sra" \
    --fastq-dir "$NVME_DIR/aria2c/fastq" \
    --out-dir  "$LUSTRE_OUT/aria2c_medium" \
    --threads 16
echo "aria2c exit code: $?"

# ── Save everything ────────────────────────────────────────────────────────
cp -r logs/ "$LUSTRE_OUT/"
cp slurm_${SLURM_JOB_ID}.out slurm_${SLURM_JOB_ID}.err "$LUSTRE_OUT/" 2>/dev/null || true

echo "=== Done. Results in $LUSTRE_OUT ==="