#!/bin/bash
#SBATCH --job-name=fastbiodl_benchmark
#SBATCH --account=umr115         # e.g. abc123 or TG-ABC123456
#SBATCH --partition=compute               # exclusive node, 128 cores, 1 TB NVMe
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16               # adjust based on your --threads setting
#SBATCH --mem=64G
#SBATCH --time=08:00:00                  # 8 hours; adjust per dataset size
#SBATCH --output=logs/slurm_%j.out
#SBATCH --error=logs/slurm_%j.err

# ─── Environment ───────────────────────────────────────────────────────────
module purge
module load slurm cpu/0.17.3b anaconda3/2021.05
conda activate fastbiodl

REPO_DIR="/expanse/lustre/scratch/$USER/temp_project/FastBioDL-Adaptive-Parallel_Downloader"
cd "$REPO_DIR"

# Add sra-toolkit to PATH
export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"

# ─── Storage setup ─────────────────────────────────────────────────────────
# Local NVMe: fast scratch, wiped at job end
NVME_DIR="/scratch/$USER/temp_project/job_$SLURM_JOB_ID"
mkdir -p "$NVME_DIR"

# Final output: Lustre, persists after job
LUSTRE_OUT="/expanse/lustre/scratch/$USER/temp_project/benchmark_results"
mkdir -p "$LUSTRE_OUT"
mkdir -p logs/fastbiodl logs/kingfisher

# Export so the Python code can pick them up
export LOCAL_SCRATCH="$NVME_DIR"
export SLURM_JOB_ID="$SLURM_JOB_ID"

# ─── Optional: your NCBI credentials for higher rate limits ────────────────
# export NCBI_EMAIL="your@email.com"
# export NCBI_API_KEY="your_key"

# ─── Run: FastBioDL medium ─────────────────────────────────────────────────
echo "=== FastBioDL medium ==="
python fastbiodl_upgrade.py \
    -i accessions_medium_PRJNA353374.txt \
    -o "$LUSTRE_OUT/fastbiodl_medium" \
    --segment-size 512 \
    --max-segments 8 \
    --max-retries 3

# ─── Run: aria2c iterative medium ──────────────────────────────────────────
echo "=== aria2c iterative medium ==="
mkdir -p "$NVME_DIR/aria2c_medium/sra" \
         "$NVME_DIR/aria2c_medium/fastq"
mkdir -p "$LUSTRE_OUT/aria2c_medium"

python benchmark_aria2c_kingfisher.py \
    -i accessions_medium_PRJNA353374.txt \
    --sra-dir  "$NVME_DIR/aria2c_medium/sra" \
    --fastq-dir "$NVME_DIR/aria2c_medium/fastq" \
    --out-dir  "$LUSTRE_OUT/aria2c_medium" \
    --threads 16

# ─── Copy all JSON results and logs to Lustre ──────────────────────────────
echo "=== Copying results to Lustre ==="
cp -r logs/ "$LUSTRE_OUT/"

echo "=== Done. Results in $LUSTRE_OUT ==="