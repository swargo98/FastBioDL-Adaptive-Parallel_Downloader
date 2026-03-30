# FastBioDL + Codex CLI Runbook using `benchmark_aria2c_kingfisher.py`

This runbook assumes you are inside the `journal` branch working tree of `swargo98/FastBioDL-Adaptive-Parallel_Downloader`, with an existing virtual environment that you will activate with:

```bash
source venv/bin/activate
```

The benchmark baseline is **`benchmark_aria2c_kingfisher.py`**, which runs an **iterative per-accession pipeline**:
1. NCBI URL fetch
2. `aria2c` download to `--sra-dir` on DISK
3. `fasterq-dump` conversion to `--fastq-dir` on NVMe
4. `pigz` compression to `--out-dir` on DISK

It writes logs and JSON outputs under `logs/kingfisher/` by default.

FastBioDL still stages downloads on NVMe and writes benchmark JSON under `logs/fastbiodl/`. Its current uploaded config uses `max_conversion_jobs=2` and `conversion_threads=8`, which is a reasonable conservative baseline to preserve stability before tuning.

The uploaded FastBioDL code path still centers on `fastbiodl_upgrade.py`, `converter.py`, and `mover.py`, so Codex should focus its fixes there, plus the new benchmark script.

## 1. Environment setup

```bash
git fetch origin
git checkout journal
git pull --ff-only origin journal
git checkout -B codex-fastbiodl-debug

source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

Install required system tools if missing:

```bash
which aria2c || echo "Install aria2c"
which fasterq-dump || echo "Install sra-tools"
which pigz || echo "Install pigz"
```

Optional but recommended:

```bash
export NCBI_EMAIL="your_email@example.com"
export NCBI_API_KEY="your_ncbi_api_key"
```

## 2. Benchmark directories

```bash
NVME_BASE="${LOCAL_SCRATCH:-/scratch/${USER}/job_${SLURM_JOB_ID:-local}}"
mkdir -p benchmark/fastbiodl/{small,medium,large}
mkdir -p benchmark/aria2c/{small,medium,large}/{sra,output}
mkdir -p "$NVME_BASE"/benchmark/aria2c/{small,medium,large}/fastq
mkdir -p logs/fastbiodl logs/kingfisher
```

## 3. Codex prompts

Start Codex in the repo root using your installed CLI syntax. Since CLI flags vary by version, use `codex --help` if needed.

### Prompt 1: audit + first-pass fixes

```text
You are in the root of the FastBioDL-Adaptive-Parallel_Downloader repository on the current branch.

Goal:
Get the journal-branch prototype into a working state for end-to-end benchmarking with fastbiodl_upgrade.py and benchmark_aria2c_kingfisher.py using these accession lists:
- accessions_large_PRJNA200694.txt
- accessions_medium_PRJNA353374.txt
- accessions_small_PRJNA916347.txt

Constraints:
- Use the already activated Python virtual environment.
- Do not rewrite the project from scratch.
- Keep changes minimal and research-prototype friendly.
- Ignore transient server-side download failures from NCBI/ENA/AWS if the pipeline is otherwise correct.
- Prioritize correctness of end-to-end execution and benchmark fairness over code elegance.

Tasks:
1. Inspect fastbiodl_upgrade.py, converter.py, mover.py, benchmark_aria2c_kingfisher.py, config_fastbiodl.py, ncbi_lookup.py, and utils.py.
2. Identify critical blockers that will prevent:
   - successful FastBioDL runs
   - successful aria2c iterative baseline runs
   - valid end-to-end wall-clock comparison
3. Propose a minimal patch plan in priority order.
4. Then implement only the high-priority fixes first.
5. After editing, summarize exactly what changed and why.

Critical issues to verify specifically:
- blocking mp.Queue.get() inside async downloader loop
- free-space unit mismatch in downloader
- pigz sibling-process leak on partial compression failure
- shutdown path that signals the wrong mover object or leaves workers hanging
- aria2c benchmark correctness for per-accession download -> convert -> compress pipeline
- any other issue that would obviously break the requested runs

Do not run the full benchmarks yet. First finish the code audit and the first-pass patch set.
```

### Prompt 2: smoke test only

```text
Now do a smoke test only.

Tasks:
1. Run Python syntax checks or import checks on the modified files.
2. Confirm the following commands resolve in PATH:
   - aria2c
   - fasterq-dump
   - pigz
3. Run the benchmark script with --help and the FastBioDL script with --help.
4. Report any missing dependency or CLI mismatch.
5. If needed, make small compatibility fixes, but do not start the full accession runs yet.

At the end, print the exact commands you recommend for:
- FastBioDL medium run
- aria2c iterative benchmark medium run
```

### Prompt 3: analyze and fix medium FastBioDL run

```text
The medium FastBioDL run has completed or failed.

Tasks:
1. Inspect the newest files in:
   - logs/fastbiodl/
   - any failed_downloads_*.txt
2. Classify every failure into one of:
   - code bug
   - environment/dependency issue
   - transient server-side or remote-data issue
3. Ignore only truly server-side download failures.
4. For all code or environment issues, patch the code and explain the root cause.
5. Re-run the medium FastBioDL command until it finishes cleanly or only server-side failures remain.
6. At the end, summarize:
   - total successes
   - total failures
   - which failures were ignored as server-side
   - exact files changed
```

### Prompt 4: compare against aria2c iterative baseline

```text
Now compare the newest benchmark JSON results for FastBioDL and benchmark_aria2c_kingfisher.py.

Tasks:
1. Locate the newest:
   - logs/fastbiodl/benchmark_fastbiodl_results_*.json
   - logs/kingfisher/benchmark_aria2c_kingfisher_results_*.json
2. Compute speedup as:
   speedup = aria2c_total_time / fastbiodl_total_time
3. Print:
   - FastBioDL total_time_s
   - aria2c iterative total_time_s
   - speedup
4. If speedup is below 1.3x, identify the dominant bottleneck from logs and code.
5. Apply only low-risk tuning or performance fixes that should improve end-to-end wall clock without destabilizing correctness.
6. Re-run medium until speedup is at least 1.3x or you can clearly justify why a remaining limitation is external.
```

### Prompt 5: performance tuning pass

```text
We now care about end-to-end wall-clock time, not internal elegance.

Objective:
Get FastBioDL to at least 1.3x speedup over benchmark_aria2c_kingfisher.py on the medium list, then carry the same stable patch set to small and large.

Rules:
- Do not introduce risky architectural rewrites.
- Prefer small changes with measurable impact.
- Prioritize changes that reduce total_time_s.
- Use logs and benchmark JSON to justify every tuning change.
- Do not optimize download throughput if download is not the dominant phase.
- If you change config defaults, explain why and keep them conservative.

Tasks:
1. Inspect benchmark phase timing and logs.
2. Identify whether the bottleneck is download, conversion, compression, or move.
3. Apply one tuning step at a time.
4. Re-run benchmark after each meaningful change.
5. Stop once speedup >= 1.3x and the run is stable.
6. Print the final recommended config values and command lines.
```

### Prompt 6: final validation across all three lists

```text
Now validate the final prototype across large, medium, and small accession sets.

Definition of done:
- FastBioDL runs successfully on all three accession lists, except for any clearly transient server-side failures
- benchmark_aria2c_kingfisher.py runs successfully on all three lists
- speedup >= 1.3x on total end-to-end wall-clock time for each list
- no critical bugs remain in downloader, converter, mover, or aria2c iterative benchmark logic
- no silent data-loss paths remain
- the final state is a working research prototype, not necessarily production-perfect

Tasks:
1. Collect the newest FastBioDL and aria2c iterative JSON results for all three datasets.
2. Print a summary table:
   dataset | fastbiodl_total_time_s | aria2c_total_time_s | speedup | notes
3. Print the final list of source files changed.
4. Print the final commands required to reproduce all six runs.
5. Print any remaining known limitations that are acceptable for a research prototype.
```

## 4. Dataset run commands

### Medium first

```bash
NVME_BASE="${LOCAL_SCRATCH:-/scratch/${USER}/job_${SLURM_JOB_ID:-local}}"
rm -rf benchmark/fastbiodl/medium
rm -rf benchmark/aria2c/medium
rm -rf "$NVME_BASE/benchmark/aria2c/medium"
mkdir -p benchmark/fastbiodl/medium
mkdir -p benchmark/aria2c/medium/{sra,output}
mkdir -p "$NVME_BASE/benchmark/aria2c/medium/fastq"

python fastbiodl_upgrade.py \
  -i accessions_medium_PRJNA353374.txt \
  -o benchmark/fastbiodl/medium \
  --segment-size 512 \
  --max-segments 8 \
  --max-retries 3

python benchmark_aria2c_kingfisher.py \
  -i accessions_medium_PRJNA353374.txt \
  --sra-dir benchmark/aria2c/medium/sra \
  --fastq-dir "$NVME_BASE/benchmark/aria2c/medium/fastq" \
  --out-dir benchmark/aria2c/medium/output \
  --threads 8
```

### Large

```bash
NVME_BASE="${LOCAL_SCRATCH:-/scratch/${USER}/job_${SLURM_JOB_ID:-local}}"
rm -rf benchmark/fastbiodl/large
rm -rf benchmark/aria2c/large
rm -rf "$NVME_BASE/benchmark/aria2c/large"
mkdir -p benchmark/fastbiodl/large
mkdir -p benchmark/aria2c/large/{sra,output}
mkdir -p "$NVME_BASE/benchmark/aria2c/large/fastq"

python fastbiodl_upgrade.py \
  -i accessions_large_PRJNA200694.txt \
  -o benchmark/fastbiodl/large \
  --segment-size 512 \
  --max-segments 8 \
  --max-retries 3

python benchmark_aria2c_kingfisher.py \
  -i accessions_large_PRJNA200694.txt \
  --sra-dir benchmark/aria2c/large/sra \
  --fastq-dir "$NVME_BASE/benchmark/aria2c/large/fastq" \
  --out-dir benchmark/aria2c/large/output \
  --threads 8
```

### Small

```bash
NVME_BASE="${LOCAL_SCRATCH:-/scratch/${USER}/job_${SLURM_JOB_ID:-local}}"
rm -rf benchmark/fastbiodl/small
rm -rf benchmark/aria2c/small
rm -rf "$NVME_BASE/benchmark/aria2c/small"
mkdir -p benchmark/fastbiodl/small
mkdir -p benchmark/aria2c/small/{sra,output}
mkdir -p "$NVME_BASE/benchmark/aria2c/small/fastq"

python fastbiodl_upgrade.py \
  -i accessions_small_PRJNA916347.txt \
  -o benchmark/fastbiodl/small \
  --segment-size 512 \
  --max-segments 8 \
  --max-retries 3

python benchmark_aria2c_kingfisher.py \
  -i accessions_small_PRJNA916347.txt \
  --sra-dir benchmark/aria2c/small/sra \
  --fastq-dir "$NVME_BASE/benchmark/aria2c/small/fastq" \
  --out-dir benchmark/aria2c/small/output \
  --threads 8
```

## 5. Quick comparison helper

```bash
python - <<'PY'
import glob, json
f = sorted(glob.glob('logs/fastbiodl/benchmark_fastbiodl_results_*.json'))[-1]
a = sorted(glob.glob('logs/kingfisher/benchmark_aria2c_kingfisher_results_*.json'))[-1]
fj = json.load(open(f))
aj = json.load(open(a))
speedup = aj['total_time_s'] / fj['total_time_s']
print('FastBioDL total_time_s:', fj['total_time_s'])
print('aria2c iterative total_time_s:', aj['total_time_s'])
print('Speedup:', round(speedup, 3))
PY
```

## 6. What to optimize first if speedup is weak

1. Fix correctness blockers first.
2. Tune conversion and compression before obsessing over downloader micro-optimizations.
3. Keep changes low-risk.
4. Ignore genuinely server-side failures.

That is the civilized version. The shell script is next, for when civilization fails.


./run_fastbiodl_codex.sh setup
./run_fastbiodl_codex.sh prompts
./run_fastbiodl_codex.sh smoke
./run_fastbiodl_codex.sh medium
./run_fastbiodl_codex.sh compare
