#!/usr/bin/env bash
set -Eeuo pipefail

# FastBioDL + aria2c iterative benchmark runner.
# Usage examples:
#   bash run_fastbiodl_codex.sh setup
#   bash run_fastbiodl_codex.sh prompts
#   bash run_fastbiodl_codex.sh medium
#   bash run_fastbiodl_codex.sh large
#   bash run_fastbiodl_codex.sh small
#   bash run_fastbiodl_codex.sh compare
#   bash run_fastbiodl_codex.sh all
#
# This script avoids trying to automate Codex CLI flags because those vary by version.
# Instead it generates the prompt files and runs the deterministic benchmark commands.

MODE="${1:-all}"
REPO_DIR="${REPO_DIR:-$PWD}"
VENV_ACTIVATE="${VENV_ACTIVATE:-venv/bin/activate}"
FASTBIODL_OUT_BASE="${FASTBIODL_OUT_BASE:-benchmark/fastbiodl}"
ARIA2C_DISK_BASE="${ARIA2C_DISK_BASE:-benchmark/aria2c}"
NVME_BASE="${LOCAL_SCRATCH:-/scratch/${USER:-user}/job_${SLURM_JOB_ID:-local}}"
ARIA2C_NVME_BASE="${ARIA2C_NVME_BASE:-$NVME_BASE/benchmark/aria2c}"
THREADS="${THREADS:-8}"
SEGMENT_SIZE_MB="${SEGMENT_SIZE_MB:-512}"
MAX_SEGMENTS="${MAX_SEGMENTS:-8}"
MAX_RETRIES="${MAX_RETRIES:-3}"

cd "$REPO_DIR"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing command: $1" >&2
    exit 1
  }
}

activate_venv() {
  if [[ -f "$VENV_ACTIVATE" ]]; then
    # shellcheck disable=SC1090
    source "$VENV_ACTIVATE"
  else
    echo "Virtual environment activate script not found: $VENV_ACTIVATE" >&2
    exit 1
  fi
}

setup_env() {
  echo "[setup] activating venv"
  activate_venv
  echo "[setup] updating pip and installing Python requirements"
  python -m pip install -U pip
  python -m pip install -r requirements.txt
  echo "[setup] checking external tools"
  need_cmd python
  need_cmd git
  need_cmd aria2c
  need_cmd fasterq-dump
  need_cmd pigz
  mkdir -p logs/fastbiodl logs/kingfisher
  mkdir -p "$FASTBIODL_OUT_BASE"/{small,medium,large}
  mkdir -p "$ARIA2C_DISK_BASE"/{small,medium,large}/{sra,output}
  mkdir -p "$ARIA2C_NVME_BASE"/{small,medium,large}/fastq
  echo "[setup] done"
}

write_prompts() {
  mkdir -p codex_prompts

  cat > codex_prompts/01_audit_and_patch.txt <<'EOF'
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
EOF

  cat > codex_prompts/02_smoke_test.txt <<'EOF'
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
EOF

  cat > codex_prompts/03_fix_medium_fastbiodl.txt <<'EOF'
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
EOF

  cat > codex_prompts/04_compare_vs_aria2c.txt <<'EOF'
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
EOF

  cat > codex_prompts/05_tuning.txt <<'EOF'
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
EOF

  cat > codex_prompts/06_final_validation.txt <<'EOF'
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
EOF

  echo "[prompts] wrote prompt files to codex_prompts/"
}

clean_dataset_dirs() {
  local ds="$1"
  rm -rf "$FASTBIODL_OUT_BASE/$ds"
  rm -rf "$ARIA2C_DISK_BASE/$ds"
  rm -rf "$ARIA2C_NVME_BASE/$ds"
  mkdir -p "$FASTBIODL_OUT_BASE/$ds"
  mkdir -p "$ARIA2C_DISK_BASE/$ds"/{sra,output}
  mkdir -p "$ARIA2C_NVME_BASE/$ds/fastq"
}

run_fastbiodl() {
  local ds="$1"
  local input_file
  case "$ds" in
    medium) input_file="accessions_medium_PRJNA353374.txt" ;;
    large)  input_file="accessions_large_PRJNA200694.txt" ;;
    small)  input_file="accessions_small_PRJNA916347.txt" ;;
    *) echo "Unknown dataset: $ds" >&2; exit 1 ;;
  esac

  activate_venv
  echo "[fastbiodl] dataset=$ds"
  python fastbiodl_upgrade.py \
    -i "$input_file" \
    -o "$FASTBIODL_OUT_BASE/$ds" \
    --segment-size "$SEGMENT_SIZE_MB" \
    --max-segments "$MAX_SEGMENTS" \
    --max-retries "$MAX_RETRIES"
}

run_aria2c_benchmark() {
  local ds="$1"
  local input_file
  case "$ds" in
    medium) input_file="accessions_medium_PRJNA353374.txt" ;;
    large)  input_file="accessions_large_PRJNA200694.txt" ;;
    small)  input_file="accessions_small_PRJNA916347.txt" ;;
    *) echo "Unknown dataset: $ds" >&2; exit 1 ;;
  esac

  activate_venv
  echo "[aria2c-benchmark] dataset=$ds"
  python benchmark_aria2c_kingfisher.py \
    -i "$input_file" \
    --sra-dir "$ARIA2C_DISK_BASE/$ds/sra" \
    --fastq-dir "$ARIA2C_NVME_BASE/$ds/fastq" \
    --out-dir "$ARIA2C_DISK_BASE/$ds/output" \
    --threads "$THREADS"
}

run_dataset_pair() {
  local ds="$1"
  clean_dataset_dirs "$ds"
  run_fastbiodl "$ds"
  run_aria2c_benchmark "$ds"
}

compare_latest() {
  python - <<'PY'
import glob, json, os, sys
ff = sorted(glob.glob('logs/fastbiodl/benchmark_fastbiodl_results_*.json'))
af = sorted(glob.glob('logs/kingfisher/benchmark_aria2c_kingfisher_results_*.json'))
if not ff:
    print('No FastBioDL benchmark JSON found.', file=sys.stderr)
    sys.exit(1)
if not af:
    print('No aria2c iterative benchmark JSON found.', file=sys.stderr)
    sys.exit(1)
f = ff[-1]
a = af[-1]
with open(f) as fh:
    fj = json.load(fh)
with open(a) as ah:
    aj = json.load(ah)
speedup = aj['total_time_s'] / fj['total_time_s']
print('FastBioDL JSON :', f)
print('aria2c JSON    :', a)
print('FastBioDL total_time_s      :', fj['total_time_s'])
print('aria2c iterative total_time_s:', aj['total_time_s'])
print('Speedup                    :', round(speedup, 3))
PY
}

syntax_check() {
  activate_venv
  python -m py_compile \
    fastbiodl_upgrade.py \
    benchmark_aria2c_kingfisher.py \
    config_fastbiodl.py \
    converter.py \
    mover.py
  python fastbiodl_upgrade.py --help >/dev/null
  python benchmark_aria2c_kingfisher.py --help >/dev/null
  echo "[smoke] syntax and help checks passed"
}

case "$MODE" in
  setup)
    setup_env
    ;;
  prompts)
    write_prompts
    ;;
  smoke)
    setup_env
    syntax_check
    ;;
  medium)
    run_dataset_pair medium
    ;;
  large)
    run_dataset_pair large
    ;;
  small)
    run_dataset_pair small
    ;;
  compare)
    compare_latest
    ;;
  all)
    setup_env
    write_prompts
    syntax_check
    echo
    echo "=== Paste codex_prompts/01_audit_and_patch.txt into Codex, then codex_prompts/02_smoke_test.txt ==="
    echo "=== After Codex patches the code, this script will continue with medium, large, small runs in order. ==="
    run_dataset_pair medium
    compare_latest
    run_dataset_pair large
    compare_latest
    run_dataset_pair small
    compare_latest
    echo
    echo "=== Paste codex_prompts/06_final_validation.txt into Codex after reviewing results. ==="
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    exit 1
    ;;
esac
