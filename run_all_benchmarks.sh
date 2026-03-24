#!/usr/bin/env bash

# Run benchmark scripts across accession lists, repeating each combination.
# Loop order:
#   i: accession list
#   j: benchmark script
#   k: run count (1..4)

set -u

to_human() {
  local bytes="$1"
  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec --suffix=B "$bytes"
  else
    echo "${bytes}B"
  fi
}

total_deleted_bytes=0

accession_lists=(
  "accessions_large_PRJNA251383.txt"
  "accessions_medium_PRJNA353374.txt"
  "accessions_small_PRJNA916347.txt"
  # "accessions_large_PRJNA200694.txt"
)

scripts=(
  "fastbiodl_upgrade.py"
  "benchmark_aria2c_kingfisher.py"
  # "benchmark_aria2c.py"
  # "benchmark_pysradb.py"
  # "benchmark_sratools.py"
)

for accession_list in "${accession_lists[@]}"; do
  if [[ ! -f "$accession_list" ]]; then
    echo "[ERROR] Missing accession list: $accession_list"
    continue
  fi

  for run_count in {1..3}; do
    for script in "${scripts[@]}"; do
      if [[ ! -f "$script" ]]; then
        echo "[ERROR] Missing script: $script"
        continue
      fi
      echo "============================================================"
      echo "Running: python3 $script -i $accession_list (run $run_count/3)"
      echo "============================================================"

      python3 "$script" -i "$accession_list"
      exit_code=$?
      if [[ $exit_code -ne 0 ]]; then
        echo "[WARN] Command failed with exit code $exit_code: python3 $script -i $accession_list"
      fi

      clear_output="$(CLEAR_CONTEXT="script=$script accession_list=$accession_list run=$run_count" ./clear_files.sh 2>&1)"
      clear_exit_code=$?
      echo "$clear_output"
      if [[ $clear_exit_code -ne 0 ]]; then
        echo "[WARN] clear_files.sh failed with exit code $clear_exit_code"
      else
        deleted_bytes="$(echo "$clear_output" | awk -F= '/^WORKDIR_DELETED_BYTES=/{print $2}' | tail -n1)"
        if [[ "$deleted_bytes" =~ ^[0-9]+$ ]]; then
          total_deleted_bytes=$((total_deleted_bytes + deleted_bytes))
        else
          echo "[WARN] Could not parse WORKDIR_DELETED_BYTES from clear_files.sh output"
        fi
      fi

      sleep 60
    done
  done
done

echo "All loop combinations completed."
echo "Total working-directory deleted across this run: $(to_human "$total_deleted_bytes") ($total_deleted_bytes bytes)"
