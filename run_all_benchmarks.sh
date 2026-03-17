#!/usr/bin/env bash

# Run benchmark scripts across accession lists, repeating each combination.
# Loop order:
#   i: accession list
#   j: benchmark script
#   k: run count (1..4)

set -u

accession_lists=(
  "accessions_large_PRJNA200694.txt"
  "accessions_medium_PRJNA353374.txt"
  "accessions_small_PRJNA916347.txt"
)

scripts=(
  "fastbiodl_upgrade.py"
  "benchmark_aria2c_kingfisher.py"
  "benchmark_aria2c.py"
  "benchmark_pysradb.py"
)

for accession_list in "${accession_lists[@]}"; do
  if [[ ! -f "$accession_list" ]]; then
    echo "[ERROR] Missing accession list: $accession_list"
    continue
  fi

  for script in "${scripts[@]}"; do
    if [[ ! -f "$script" ]]; then
      echo "[ERROR] Missing script: $script"
      continue
    fi

    for run_count in {1..5}; do
      echo "============================================================"
      echo "Running: python3 $script -i $accession_list (run $run_count/5)"
      echo "============================================================"

      python3 "$script" -i "$accession_list"
      exit_code=$?
      if [[ $exit_code -ne 0 ]]; then
        echo "[WARN] Command failed with exit code $exit_code: python3 $script -i $accession_list"
      fi

      ./clear_files.sh
      clear_exit_code=$?
      if [[ $clear_exit_code -ne 0 ]]; then
        echo "[WARN] clear_files.sh failed with exit code $clear_exit_code"
      fi

      sleep 60
    done
  done
done

echo "All loop combinations completed."
