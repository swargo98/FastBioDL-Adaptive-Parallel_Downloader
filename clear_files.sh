#!/bin/bash
# clear_files.sh — Remove experiment files from NVMe and clean working directory folders.

set -euo pipefail

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NVME_MOUNT="${NVME_MOUNT:-${LOCAL_SCRATCH:-/scratch/${USER:-user}/job_${SLURM_JOB_ID:-local}}}"
LOG_FILE="$WORKDIR/clear_files_deletion.log"
CONTEXT="${CLEAR_CONTEXT:-manual}"

to_human() {
    local bytes="$1"
    if command -v numfmt >/dev/null 2>&1; then
        numfmt --to=iec --suffix=B "$bytes"
    else
        echo "${bytes}B"
    fi
}

total_workdir_deleted_bytes=0

echo "=== Clearing NVMe experiment files ==="
# Remove all entries under the NVMe mount except lost+found
for entry in "$NVME_MOUNT"/*/; do
    name="$(basename "$entry")"
    if [[ "$name" == "lost+found" ]]; then
        echo "  Skipping $name"
        continue
    fi
    echo "  Removing $entry"
    rm -rf "$entry"
done

echo ""
echo "=== Removing working directory folders ==="
for folder in aria2c fastbiodl sratools kingfisher pysradb; do
    target="$WORKDIR/$folder"
    if [[ -d "$target" ]]; then
        folder_bytes=$(du -sb "$target" | awk '{print $1}')
        total_workdir_deleted_bytes=$((total_workdir_deleted_bytes + folder_bytes))
        echo "  Removing $target"
        rm -rf "$target"
    else
        echo "  Skipping $folder (not found)"
    fi
done

timestamp="$(date '+%Y-%m-%d %H:%M:%S')"
total_workdir_deleted_human="$(to_human "$total_workdir_deleted_bytes")"
printf '%s\t%s\t%s\n' "$timestamp" "$total_workdir_deleted_bytes" "$CONTEXT" >> "$LOG_FILE"

echo ""
echo "Working-directory deleted this cleanup: $total_workdir_deleted_human ($total_workdir_deleted_bytes bytes)"
echo "Deletion log appended to: $LOG_FILE"
# Machine-readable line consumed by run_all_benchmarks.sh
echo "WORKDIR_DELETED_BYTES=$total_workdir_deleted_bytes"

echo ""
echo "Done."
