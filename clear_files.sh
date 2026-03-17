#!/bin/bash
# clear_files.sh — Remove experiment files from NVMe and clean working directory folders.

set -euo pipefail

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NVME_MOUNT="/mnt/nvme0n1"

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
        echo "  Removing $target"
        rm -rf "$target"
    else
        echo "  Skipping $folder (not found)"
    fi
done

echo ""
echo "Done."
