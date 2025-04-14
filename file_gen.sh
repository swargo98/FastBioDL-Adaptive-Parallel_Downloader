#!/bin/bash

# Variables
output_dir="."       # Output directory to store the files
file_count=10          # Number of files to create
file_size=3000           # Size of each file in MB
parallel_jobs=10        # Set parallel jobs to the number of CPU cores

# Create the output directory if it doesn't exist
# mkdir -p "$output_dir" || { echo "Error: Unable to create the output directory."; exit 1; }

# Function to create a single file
create_file() {
    local file_index=$1
    echo "Generating file: random_file_$file_index (100MB)..."
    dd if=/dev/zero of="$output_dir/random_file_$file_index" bs=1M count=$file_size status=none
    echo "File random_file_$file_index created."
}

export -f create_file
export output_dir file_size

# Generate files in parallel
seq 1 $file_count | xargs -n 1 -P $parallel_jobs -I {} bash -c 'create_file "$@"' _ {}

echo "Created $file_count random files of ${file_size}MB each in the directory: $output_dir"